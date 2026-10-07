//! One bounded metadata GET. Provider bodies remain in memory and anonymous IPC.
use base64::Engine;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::time::{Duration, Instant};

pub const CAPABILITY: &str = "metadata_attempt_v1";
pub const BODY_CAP: u64 = 1 << 20;
pub const RESPONSE_LINE_CAP: usize = 2 << 20;
pub const MAX_TIMEOUT_MS: u64 = 60_000;
pub const MAX_REMAINING_MS: u64 = 125_000;
pub const MAX_RESPONSE_NODES: u64 = 128;
// Explicit bundles are configuration, not provider bodies. Bound certificate
// parsing under the separate 32 MiB native-client residency allowance.
const CA_FILE_CAP: u64 = 2 << 20;

#[derive(Serialize)]
pub struct Limits {
    pub payload_revision: u32,
    pub max_body_bytes: u64,
    pub response_line_bytes: usize,
}
pub const LIMITS: Limits = Limits {
    payload_revision: 1,
    max_body_bytes: BODY_CAP,
    response_line_bytes: RESPONSE_LINE_CAP,
};

/// Kept separate from per-attempt envelope/body allocation and durable IO credit.
pub fn resident_memory(rpc: usize, header: usize) -> Result<u64, &'static str> {
    crate::production::protocolmemory(rpc, header)?
        .checked_add(32 << 20)
        .ok_or("metadata_limit")
}

/// Mirrored in Python: five raw/copy extents, UTF-8/JSON string copies, small
/// structural graph, encoded/decoded payload overlap, selected headers, scratch.
pub fn response_memory(body: u64, header: usize) -> Result<u64, &'static str> {
    if body > BODY_CAP || header == 0 || header > 417_760 {
        return Err("metadata_limit");
    }
    Ok(13 * RESPONSE_LINE_CAP as u64
        + 6 * (body + 1)
        + 16 * header as u64
        + 256 * MAX_RESPONSE_NODES
        + 65_536)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsPolicy {
    pub mode: String,
    pub pem_file: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ticket {
    pub payload_revision: u32,
    pub profile: String,
    pub url: String,
    pub origin: String,
    pub trusted_hosts: Vec<String>,
    pub token: Option<String>,
    pub cookie: Option<String>,
    pub proxy_url: Option<String>,
    pub tls_policy: TlsPolicy,
    pub max_body_bytes: u64,
    pub http_header_bytes: usize,
    pub remaining_ms: u64,
}

#[derive(Default, Serialize)]
pub struct SelectedHeaders {
    location: Option<String>,
    retry_after: Option<String>,
}

#[derive(Serialize)]
pub struct Outcome {
    format: &'static str,
    status: Option<u16>,
    code: &'static str,
    phase: &'static str,
    headers: SelectedHeaders,
    disposition: &'static str,
    attempt_started: bool,
    body_read_started: bool,
    accounting_complete: bool,
    observed_bytes: u64,
    body_eof: bool,
    request_finalized: bool,
    retry_safe: bool,
    body_base64: Option<String>,
    body_bytes: Option<u64>,
    body_sha256: Option<String>,
}

impl Default for Outcome {
    fn default() -> Self {
        Self {
            format: "sakurapool-metadata-attempt-v1",
            status: None,
            code: "ok",
            phase: "metadata_send",
            headers: SelectedHeaders::default(),
            disposition: "NOT_READ",
            attempt_started: false,
            body_read_started: false,
            accounting_complete: true,
            observed_bytes: 0,
            body_eof: false,
            request_finalized: false,
            retry_safe: false,
            body_base64: None,
            body_bytes: None,
            body_sha256: None,
        }
    }
}

#[derive(Default)]
pub struct Context {
    client: Option<Client>,
    identity: Option<(String, Option<String>, String, Option<String>)>,
}

fn safe_text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.bytes().all(|byte| (0x21..0x7f).contains(&byte))
}

fn percent_decoded(raw: &[u8]) -> Vec<u8> {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let mut decoded = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%' && index + 2 < raw.len() {
            if let (Some(high), Some(low)) = (hex(raw[index + 1]), hex(raw[index + 2])) {
                decoded.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        decoded.push(raw[index]); // '+' is literal in userinfo and Location.
        index += 1;
    }
    decoded
}

fn credential_echo(ticket: &Ticket, value: &str) -> bool {
    let contains = |layer: &[u8], secret: &[u8]| {
        !secret.is_empty() && layer.windows(secret.len()).any(|part| part == secret)
    };
    let proxy = ticket.proxy_url.as_deref().and_then(|raw| Url::parse(raw).ok());
    let proxy_password = proxy.as_ref().and_then(Url::password);
    if !ticket.token.as_deref().is_some_and(|v| !v.is_empty())
        && !ticket.cookie.as_deref().is_some_and(|cookie| {
            cookie.split(';').filter_map(|part| part.trim().split_once('='))
                .any(|(_, value)| !value.is_empty())
        })
        && !proxy_password.is_some_and(|v| !v.is_empty())
    {
        return false;
    }
    let proxy_decoded = proxy_password.map(|password| percent_decoded(password.as_bytes()));
    // Hyper's proxy Basic-auth userinfo and Python unquote use lossy UTF-8.
    // Compare both the original bytes and the credential actually sent.
    let proxy_lossy = proxy_decoded.as_ref()
        .map(|password| String::from_utf8_lossy(password).into_owned());
    let mut layer = value.as_bytes().to_vec();
    for _ in 0..5 {
        let lossy = String::from_utf8_lossy(&layer);
        let echo = |secret: &[u8]| contains(&layer, secret) || contains(lossy.as_bytes(), secret);
        if ticket.token.as_deref().is_some_and(|token| echo(token.as_bytes()))
            || ticket.cookie.as_deref().is_some_and(|cookie| {
                cookie.split(';').filter_map(|part| part.trim().split_once('='))
                    .any(|(_, secret)| echo(secret.as_bytes()))
            })
            || proxy_password.is_some_and(|password| echo(password.as_bytes()))
            || proxy_decoded.as_ref().is_some_and(|password| echo(password))
            || proxy_lossy.as_ref().is_some_and(|password| echo(password.as_bytes()))
        {
            return true;
        }
        let decoded = percent_decoded(&layer);
        if decoded == layer { return false; }
        layer = decoded;
    }
    true // Reject deeper ambiguous encoding instead of claiming it is safe.
}

fn target(ticket: &Ticket) -> Result<Url, &'static str> {
    let test = ticket.profile == "loopback_test";
    if ticket.payload_revision != 1
        || (!test && ticket.profile != "modelscope_https_v1")
        || ticket.max_body_bytes > BODY_CAP
        || ticket.http_header_bytes == 0
        || ticket.http_header_bytes > 417_760
        || !(1..=MAX_REMAINING_MS).contains(&ticket.remaining_ms)
        || !safe_text(&ticket.url, 8192)
        || ticket.url.contains('\\')
        || !safe_text(&ticket.origin, 256)
        || ticket.trusted_hosts.is_empty()
        || ticket.trusted_hosts.len() > 8
    {
        return Err("rejected");
    }
    let origin = Url::parse(&ticket.origin).map_err(|_| "rejected")?;
    let url = Url::parse(&ticket.url).map_err(|_| "rejected")?;
    if origin.path() != "/" || origin.query().is_some() || origin.fragment().is_some()
        || !origin.username().is_empty() || origin.password().is_some()
        || !url.username().is_empty() || url.password().is_some() || url.fragment().is_some()
        || ticket.origin.ends_with('/')
        || ticket.origin != origin.origin().ascii_serialization()
    {
        return Err("rejected");
    }
    if test {
        if origin.scheme() != "http" || origin.host_str() != Some("127.0.0.1")
            || origin.port().is_none() || url.origin() != origin.origin()
            || ticket.trusted_hosts != ["127.0.0.1"]
            || ticket.token.is_some() || ticket.cookie.is_some() || ticket.proxy_url.is_some()
            || ticket.tls_policy.mode != "native" || ticket.tls_policy.pem_file.is_some()
        {
            return Err("rejected");
        }
    } else {
        if !matches!(ticket.origin.as_str(), "https://modelscope.cn" | "https://www.modelscope.cn")
            || url.scheme() != "https" || url.port_or_known_default() != Some(443)
            || ticket.trusted_hosts.iter().any(|host| !matches!(host.as_str(), "modelscope.cn" | "www.modelscope.cn"))
            || !url.host_str().is_some_and(|host| ticket.trusted_hosts.iter().any(|h| h == host))
            || !ticket.trusted_hosts.iter().any(|h| Some(h.as_str()) == origin.host_str())
        {
            return Err("rejected");
        }
        if (ticket.token.is_some() || ticket.cookie.is_some()) && url.origin() != origin.origin() {
            return Err("rejected");
        }
    }
    if ticket.token.as_deref().is_some_and(|v| !safe_text(v, 4096))
        || ticket.cookie.as_deref().is_some_and(|v| {
            v.is_empty() || v.len() > 512 || v.bytes().any(|b| !(0x20..0x7f).contains(&b))
        })
        || credential_echo(ticket, &ticket.url)
    {
        return Err("rejected");
    }
    Ok(url)
}

fn make_client(ticket: &Ticket) -> Result<Client, &'static str> {
    let mut builder = Client::builder()
        .no_proxy()
        .user_agent("SakuraMoon/1")
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60));
    if let Some(proxy) = &ticket.proxy_url {
        if proxy.is_empty() || proxy.len() > 8192 || proxy.bytes().any(|b| b <= 0x20 || b == 0x7f || b == b'\\') {
            return Err("rejected");
        }
        let parsed = Url::parse(proxy).map_err(|_| "rejected")?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none()
            || parsed.fragment().is_some() || parsed.query().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            return Err("rejected");
        }
        builder = builder.proxy(reqwest::Proxy::all(proxy).map_err(|_| "rejected")?);
    }
    match (ticket.tls_policy.mode.as_str(), ticket.tls_policy.pem_file.as_deref()) {
        ("native", None) => {}
        ("pem_bundle", Some(path)) => {
            if path.is_empty() || path.len() > 4096 || path.chars().any(char::is_control) {
                return Err("rejected");
            }
            let mut file = std::fs::File::open(path).map_err(|_| "rejected")?;
            let meta = file.metadata().map_err(|_| "rejected")?;
            if !meta.is_file() || meta.len() == 0 || meta.len() > CA_FILE_CAP {
                return Err("rejected");
            }
            let mut bytes = Vec::with_capacity(meta.len() as usize);
            file.by_ref().take(CA_FILE_CAP + 1).read_to_end(&mut bytes).map_err(|_| "rejected")?;
            if bytes.is_empty() || bytes.len() as u64 > CA_FILE_CAP {
                return Err("rejected");
            }
            let certificates = reqwest::Certificate::from_pem_bundle(&bytes).map_err(|_| "rejected")?;
            if certificates.is_empty() { return Err("rejected"); }
            builder = builder.tls_built_in_root_certs(false);
            for certificate in certificates { builder = builder.add_root_certificate(certificate); }
        }
        _ => return Err("rejected"),
    }
    builder.build().map_err(|_| "rejected")
}

fn single<'a>(headers: &'a HeaderMap, key: &str) -> Result<Option<&'a str>, &'static str> {
    if headers.get_all(key).iter().count() > 1 { return Err("body_framing"); }
    headers.get(key).map(|v| v.to_str().map_err(|_| "body_framing")).transpose()
}

fn selected(response: &Response, ticket: &Ticket) -> Result<SelectedHeaders, &'static str> {
    let mut total = 0usize;
    for (key, value) in response.headers() {
        total = total.checked_add(key.as_str().len() + value.as_bytes().len() + 4)
            .ok_or("metadata_limit")?;
    }
    if total > ticket.http_header_bytes { return Err("metadata_limit"); }
    let location = single(response.headers(), "location")?;
    let retry_after = single(response.headers(), "retry-after")?;
    for (value, limit) in [(location, 8192), (retry_after, 256)] {
        if value.is_some_and(|v| v.len() > limit || !v.is_ascii()
            || v.bytes().any(|b| b < 0x20 || b == 0x7f) || credential_echo(ticket, v))
        {
            return Err("body_framing");
        }
    }
    Ok(SelectedHeaders {
        location: location.map(str::to_owned),
        retry_after: retry_after.map(str::to_owned),
    })
}

fn framing(response: &Response) -> Result<Option<u64>, &'static str> {
    let headers = response.headers();
    if !single(headers, "content-encoding")?.unwrap_or("identity").eq_ignore_ascii_case("identity") {
        return Err("metadata_encoding");
    }
    let length = single(headers, "content-length")?;
    let transfer = single(headers, "transfer-encoding")?;
    if (length.is_some() && transfer.is_some())
        || transfer.is_some_and(|v| !v.eq_ignore_ascii_case("chunked"))
    {
        return Err("body_framing");
    }
    length.map(|v| {
        if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) { return Err("body_framing"); }
        v.parse::<u64>().map_err(|_| "body_framing")
    }).transpose()
}

fn perform(ticket: &Ticket, context: &mut Context, outcome: &mut Outcome, deadline: Instant)
    -> Result<(), &'static str>
{
    let url = target(ticket)?;
    let identity = (url.origin().ascii_serialization(), ticket.proxy_url.clone(),
        ticket.tls_policy.mode.clone(), ticket.tls_policy.pem_file.clone());
    if context.identity.as_ref() != Some(&identity) {
        context.client = None;
        context.identity = None;
        context.client = Some(make_client(ticket)?);
        context.identity = Some(identity);
    }
    let timeout = deadline.checked_duration_since(Instant::now()).filter(|v| !v.is_zero())
        .ok_or("network_ambiguous")?;
    let client = context.client.as_ref().ok_or("rejected")?;
    let mut request = client.get(url).header("accept-encoding", "identity")
        .header("accept", "application/json").timeout(timeout);
    if let Some(token) = &ticket.token {
        request = request.header("authorization", HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| "rejected")?);
    }
    if let Some(cookie) = &ticket.cookie {
        request = request.header("cookie", HeaderValue::from_str(cookie).map_err(|_| "rejected")?);
    }
    outcome.attempt_started = true;
    outcome.accounting_complete = false;
    let mut response = match request.send() {
        Ok(response) => response,
        Err(error) => {
            let code = if error.is_timeout() { "origin_timeout" }
                else if error.is_connect() { "origin_connect" } else { "network_ambiguous" };
            // Drop the final blocking client owner. Its runtime thread joins before
            // a clean pre-body terminal proof can be emitted. Python kills an
            // unresponsive process rather than assuming this has completed.
            context.client = None;
            context.identity = None;
            outcome.accounting_complete = code != "network_ambiguous";
            outcome.retry_safe = outcome.accounting_complete;
            return Err(code);
        }
    };
    outcome.phase = "metadata_headers";
    outcome.accounting_complete = true; // No application body read has happened.
    if !(100..=599).contains(&response.status().as_u16()) {
        return Err("body_framing");
    }
    outcome.status = Some(response.status().as_u16());
    outcome.headers = selected(&response, ticket)?;
    if response.status().as_u16() != 200 {
        outcome.retry_safe = true; // Technical permission only; Python gates status/URL policy.
        return Ok(());
    }
    let declared = framing(&response)?;
    if declared.is_some_and(|size| size > ticket.max_body_bytes) {
        return Err("metadata_limit");
    }
    outcome.phase = "metadata_body";
    outcome.disposition = "PARTIAL";
    outcome.accounting_complete = false;
    let mut body = Vec::with_capacity(ticket.max_body_bytes.min(65_536) as usize);
    let mut scratch = [0u8; 65_536];
    loop {
        if Instant::now() >= deadline {
            if !outcome.body_read_started {
                outcome.disposition = "NOT_READ";
                outcome.accounting_complete = true;
            }
            return Err("body_io");
        }
        let count = (ticket.max_body_bytes + 1 - outcome.observed_bytes).min(scratch.len() as u64) as usize;
        outcome.body_read_started = true;
        let got = response.read(&mut scratch[..count]).map_err(|_| "body_io")?;
        outcome.observed_bytes += got as u64;
        if outcome.observed_bytes > ticket.max_body_bytes {
            outcome.disposition = "LIMIT";
            outcome.accounting_complete = true;
            return Err("metadata_limit");
        }
        if got == 0 {
            if declared.is_some_and(|n| n != outcome.observed_bytes) { return Err("body_io"); }
            if Instant::now() >= deadline { return Err("body_io"); }
            outcome.disposition = "EOF";
            outcome.body_eof = true;
            outcome.accounting_complete = true;
            outcome.body_bytes = Some(outcome.observed_bytes);
            outcome.body_sha256 = Some(format!("{:x}", Sha256::digest(&body)));
            outcome.body_base64 = Some(base64::engine::general_purpose::STANDARD.encode(&body));
            if Instant::now() >= deadline {
                outcome.disposition = "PARTIAL";
                outcome.accounting_complete = false;
                outcome.body_eof = false;
                outcome.body_bytes = None;
                outcome.body_sha256 = None;
                outcome.body_base64 = None;
                return Err("body_io");
            }
            return Ok(());
        }
        body.extend_from_slice(&scratch[..got]);
    }
}

pub fn run(ticket: Ticket, context: &mut Context) -> Outcome {
    let mut outcome = Outcome::default();
    let deadline = Instant::now() + Duration::from_millis(ticket.remaining_ms.min(MAX_TIMEOUT_MS));
    if let Err(code) = perform(&ticket, context, &mut outcome, deadline) {
        outcome.code = code;
        // Unknown/partial response ownership cannot be pooled across a new call.
        if !outcome.accounting_complete {
            context.client = None;
            context.identity = None;
        }
    }
    // perform owns the response; no acknowledgement precedes its destruction.
    outcome.request_finalized = true;
    outcome
}
