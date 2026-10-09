//! Eight independent bounded lightweight channels in one explicitly negotiated process.
use super::*;
use std::sync::{mpsc, Arc, Mutex};

pub const CHANNELS: usize = 8;
#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Message {
    #[serde(rename = "channel_open")]
    Open { channel: usize, generation: u64, request_id: String, metadata: bool },
    #[serde(rename = "channel_close")]
    Close { channel: usize, generation: u64, request_id: String },
    #[serde(rename = "channel_request")]
    Request { channel: usize, generation: u64, request: Request },
}
#[derive(Default)]
struct State {
    generation: u64,
    open: bool,
    busy: bool,
    metadata: bool,
    seen: BTreeSet<String>,
}
enum Command {
    Open(String),
    Close(String),
    Request(Request),
}
#[derive(Serialize)]
struct Reply<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    channel: usize,
    generation: u64,
    reply: &'a Outbound,
}
fn message(request_id: &str, outcome: Result<serde_json::Value, &'static str>) -> Outbound {
    let result = outcome.unwrap_or_else(|_| serde_json::json!({
        "production_error":"rejected",
        "diagnostic":{"code":"rejected","phase":"worker","recoverable":false,
            "delivery_safe":false,"http_status":null},
        "technical":{"eof_observed":false}
    }));
    let error = result.get("production_error").map(|_| "production_rejected");
    Outbound::Response {request_id:request_id.to_owned(),ok:error.is_none(),
        result:Some(result),error}
}
fn emit_reply(writer: &Arc<Mutex<io::BufWriter<io::Stdout>>>, channel: usize,
              generation: u64, reply: &Outbound, limit: usize) {
    let mut stdout = writer.lock().unwrap();
    let mut checked = LimitedWriter {inner:&mut *stdout,remaining:limit};
    if serde_json::to_writer(&mut checked,&Reply {kind:"channel_response",channel,
            generation,reply}).is_err() || checked.write_all(b"\n").is_err()
            || checked.flush().is_err() {
        std::process::exit(2);
    }
}
fn invalid() -> ! { std::process::exit(2) }
fn identifier(value: &str) -> bool { !value.is_empty() && value.len() <= MAX_REQUEST_ID_BYTES }

pub fn run(reader: &mut impl BufRead, capacity: StreamCapacity) -> io::Result<()> {
    let writer = Arc::new(Mutex::new(io::BufWriter::new(io::stdout())));
    let mut slots = Vec::with_capacity(CHANNELS);
    let mut senders = Vec::with_capacity(CHANNELS);
    for channel in 0..CHANNELS {
        let state = Arc::new(Mutex::new(State::default()));
        let (send, receive) = mpsc::sync_channel::<Command>(1);
        let local = state.clone();
        let output = writer.clone();
        let config = capacity.clone();
        std::thread::Builder::new().name(format!("sakura-channel-{channel}"))
            .spawn(move || {
                let mut execution = sakurapool_rust::production::ExecutionContext::default();
                let mut metadata = sakurapool_rust::metadata::Context::default();
                while let Ok(command) = receive.recv() {
                    let (id, outcome, closing, response_limit) = match command {
                        Command::Open(id) => {
                            execution = Default::default(); metadata = Default::default();
                            (id,Ok(serde_json::json!({"channel_state":"open"})),false,config.rpc_line_bytes)
                        }
                        Command::Close(id) => {
                            execution = Default::default(); metadata = Default::default();
                            (id,Ok(serde_json::json!({"channel_state":"closed"})),true,config.rpc_line_bytes)
                        }
                        Command::Request(request) => {
                            let limit = if request.operation == "metadata_attempt" {
                                sakurapool_rust::metadata::RESPONSE_LINE_CAP
                            } else {config.rpc_line_bytes};
                            let outcome = dispatch(&request,&mut execution,&mut metadata,Some(&config),true);
                            (request.request_id,outcome,false,limit)
                        }
                    };
                    let reply = message(&id,outcome);
                    // Completion/state and acknowledgement are one critical section.
                    // The main reader never holds this lock during network execution.
                    let mut state = local.lock().unwrap();
                    if closing {state.open=false;state.seen.clear();}
                    emit_reply(&output,channel,state.generation,&reply,response_limit+512);
                    state.busy=false;
                }
            })?;
        slots.push(state); senders.push(send);
    }
    loop {
        let line = match read_line_bounded(reader,capacity.rpc_line_bytes+512)? {
            Ok(line) => line,
            Err(_) => invalid(),
        };
        if line.is_empty() {break;}
        let text = match std::str::from_utf8(&line) {Ok(text)=>text,Err(_)=>invalid()};
        if lexical_bounds(text).is_err() {invalid();}
        let message: Message = match serde_json::from_str(text) {Ok(value)=>value,Err(_)=>invalid()};
        let (channel,generation,id,role,request,closing) = match message {
            Message::Open {channel,generation,request_id,metadata} =>
                (channel,generation,request_id,Some(metadata),None,false),
            Message::Close {channel,generation,request_id} =>
                (channel,generation,request_id,None,None,true),
            Message::Request {channel,generation,request} =>
                (channel,generation,request.request_id.clone(),None,Some(request),false),
        };
        if channel>=CHANNELS || !identifier(&id) {invalid();}
        let mut state = slots[channel].lock().unwrap();
        if state.busy {invalid();}
        let command = if let Some(metadata) = role {
            if state.open || state.generation.checked_add(1)!=Some(generation) {invalid();}
            state.generation=generation;state.open=true;state.metadata=metadata;state.seen.clear();
            Command::Open(id)
        } else {
            if !state.open || state.generation!=generation {invalid();}
            if closing {Command::Close(id)} else {
                let request=request.unwrap();
                if request.kind!="request" || request.budget.is_some()
                    || state.seen.len()>=MAX_SESSION_REQUESTS || !state.seen.insert(id)
                    || (state.metadata && request.operation!="metadata_attempt")
                    || (!state.metadata && (request.operation=="metadata_attempt"
                        || request.payload.get("production").is_none())) {invalid();}
                Command::Request(request)
            }
        };
        state.busy=true;
        if senders[channel].try_send(command).is_err() {invalid();}
    }
    // EOF ends the foreground owner process. No detached service outlives it.
    Ok(())
}
