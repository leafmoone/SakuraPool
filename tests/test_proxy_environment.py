"""Proxy/TLS configuration and credential-scope regression checks (no live network)."""

import io
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from sakurapool.storage.transport import GuardedTransport, _network_options


class Raw(io.BytesIO):
    def read(self, n=-1, decode_content=False):
        return super().read(n)


def response(status=200, headers=None, body=b""):
    r = requests.Response()
    r.status_code = status
    r.headers = CaseInsensitiveDict(headers or {})
    r.raw = Raw(body)
    return r


class ProxyEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_proxy_selection_is_per_url_and_no_proxy_honored(self):
        os.environ.update(
            HTTPS_PROXY="http://proxy.invalid:8080",
            HTTP_PROXY="http://http.invalid:8080",
            ALL_PROXY="http://all.invalid:8080",
            NO_PROXY="origin.example.test",
        )
        self.assertEqual(_network_options("https://origin.example.test/file")["proxies"], {})
        p = _network_options("https://cdn.example.test/file")["proxies"]
        self.assertEqual(p["https"], "http://proxy.invalid:8080")
        self.assertEqual(p["http"], "http://http.invalid:8080")
        self.assertEqual(p["all"], "http://all.invalid:8080")
        self.assertIsNot(_network_options("https://cdn.example.test/file")["verify"], False)

    def test_lowercase_proxy_and_no_proxy(self):
        os.environ.update(https_proxy="http://proxy.invalid:8080", no_proxy=".example.test")
        self.assertEqual(_network_options("https://cdn.example.test/file")["proxies"], {})
        self.assertEqual(
            _network_options("https://cdn.other.test/file")["proxies"]["https"],
            "http://proxy.invalid:8080",
        )

    def test_requests_ca_override_and_curl_fallback(self):
        os.environ.update(REQUESTS_CA_BUNDLE="/test/requests.pem", CURL_CA_BUNDLE="/test/curl.pem")
        self.assertEqual(
            _network_options("https://origin.example.test")["verify"], "/test/requests.pem"
        )
        del os.environ["REQUESTS_CA_BUNDLE"]
        self.assertEqual(
            _network_options("https://origin.example.test")["verify"], "/test/curl.pem"
        )

    def test_system_cafile_used_without_bundle_override(self):
        with patch(
            "sakurapool.storage.transport.ssl.get_default_verify_paths",
            return_value=SimpleNamespace(cafile="/test/system.pem", capath="/test/certs"),
        ):
            self.assertEqual(
                _network_options("https://origin.example.test")["verify"], "/test/system.pem"
            )

    def test_system_capath_fallback(self):
        with patch(
            "sakurapool.storage.transport.ssl.get_default_verify_paths",
            return_value=SimpleNamespace(cafile=None, capath="/test/certs"),
        ):
            self.assertEqual(
                _network_options("https://origin.example.test")["verify"], "/test/certs"
            )

    def test_all_proxy_fallback_and_mixedcase_https(self):
        os.environ.update(all_proxy="http://proxy.invalid:8080", NO_PROXY="origin.example.test")
        self.assertEqual(_network_options("hTtPs://origin.example.test/file")["proxies"], {})
        p = _network_options("hTtPs://cdn.example.test/file")["proxies"]
        self.assertEqual(
            requests.utils.select_proxy("https://cdn.example.test/file", p),
            "http://proxy.invalid:8080",
        )
        with GuardedTransport(trusted_hosts=frozenset({"cdn.example.test"})) as t:
            with patch.object(HTTPAdapter, "send", autospec=True, return_value=response()) as send:
                t._once(
                    "hTtPs://cdn.example.test/file",
                    max_body=0,
                    metadata=False,
                    inflight=0,
                    headers={},
                )
            self.assertEqual(send.call_args.kwargs["proxies"]["all"], "http://proxy.invalid:8080")

    def test_mixedcase_http_offline_stays_local(self):
        os.environ.update(
            http_proxy="http://proxy.invalid:8080", all_proxy="http://all.invalid:8080"
        )
        with GuardedTransport(
            trusted_hosts=frozenset({"127.0.0.1"}), offline_mode=True, allow_loopback_http=True
        ) as t:
            for scheme in ("HTTP", "hTtP", "http"):
                with patch.object(
                    HTTPAdapter, "send", autospec=True, return_value=response()
                ) as send:
                    t._once(
                        f"{scheme}://127.0.0.1:34567/file",
                        max_body=0,
                        metadata=False,
                        inflight=0,
                        headers={},
                    )
                self.assertEqual(send.call_args.kwargs["proxies"], {})

    def test_tls_never_boolean_disabled_by_env(self):
        for raw in ("false", "0", ""):
            with self.subTest(raw=raw):
                os.environ["REQUESTS_CA_BUNDLE"] = raw
                self.assertIsNot(_network_options("https://origin.example.test")["verify"], False)

    def test_offline_ignores_proxies_and_ca_overrides(self):
        os.environ.update(
            HTTP_PROXY="http://proxy.invalid:8080",
            HTTPS_PROXY="http://proxy.invalid:8080",
            ALL_PROXY="http://proxy.invalid:8080",
            REQUESTS_CA_BUNDLE="/nonexistent.pem",
        )
        self.assertEqual(
            _network_options("http://127.0.0.1:34567/file", offline=True),
            {"proxies": {}, "verify": True},
        )
        with GuardedTransport(
            trusted_hosts=frozenset({"127.0.0.1"}), offline_mode=True, allow_loopback_http=True
        ) as t:
            with patch.object(HTTPAdapter, "send", autospec=True, return_value=response()) as send:
                t._once(
                    "http://127.0.0.1:34567/file",
                    max_body=0,
                    metadata=False,
                    inflight=0,
                    headers={},
                )
            self.assertEqual(send.call_args.kwargs["proxies"], {})
            self.assertIs(send.call_args.kwargs["verify"], True)

    def test_origin_credentials_never_attach_to_direct_cdn(self):
        os.environ.update(HTTPS_PROXY="http://proxy.invalid:8080", NO_PROXY="origin.example.test")
        seen = []

        def capture(adapter, request, **kwargs):
            seen.append((request, kwargs))
            return response()

        with GuardedTransport(
            trusted_hosts=frozenset({"origin.example.test", "cdn.example.test"}),
            token="synthetic-token",
            credential_origin="https://origin.example.test",
            same_origin_cookie="session=synthetic-cookie",
        ) as t:
            self.assertFalse(t.session.trust_env)
            with (
                patch(
                    "requests.sessions.get_netrc_auth",
                    side_effect=AssertionError("netrc must not be read"),
                ),
                patch.object(HTTPAdapter, "send", autospec=True, side_effect=capture),
            ):
                for host in ("origin.example.test", "cdn.example.test"):
                    t._once(
                        f"https://{host}/file", max_body=0, metadata=False, inflight=0, headers={}
                    )
        self.assertEqual(seen[0][0].headers["Authorization"], "Bearer synthetic-token")
        self.assertEqual(seen[0][0].headers["Cookie"], "session=synthetic-cookie")
        self.assertEqual(seen[0][1]["proxies"], {})
        self.assertNotIn("Authorization", seen[1][0].headers)
        self.assertNotIn("Cookie", seen[1][0].headers)
        self.assertNotIn("Proxy-Authorization", seen[1][0].headers)
        self.assertEqual(seen[1][1]["proxies"]["https"], "http://proxy.invalid:8080")

    def test_two_hop_fresh_session_credentials_and_proxy_isolation(self):
        os.environ.update(HTTPS_PROXY="http://proxy.invalid:8080", NO_PROXY="origin.example.test")
        seen = []

        def capture(adapter, request, **kwargs):
            seen.append((request, kwargs))
            if len(seen) == 1:
                r = response(
                    302,
                    {
                        "Location": "https://cdn.example.test/file?Signature=fixture",
                        "Set-Cookie": "server-secret=fixture",
                    },
                )
                r.request, r.url = request, request.url
                return r
            return response(
                206,
                {"Content-Range": "bytes 0-2/3", "Content-Length": "3", "ETag": '"fixture-etag"'},
                b"abc",
            )

        with GuardedTransport(
            trusted_hosts=frozenset({"origin.example.test"}),
            token="synthetic-token",
            credential_origin="https://origin.example.test",
            same_origin_cookie="session=synthetic-cookie",
        ) as t:
            with (
                patch(
                    "requests.sessions.get_netrc_auth",
                    side_effect=AssertionError("netrc must not be read"),
                ),
                patch.object(HTTPAdapter, "send", autospec=True, side_effect=capture),
            ):
                result = t.two_hop_range(
                    "https://origin.example.test/file", start=0, length=3, expected_size=3
                )
        self.assertEqual(result.payload, b"abc")
        self.assertEqual(seen[0][0].headers["Authorization"], "Bearer synthetic-token")
        self.assertEqual(seen[0][0].headers["Cookie"], "session=synthetic-cookie")
        self.assertEqual(seen[0][1]["proxies"], {})
        self.assertNotIn("Authorization", seen[1][0].headers)
        self.assertNotIn("Cookie", seen[1][0].headers)
        self.assertEqual(seen[1][1]["proxies"]["https"], "http://proxy.invalid:8080")

    def test_session_cookies_are_cleared_before_next_request(self):
        with GuardedTransport(trusted_hosts=frozenset({"origin.example.test"})) as t:
            t.session.cookies.set(
                "from-earlier-response", "synthetic", domain="origin.example.test"
            )
            with patch.object(HTTPAdapter, "send", autospec=True, return_value=response()) as send:
                t._once(
                    "https://origin.example.test/file",
                    max_body=0,
                    metadata=False,
                    inflight=0,
                    headers={},
                )
            self.assertNotIn("Cookie", send.call_args.args[1].headers)

    def test_clone_retains_security_and_proxy_behavior(self):
        with GuardedTransport(
            trusted_hosts=frozenset({"origin.example.test"}),
            token="synthetic-token",
            credential_origin="https://origin.example.test",
        ) as t:
            with t.clone() as c:
                self.assertFalse(c.session.trust_env)
                self.assertIsNot(c.session, t.session)
                self.assertEqual(c.token, t.token)
                self.assertEqual(c.credential_origin, t.credential_origin)


if __name__ == "__main__":
    unittest.main(verbosity=2)
