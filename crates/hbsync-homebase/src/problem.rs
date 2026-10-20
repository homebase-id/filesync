//! Error answers from an identity: RFC 7807 ProblemDetails, plaintext even on enveloped routes
//! (verified in odin-core `ExceptionHandlingMiddleware`, which sits outside the shared-secret
//! middleware). `errorCode` is the camelCase name of `OdinClientErrorCode`; older servers and
//! other clients also use the number, so both are read.
//!
//! Who sent an error matters for a "not there": only the identity's own 404 means a file is
//! gone (a proxy's, a load balancer's or a maintenance page's must not delete anything). odin-core
//! sets `X-Odin-Version` on every answer before anything else runs (`OdinVersionNumberMiddleware`,
//! first after logging in `Startup.Configure`; odin-core itself tells a peer that is an Odin
//! server by it, `PeerDriveQueryService`), and a route it matched answers either sealed
//! (`X-SSE: 1`: `SharedSecretEncryptionMiddleware` seals only a matched endpoint's answer) or, on
//! a route that isn't sealed, with ProblemDetails (`[ApiController]` turns `NotFound()` and
//! `StatusCode(410)` into one; `ExceptionHandlingMiddleware` writes one for an exception). All
//! **[verified]** in odin-core's code. A 404 for a route it has no endpoint for has neither.

use serde::Deserialize;

/// A non-success HTTP answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    /// The `errorCode` as its camelCase name (numbers of known codes are translated).
    pub code: Option<String>,
    pub title: String,
    /// The identity itself answered ([`ApiError::read`]), not something in front of it.
    pub from_identity: bool,
    /// When to ask again (`Retry-After`, in seconds): odin-core sends it with a refusal the
    /// caller can wait out (`OdinRetryLaterException`: out of quota, 507) and for a paused
    /// identity (503). Only the identity's own is taken (its header came with it): a box in
    /// front of it that is down for a moment isn't waited for an hour.
    pub retry_after: Option<std::time::Duration>,
}

/// What an error answer's headers say about who sent it (the module's doc).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sender {
    /// `X-Odin-Version`: odin-core sets it on every answer.
    pub odin: bool,
    /// `X-SSE: 1`: sealed under the shared secret, by a route the identity matched.
    pub sealed: bool,
    /// `Retry-After`, in seconds ([`ApiError::retry_after`]).
    pub retry_after: Option<std::time::Duration>,
}

/// The `OdinClientErrorCode`s the client acts on (`OdinClientErrorCode.cs`).
const KNOWN_CODES: &[(u32, &str)] = &[
    (4002, "cannotUpdateNonActiveFile"),
    (4105, "existingFileWithUniqueId"),
    (4106, "fileNotFound"),
    (4159, "missingVersionTag"),
    (4160, "versionTagMismatch"),
    (4175, "payloadHashMismatch"),
    (4176, "invalidPayloadHash"),
    (4177, "payloadHashRequired"),
    (4178, "payloadHashMissing"),
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProblemDetails {
    title: Option<String>,
    status: Option<u16>,
    error_code: Option<serde_json::Value>,
}

impl ApiError {
    /// Reads an error answer's body, of a sender not known; anything that isn't ProblemDetails
    /// keeps its text.
    pub fn parse(status: u16, body: &[u8]) -> Self {
        Self::read(status, body, Sender::default())
    }

    /// Reads an error answer's body, sent by `sender`. The identity sent it when odin-core's
    /// header came with it, and a route answered: sealed, or with ProblemDetails of its status.
    pub fn read(status: u16, body: &[u8], sender: Sender) -> Self {
        if sender.sealed {
            // Ciphertext: nothing in it to show.
            return ApiError {
                status,
                code: None,
                title: String::new(),
                from_identity: sender.odin,
                retry_after: sender.retry_after.filter(|_| sender.odin),
            };
        }
        match serde_json::from_slice::<ProblemDetails>(body) {
            Ok(p) => ApiError {
                status,
                code: p.error_code.and_then(code_name),
                title: p.title.unwrap_or_default(),
                from_identity: sender.odin && p.status == Some(status),
                retry_after: sender.retry_after.filter(|_| sender.odin),
            },
            Err(_) => ApiError {
                status,
                code: None,
                title: String::from_utf8_lossy(&body[..body.len().min(500)])
                    .trim()
                    .to_owned(),
                from_identity: false,
                retry_after: sender.retry_after.filter(|_| sender.odin),
            },
        }
    }

    /// Whether the error code is `name` (case-insensitive, as the server parses it).
    pub fn is(&self, name: &str) -> bool {
        self.code
            .as_deref()
            .is_some_and(|c| c.eq_ignore_ascii_case(name))
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}", self.status)?;
        if let Some(code) = &self.code {
            write!(f, " {code}")?;
        }
        if !self.title.is_empty() {
            write!(f, ": {}", self.title)?;
        }
        Ok(())
    }
}

fn code_name(code: serde_json::Value) -> Option<String> {
    match code {
        serde_json::Value::String(s) => Some(match s.parse::<u32>() {
            Ok(n) => number_name(n),
            Err(_) => s,
        }),
        serde_json::Value::Number(n) => n.as_u64().map(|n| number_name(n as u32)),
        _ => None,
    }
}

fn number_name(n: u32) -> String {
    KNOWN_CODES
        .iter()
        .find(|(number, _)| *number == n)
        .map_or_else(|| n.to_string(), |(_, name)| (*name).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_numbers_and_text() {
        let by_name = ApiError::parse(
            400,
            br#"{"type":"https://tools.ietf.org/html/rfc7231","title":"Version mismatch","status":400,"correlationId":"x","errorCode":"versionTagMismatch"}"#,
        );
        assert!(by_name.is("VersionTagMismatch"));
        assert_eq!(by_name.title, "Version mismatch");

        let by_number = ApiError::parse(400, br#"{"errorCode":4105}"#);
        assert!(by_number.is("existingFileWithUniqueId"));
        let as_string = ApiError::parse(400, br#"{"errorCode":"4177"}"#);
        assert!(as_string.is("payloadHashRequired"));
        let unknown = ApiError::parse(400, br#"{"errorCode":9999}"#);
        assert_eq!(unknown.code.as_deref(), Some("9999"));

        let text = ApiError::parse(502, b"<html>Bad Gateway</html>");
        assert_eq!(
            (text.code, text.title.as_str()),
            (None, "<html>Bad Gateway</html>")
        );
        let empty = ApiError::parse(401, b"");
        assert_eq!(empty.to_string(), "HTTP 401");
    }

    /// Only odin-core's header with a route's answer (sealed, or ProblemDetails of the same
    /// status) is the identity's: a proxy's page, an unrouted 404 and a bare one aren't.
    /// A `Retry-After` counts only from the identity: a box in front of it that is down for a
    /// moment isn't waited for an hour.
    #[test]
    fn retry_after_counts_from_the_identity_only() {
        let wait = Some(std::time::Duration::from_secs(600));
        let from = |odin| Sender {
            odin,
            retry_after: wait,
            ..Sender::default()
        };
        assert_eq!(ApiError::read(503, b"paused", from(true)).retry_after, wait);
        assert_eq!(ApiError::read(503, b"down", from(false)).retry_after, None);
    }

    #[test]
    fn the_identity_answered_only_with_its_header_and_a_route() {
        let odin = Sender {
            odin: true,
            ..Sender::default()
        };
        let sealed = Sender {
            odin: true,
            sealed: true,
            ..Sender::default()
        };
        let problem = br#"{"type":"https://tools.ietf.org/html/rfc9110#section-15.5.5","title":"Not Found","status":404,"traceId":"00-ab"}"#;
        let cases: [(u16, &[u8], Sender, bool); 9] = [
            // The header route's NotFound(), sealed.
            (404, br#"{"iv":"AAAA","data":"BBBB"}"#, sealed, true),
            // The payload route's NotFound(), and a payload missing (410).
            (404, problem, odin, true),
            (410, br#"{"status":410,"traceId":"00-ab"}"#, odin, true),
            (
                400,
                br#"{"title":"x","status":400,"errorCode":"fileNotFound"}"#,
                odin,
                true,
            ),
            // A route odin-core has no endpoint for: its header, no body.
            (404, b"", odin, false),
            // ProblemDetails of another status, or of no status.
            (404, br#"{"title":"Not Found","status":400}"#, odin, false),
            (404, br#"{"title":"Not Found"}"#, odin, false),
            // Something in front of the identity: no odin-core header.
            (404, problem, Sender::default(), false),
            (404, b"<html>404 Not Found</html>", Sender::default(), false),
        ];
        for (status, body, sender, identity) in cases {
            let error = ApiError::read(status, body, sender);
            assert_eq!(
                error.from_identity,
                identity,
                "{status} {} {sender:?}",
                String::from_utf8_lossy(body)
            );
        }
        let sealed_404 = ApiError::read(404, br#"{"iv":"AAAA","data":"BBBB"}"#, sealed);
        assert_eq!(sealed_404.to_string(), "HTTP 404", "no ciphertext shown");
    }
}
