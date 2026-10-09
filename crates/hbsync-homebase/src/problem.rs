//! Error answers from an identity: RFC 7807 ProblemDetails, plaintext even on enveloped routes
//! (verified in odin-core `ExceptionHandlingMiddleware`, which sits outside the shared-secret
//! middleware). `errorCode` is the camelCase name of `OdinClientErrorCode`; older servers and
//! other clients also use the number, so both are read.

use serde::Deserialize;

/// A non-success HTTP answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    /// The `errorCode` as its camelCase name (numbers of known codes are translated).
    pub code: Option<String>,
    pub title: String,
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
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProblemDetails {
    title: Option<String>,
    error_code: Option<serde_json::Value>,
}

impl ApiError {
    /// Reads an error answer's body; anything that isn't ProblemDetails keeps its text.
    pub fn parse(status: u16, body: &[u8]) -> Self {
        match serde_json::from_slice::<ProblemDetails>(body) {
            Ok(p) => ApiError {
                status,
                code: p.error_code.and_then(code_name),
                title: p.title.unwrap_or_default(),
            },
            Err(_) => ApiError {
                status,
                code: None,
                title: String::from_utf8_lossy(&body[..body.len().min(500)])
                    .trim()
                    .to_owned(),
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
}
