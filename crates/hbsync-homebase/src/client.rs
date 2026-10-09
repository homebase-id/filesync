//! Calls to an identity's V2 API as an authorized app client. Every request goes inside the
//! shared-secret envelope (verified in odin-core `SharedSecretEncryptionMiddleware`): a GET's
//! query string becomes one `ss` parameter, even when empty; a JSON body becomes `{iv, data}`;
//! a response marked `X-SSE: 1` comes back the same way.

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::de::DeserializeOwned;

use crate::HomebaseError;
use crate::crypto::{Envelope, Key};
use crate::youauth::Credentials;

#[derive(Clone)]
pub struct Client {
    base: String,
    bearer: String,
    key: Key,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl Client {
    pub fn new(credentials: &Credentials) -> Result<Self, HomebaseError> {
        Ok(Client {
            base: format!("https://{}", credentials.identity),
            bearer: format!("Bearer {}", credentials.client_auth_token),
            key: credentials.shared_secret_key()?,
        })
    }

    /// `GET {path}` with `query` encrypted into `ss`.
    pub fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, HomebaseError> {
        let qs: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{k}={}", utf8_percent_encode(v, NON_ALPHANUMERIC)))
            .collect();
        let ss = serde_json::to_string(&Envelope::seal(&self.key, qs.join("&").as_bytes())?)?;
        let url = format!(
            "{}{path}?ss={}",
            self.base,
            utf8_percent_encode(&ss, NON_ALPHANUMERIC)
        );
        let response = ureq::get(&url).header("Authorization", &self.bearer).call();
        self.decode(response, path)
    }

    /// `POST {path}` with `body` in an envelope.
    pub fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, HomebaseError> {
        let sealed = Envelope::seal(&self.key, &serde_json::to_vec(body)?)?;
        let response = ureq::post(format!("{}{path}", self.base))
            .header("Authorization", &self.bearer)
            .send_json(&sealed);
        self.decode(response, path)
    }

    fn decode<T: DeserializeOwned>(
        &self,
        response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
        what: &str,
    ) -> Result<T, HomebaseError> {
        let mut response = check(response, what)?;
        let encrypted = response
            .headers()
            .get("X-SSE")
            .is_some_and(|v| v.as_bytes() == b"1");
        let body = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| HomebaseError::Http(format!("{what}: {e}")))?;
        let plain = if encrypted {
            serde_json::from_slice::<Envelope>(&body)?.open(&self.key)?
        } else {
            body
        };
        Ok(serde_json::from_slice(&plain)?)
    }
}

/// A plain JSON exchange (no envelope), for the YouAuth token call.
pub(crate) fn http_json<T: DeserializeOwned>(
    response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<T, HomebaseError> {
    let mut response = check(response, what)?;
    response
        .body_mut()
        .read_json()
        .map_err(|e| HomebaseError::Http(format!("{what}: {e}")))
}

fn check(
    response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<ureq::http::Response<ureq::Body>, HomebaseError> {
    response.map_err(|e| match e {
        ureq::Error::StatusCode(401 | 403) => {
            HomebaseError::Denied(format!("{what}: not authorized"))
        }
        e => HomebaseError::Http(format!("{what}: {e}")),
    })
}
