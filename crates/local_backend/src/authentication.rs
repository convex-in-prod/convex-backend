//! Code for handling authentication between the CLI user / dashboard and the
//! backend.

use anyhow::{
    anyhow,
    Context,
};
use authentication::extract_bearer_token;
use axum::{
    extract::FromRequestParts,
    RequestPartsExt,
};
use common::{
    http::{
        extract::{
            FromMtState,
            Query,
        },
        ExtractRequestId,
        ExtractRequestMetadata,
        ExtractResolvedHostname,
        HttpResponseError,
    },
    runtime::Runtime,
    types::remove_type_prefix_from_admin_key,
    RequestContext,
};
use errors::ErrorMetadata;
use keybroker::Identity;
use serde::Deserialize;
use sync_types::{
    AuthenticationToken,
    UserIdentityAttributes,
};

use crate::{
    LocalAppState,
    RouterState,
};

pub struct ExtractAuthenticationToken(pub AuthenticationToken);

impl<T: Sync> FromRequestParts<T> for ExtractAuthenticationToken {
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _st: &T,
    ) -> Result<Self, Self::Rejection> {
        // First, try extracting from headers
        if let Some(h) = parts.headers.get(http::header::AUTHORIZATION) {
            let h_str = h.to_str().context(ErrorMetadata::bad_request(
                "HeaderParseFailure",
                format!("Failed to parse header {h:?}"),
            ))?;
            let is_admin_key = h_str
                .get(..7)
                .ok_or_else(|| anyhow!("Invalid Header"))
                .context(ErrorMetadata::bad_request(
                    "InvalidHeaderFailure",
                    "Invalid authentication header".to_string(),
                ))?
                .eq_ignore_ascii_case("convex ");

            return if is_admin_key {
                // This is an admin key, not an OIDC bearer token. These are sent from the
                // dashboard in lieu of our old cookie-based auth.
                Ok(Self(extract_admin_key(h_str)?))
            } else {
                let auth: String = extract_bearer_token(Some(h_str.to_string()))
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(ErrorMetadata::bad_request(
                            "InvalidAdminKey",
                            "Invalid admin key",
                        ))
                    })?
                    .unwrap();
                Ok(Self(AuthenticationToken::User(auth)))
            };
        }

        // If no header is provided, also allow extracting admin key from query param.
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct QueryParams {
            admin_key: Option<String>,
        }
        if let Query(QueryParams {
            admin_key: Some(admin_key),
        }) = parts.extract().await?
        {
            return Ok(Self(AuthenticationToken::Admin(admin_key, None)));
        }

        Ok(Self(AuthenticationToken::None))
    }
}

impl From<ExtractAuthenticationToken> for AuthenticationToken {
    fn from(token: ExtractAuthenticationToken) -> Self {
        token.0
    }
}

struct ExtractHeaderOnlyDeployKeyAuthenticationToken(AuthenticationToken);

impl<T: Sync> FromRequestParts<T> for ExtractHeaderOnlyDeployKeyAuthenticationToken {
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _st: &T,
    ) -> Result<Self, Self::Rejection> {
        let mut authorization_headers = parts.headers.get_all(http::header::AUTHORIZATION).iter();
        let Some(header) = authorization_headers.next() else {
            return Err(anyhow::anyhow!(ErrorMetadata::unauthenticated(
                "MissingDeploymentKey",
                "A deployment key Authorization header is required",
            ))
            .into());
        };
        if authorization_headers.next().is_some() {
            return Err(invalid_deployment_key_authorization_header().into());
        }
        let header = header
            .to_str()
            .map_err(|_| invalid_deployment_key_authorization_header())?;
        if strip_prefix_ignore_case(header, "convex ").is_none() {
            return Err(invalid_deployment_key_authorization_header().into());
        }
        let token =
            extract_admin_key(header).map_err(|_| invalid_deployment_key_authorization_header())?;
        Ok(Self(token))
    }
}

fn invalid_deployment_key_authorization_header() -> anyhow::Error {
    anyhow::anyhow!(ErrorMetadata::bad_request(
        "InvalidDeploymentKeyAuthorizationHeader",
        "Invalid deployment key Authorization header",
    ))
}

/// Authenticate a deployment key supplied only in the Authorization header.
/// This extractor intentionally does not inspect query parameters.
pub struct ExtractHeaderOnlyDeployKeyIdentity(pub Identity);

impl<S> FromRequestParts<S> for ExtractHeaderOnlyDeployKeyIdentity
where
    LocalAppState: FromMtState<S>,
    S: Send + Sync + Clone + 'static,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        st: &S,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .extract::<ExtractHeaderOnlyDeployKeyAuthenticationToken>()
            .await?
            .0;
        let st = LocalAppState::from_request_parts(parts, st).await?;

        Ok(Self(
            st.application
                .authenticate(token, st.application.runtime().system_time())
                .await?,
        ))
    }
}

pub struct ExtractIdentity(pub Identity);

impl<S> FromRequestParts<S> for ExtractIdentity
where
    LocalAppState: FromMtState<S>,
    S: Send + Sync + Clone + 'static,
{
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        st: &S,
    ) -> Result<Self, Self::Rejection> {
        let token: AuthenticationToken =
            parts.extract::<ExtractAuthenticationToken>().await?.into();
        let st = LocalAppState::from_request_parts(parts, st).await?;

        Ok(Self(
            st.application
                .authenticate(token, st.application.runtime().system_time())
                .await?,
        ))
    }
}

impl From<ExtractIdentity> for Identity {
    fn from(identity: ExtractIdentity) -> Self {
        identity.0
    }
}

pub struct TryExtractIdentity(pub anyhow::Result<Identity>);

impl FromRequestParts<RouterState> for TryExtractIdentity {
    type Rejection = HttpResponseError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        st: &RouterState,
    ) -> Result<Self, Self::Rejection> {
        let token = match parts.extract::<ExtractAuthenticationToken>().await {
            Ok(t) => t.into(),
            Err(e) => return Ok(Self(Err(e.into()))),
        };

        let Ok(ExtractResolvedHostname(host)) = parts.extract::<ExtractResolvedHostname>().await;

        let request_id = match parts.extract::<ExtractRequestId>().await {
            Ok(id) => id,
            Err(e) => return Ok(Self(Err(e.into()))),
        };
        let request_metadata = match parts.extract::<ExtractRequestMetadata>().await {
            Ok(m) => m.0,
            Err(e) => return Ok(Self(Err(e.into()))),
        };
        let request_context = RequestContext::new(request_id.0, request_metadata);
        Ok(Self(
            st.api.authenticate(&host, request_context, token).await,
        ))
    }
}

fn extract_admin_key(header: &str) -> anyhow::Result<AuthenticationToken> {
    let key = strip_prefix_ignore_case(header, "convex ")
        .context("Called extract_admin_key with a non-admin authorization header.")?;
    // We need to strip the unencrypted deployment type prefix ending in ':'
    // which clashes with the user impersonation logic below.
    // So theoretically this method accepts a key in the format:
    // "prod:some-depl-name123|sa67asd6a5da6d5:sd6f5sdf76dsf4ds6f4s68fd"
    // where the last part is the `acting_user_b64`.
    let key_without_prefix = remove_type_prefix_from_admin_key(key);
    // Looks for two parts split by a colon -- the first part always being the admin
    // key, and the second part being an optional base64 encoded
    // user to act as.
    match key_without_prefix.split_once(':') {
        // An admin acting as a user
        Some((key, acting_user_b64)) => {
            let attributes_s = base64::decode(acting_user_b64).context(
                ErrorMetadata::bad_request("HeaderParseFailure", "Malformed Authorization header."),
            )?;
            let attributes: UserIdentityAttributes =
                serde_json::from_slice::<serde_json::Value>(&attributes_s)
                    .context(ErrorMetadata::bad_request(
                        "HeaderParseFailure",
                        "Malformed Authorization header.",
                    ))?
                    .try_into()
                    .context(ErrorMetadata::bad_request(
                        "HeaderParseFailure",
                        "Malformed Authorization header.",
                    ))?;
            Ok(AuthenticationToken::Admin(
                key.to_string(),
                Some(attributes),
            ))
        },
        // Just an admin
        None => Ok(AuthenticationToken::Admin(key_without_prefix, None)),
    }
}

// Like `str::strip_prefix`, but ignores casing.
fn strip_prefix_ignore_case<'a>(string: &'a str, prefix: &str) -> Option<&'a str> {
    string
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))?;
    string.get(prefix.len()..)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::to_bytes,
        response::IntoResponse,
    };
    use http::{
        header::AUTHORIZATION,
        HeaderValue,
        Request,
        StatusCode,
    };

    use super::*;

    async fn extract_header_only_deploy_key(
        request: Request<()>,
    ) -> Result<AuthenticationToken, HttpResponseError> {
        let (mut parts, _) = request.into_parts();
        ExtractHeaderOnlyDeployKeyAuthenticationToken::from_request_parts(&mut parts, &())
            .await
            .map(|token| token.0)
    }

    #[tokio::test]
    async fn header_only_deploy_key_rejects_query_string_secrets_and_accepts_headers() {
        let query_only = Request::builder()
            .uri("/sensitive?adminKey=query-secret")
            .body(())
            .unwrap();
        let response = extract_header_only_deploy_key(query_only)
            .await
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("query-secret"));

        let header = Request::builder()
            .uri("/sensitive")
            .header(AUTHORIZATION, "Convex header-deploy-key")
            .body(())
            .unwrap();
        assert_eq!(
            extract_header_only_deploy_key(header).await.unwrap(),
            AuthenticationToken::Admin("header-deploy-key".to_owned(), None)
        );
    }

    #[tokio::test]
    async fn malformed_header_errors_are_constant_and_secret_free() {
        let mut request = Request::builder().uri("/sensitive").body(()).unwrap();
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_bytes(b"Convex raw-secret\xff").unwrap(),
        );
        let response = extract_header_only_deploy_key(request)
            .await
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "code": "InvalidDeploymentKeyAuthorizationHeader",
                "message": "Invalid deployment key Authorization header",
            })
        );
        assert!(!String::from_utf8_lossy(&body).contains("raw-secret"));
    }
}
