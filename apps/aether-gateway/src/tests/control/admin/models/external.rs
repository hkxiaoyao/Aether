use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::routing::any;
use axum::{extract::Request, Router};
use http::StatusCode;

use super::super::super::{build_router_with_state, start_server, AppState};
use crate::constants::{
    GATEWAY_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER, TRUSTED_ADMIN_USER_ID_HEADER,
    TRUSTED_ADMIN_USER_ROLE_HEADER,
};

struct TestEnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for TestEnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.as_deref() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn set_test_env_var(key: &'static str, value: &str) -> TestEnvVarGuard {
    let previous = std::env::var(key).ok();
    std::env::set_var(key, value);
    TestEnvVarGuard { key, previous }
}

#[tokio::test]
async fn gateway_handles_admin_external_models_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/models/external",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let external_source = Router::new().route(
        "/api.json",
        any(|_request: Request| async move {
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "openai": {
                        "name": "OpenAI",
                        "models": {
                            "gpt-5": {
                                "name": "GPT-5"
                            }
                        }
                    }
                })),
            )
        }),
    );
    let (external_source_url, external_source_handle) = start_server(external_source).await;
    let _guard = set_test_env_var(
        "AETHER_GATEWAY_EXTERNAL_MODELS_URL",
        &format!("{external_source_url}/api.json"),
    );
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/admin/models/external"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["openai"]["official"], serde_json::json!(true));
    assert_eq!(
        payload["openai"]["models"]["gpt-5"]["name"],
        serde_json::json!("GPT-5")
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    external_source_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_clears_admin_external_models_cache_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/models/external/cache",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/api/admin/models/external/cache"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["cleared"], false);
    assert_eq!(payload["message"], "缓存不存在");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
