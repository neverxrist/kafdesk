use crate::types::{
    AuthLoginRequest, AuthProviderInfo, AuthSessionPublic, KeycloakSsoConfig, StoredCredentialsInfo,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

const KEYRING_SERVICE: &str = "kafdesk-sso";

pub struct AuthState {
    session: Mutex<AuthSessionPublic>,
    access_token: Mutex<Option<String>>,
    stored_username: Mutex<Option<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredMeta {
    username: String,
}

impl AuthState {
    pub fn new() -> Self {
        let s = Self {
            session: Mutex::new(AuthSessionPublic {
                provider_id: "keycloak".into(),
                username: None,
                email: None,
                expires_at: None,
                logged_in: false,
            }),
            access_token: Mutex::new(None),
            stored_username: Mutex::new(None),
        };
        if let Some(u) = load_stored_username() {
            *s.stored_username.lock() = Some(u);
        }
        s
    }

    pub fn list_providers(&self) -> Vec<AuthProviderInfo> {
        vec![
            AuthProviderInfo {
                id: "keycloak".into(),
                label: "Keycloak".into(),
                implemented: true,
                description: Some("Password grant (ROPC)".into()),
            },
            AuthProviderInfo {
                id: "oidc".into(),
                label: "OIDC".into(),
                implemented: false,
                description: Some("Generic OIDC — not implemented".into()),
            },
        ]
    }

    pub fn session(&self) -> AuthSessionPublic {
        self.session.lock().clone()
    }

    pub fn stored_credentials(&self) -> StoredCredentialsInfo {
        match self.stored_username.lock().as_ref() {
            Some(username) => {
                let has_password = keyring_get(username).map(|p| !p.is_empty()).unwrap_or(false);
                StoredCredentialsInfo {
                    username: Some(username.clone()),
                    has_password,
                }
            }
            None => StoredCredentialsInfo {
                username: None,
                has_password: false,
            },
        }
    }

    pub fn forget_credentials(&self) {
        if let Some(u) = self.stored_username.lock().take() {
            let _ = keyring_delete(&u);
        }
        let _ = fs::remove_file(creds_meta_path());
    }

    pub async fn login(&self, request: AuthLoginRequest) -> Result<AuthSessionPublic, String> {
        let method = request.method.as_deref().unwrap_or("password");
        if method != "password" {
            return Err("Only password grant SSO is supported (browser/PKCE removed)".into());
        }
        let kc = request
            .keycloak
            .as_ref()
            .ok_or("Keycloak config required")?;
        let user = request.username.ok_or("username required")?;
        let pass = match request.password.filter(|p| !p.is_empty()) {
            Some(p) => p,
            None => keyring_get(&user).ok_or("password required (or remember one in keychain)")?,
        };
        let token = password_grant(kc, &user, &pass).await?;
        if request.remember_password == Some(true) {
            keyring_set(&user, &pass)?;
            save_stored_username(&user);
            *self.stored_username.lock() = Some(user.clone());
        }
        let session = AuthSessionPublic {
            provider_id: request.provider_id,
            username: Some(user),
            email: None,
            expires_at: Some(token.expires_at),
            logged_in: true,
        };
        *self.access_token.lock() = Some(token.access_token);
        *self.session.lock() = session.clone();
        Ok(session)
    }

    pub async fn logout(&self) -> Result<(), String> {
        *self.access_token.lock() = None;
        *self.session.lock() = AuthSessionPublic {
            provider_id: "keycloak".into(),
            username: None,
            email: None,
            expires_at: None,
            logged_in: false,
        };
        Ok(())
    }
}

struct TokenResult {
    access_token: String,
    expires_at: i64,
}

async fn password_grant(
    kc: &KeycloakSsoConfig,
    username: &str,
    password: &str,
) -> Result<TokenResult, String> {
    let url = format!(
        "{}/realms/{}/protocol/openid-connect/token",
        kc.url.trim_end_matches('/'),
        kc.realm
    );
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .form(&[
            ("grant_type", "password"),
            ("client_id", kc.client_id.as_str()),
            ("username", username),
            ("password", password),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("SSO login failed: HTTP {}", res.status()));
    }
    #[derive(Deserialize)]
    struct Tok {
        access_token: String,
        #[serde(default)]
        expires_in: i64,
    }
    let tok: Tok = res.json().await.map_err(|e| e.to_string())?;
    let expires_at = chrono::Utc::now().timestamp_millis() + tok.expires_in * 1000;
    Ok(TokenResult {
        access_token: tok.access_token,
        expires_at,
    })
}

fn creds_meta_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("kafdesk")
        .join("sso-credentials.json")
}

fn load_stored_username() -> Option<String> {
    let raw = fs::read_to_string(creds_meta_path()).ok()?;
    // Migrate: old plaintext file had {username,password} — drop password, keep username.
    if let Ok(meta) = serde_json::from_str::<StoredMeta>(&raw) {
        return Some(meta.username);
    }
    #[derive(Deserialize)]
    struct Legacy {
        username: String,
        #[serde(default)]
        password: String,
    }
    let legacy: Legacy = serde_json::from_str(&raw).ok()?;
    if !legacy.password.is_empty() {
        let _ = keyring_set(&legacy.username, &legacy.password);
    }
    save_stored_username(&legacy.username);
    Some(legacy.username)
}

fn save_stored_username(username: &str) {
    let p = creds_meta_path();
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(
        p,
        serde_json::to_string(&StoredMeta {
            username: username.to_string(),
        })
        .unwrap_or_default(),
    );
}

fn keyring_set(username: &str, password: &str) -> Result<(), String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, username).map_err(|e| e.to_string())?;
    entry.set_password(password).map_err(|e| e.to_string())
}

fn keyring_get(username: &str) -> Option<String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, username).ok()?;
    entry.get_password().ok()
}

fn keyring_delete(username: &str) -> Result<(), String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, username).map_err(|e| e.to_string())?;
    match entry.delete_credential() {
        Ok(()) => Ok(()),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}
