pub(crate) type AdminGatewayProviderTransportSnapshot =
    crate::provider_transport::GatewayProviderTransportSnapshot;
pub(crate) type AdminLocalOAuthRefreshError = crate::provider_transport::LocalOAuthRefreshError;
pub(crate) type AdminKiroRequestAuth = crate::provider_transport::kiro::KiroRequestAuth;
pub(crate) type AdminKiroAuthConfig = crate::provider_transport::kiro::KiroAuthConfig;
pub(crate) type AdminKiroOAuthRefreshAdapter =
    crate::provider_transport::kiro::KiroOAuthRefreshAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdminProviderOAuthTemplate {
    pub(crate) provider_type: String,
    pub(crate) display_name: String,
    pub(crate) authorize_url: String,
    pub(crate) token_url: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) redirect_uri: String,
    pub(crate) use_pkce: bool,
}

impl AdminProviderOAuthTemplate {
    pub(crate) fn from_fixed(
        template: crate::provider_transport::provider_types::ProviderOAuthTemplate,
    ) -> Self {
        Self {
            provider_type: template.provider_type.to_string(),
            display_name: template.display_name.to_string(),
            authorize_url: template.authorize_url.to_string(),
            token_url: template.token_url.to_string(),
            client_id: template.client_id.to_string(),
            client_secret: template.client_secret.to_string(),
            scopes: template
                .scopes
                .iter()
                .map(|scope| scope.to_string())
                .collect(),
            redirect_uri: template.redirect_uri.to_string(),
            use_pkce: template.use_pkce,
        }
    }

    pub(crate) fn from_provider_plugin(
        template: aether_provider_plugin::ProviderPluginOAuthTemplate,
    ) -> Self {
        Self {
            provider_type: template.provider_type,
            display_name: template.display_name,
            authorize_url: template.authorize_url,
            token_url: template.token_url,
            client_id: template.client_id,
            client_secret: template.client_secret.unwrap_or_default(),
            scopes: template.scopes,
            redirect_uri: template.redirect_uri,
            use_pkce: template.use_pkce,
        }
    }
}

pub(crate) fn is_fixed_provider_type_for_admin_oauth(provider_type: &str) -> bool {
    crate::provider_transport::provider_types::provider_type_is_fixed_for_admin_oauth(provider_type)
}

pub(crate) fn admin_provider_oauth_template(
    provider_type: &str,
) -> Option<AdminProviderOAuthTemplate> {
    crate::provider_transport::provider_types::provider_type_admin_oauth_template(provider_type)
        .map(AdminProviderOAuthTemplate::from_fixed)
}

pub(crate) fn admin_provider_oauth_template_types() -> impl Iterator<Item = &'static str> {
    crate::provider_transport::provider_types::ADMIN_PROVIDER_OAUTH_TEMPLATE_TYPES
        .iter()
        .copied()
}
