from flask_appbuilder.security.manager import AUTH_OID

# Authentication type
AUTH_TYPE = AUTH_OID
OID_ID_TOKEN_COOKIE_SECURE = False

# Disable anonymous/public role
PUBLIC_ROLE_LIKE_GAMMA = False  

# OIDC / OAuth2 provider (Keycloak)
OAUTH_PROVIDERS = [{
    'name': 'keycloak',
    'token_key': 'access_token',  # How to extract access token
    'icon': 'fa-key',
    'remote_app': {
        'client_id': 'superset',
        'client_secret': '<secret>',  # match Keycloak client
        'api_base_url': 'http://keycloak:8080/realms/<realm>/protocol/openid-connect',
        'request_token_url': None,
        'access_token_url': 'http://keycloak:8080/realms/<realm>/protocol/openid-connect/token',
        'authorize_url': 'http://keycloak:8080/realms/<realm>/protocol/openid-connect/auth',
        'client_kwargs': {
            'scope': 'openid email profile'
        }
    }
}]

