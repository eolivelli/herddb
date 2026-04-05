# HerdDB Authentication Guide

HerdDB supports multiple authentication mechanisms for the JDBC wire protocol
(client &rarr; server and server &rarr; server) and for the gRPC services
(remote file service, indexing service):

| Mechanism | Wire | Token format | Use case |
|---|---|---|---|
| No auth | JDBC (local mode) | — | Embedded/in-JVM usage |
| Basic (admin.username / admin.password) | JDBC | username+password | Dev, quick start |
| File-based users | JDBC | username+password from CSV | Small static user lists |
| SASL PLAIN | JDBC | username+password | Simple; transport should be TLS |
| SASL DIGEST-MD5 (default) | JDBC | hashed username+password | Password challenge-response |
| SASL GSSAPI / Kerberos | JDBC | keytab/ticket | Enterprise Kerberos deployments |
| **SASL OAUTHBEARER (OIDC/JWT)** | JDBC, gRPC | signed JWT bearer token | Centralized IdP (Keycloak, Okta, Auth0...) |

Mechanisms can coexist: the server decides based on the `client.auth.mech`
value sent by the client at handshake time.

---

## 1. Basic admin credentials

The default user manager is `SimpleSingleUserManager`: one hard-coded user
whose credentials are configured at server startup.

```properties
# server.properties
admin.username=sa
admin.password=hdb
```

JDBC URL:
```
jdbc:herddb:server:localhost:7000
# user=sa password=hdb passed via JDBC Properties or URL query
```

---

## 2. File-based users

Load users from a CSV file (`username,password,role`). Only the role
`admin` is currently recognized.

```properties
# server.properties
server.users.file=conf/users.csv
```

`users.csv`:
```
alice,alice-password,admin
bob,bob-password,admin
```

---

## 3. SASL PLAIN

Username/password sent in plaintext. **Always use TLS** with this mechanism.
Server-side, passwords come from the configured `UserManager`.

Client:
```
jdbc:herddb:server:localhost:7000?client.auth.mech=PLAIN
# + user=sa password=hdb
```

---

## 4. SASL DIGEST-MD5 (default)

Password is verified via a digest challenge-response, so the plaintext
never crosses the wire. Selected by default when `client.auth.mech` is
not set.

```
jdbc:herddb:server:localhost:7000
# + user=sa password=hdb
```

---

## 5. SASL GSSAPI / Kerberos

Requires a JAAS configuration and a keytab.

**Server JAAS** (`jaas.conf`):
```
HerdDBServer {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/herddb/herddb.keytab"
    storeKey=true
    useTicketCache=false
    principal="herddb/server.example.com@EXAMPLE.COM";
};
```

**Client JAAS**:
```
HerdDBClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/herddb/client.keytab"
    storeKey=true
    principal="alice@EXAMPLE.COM";
};
```

Start with `-Djava.security.auth.login.config=/path/to/jaas.conf`.
The client sends `client.auth.mech=GSSAPI` transparently when JAAS
login succeeds.

---

## 6. SASL OAUTHBEARER (OIDC/JWT) — *new*

HerdDB integrates with any OIDC provider (Keycloak, Okta, Auth0,
Google Identity, Azure AD, etc). Clients obtain a JWT access token via
the **OAuth2 client_credentials grant** and present it to the server
through the **OAUTHBEARER** SASL mechanism (RFC 7628). The server
validates the JWT signature against the provider's JWKS, checks
`iss`/`exp`/`aud`, and maps a configurable claim to the HerdDB
principal.

### 6.1 Server configuration

```properties
# Enable OIDC validation
oidc.enabled=true
# Issuer URL — HerdDB performs .well-known/openid-configuration discovery
oidc.issuer.url=https://keycloak.example.com/realms/herddb
# Optional — expected audience claim in incoming tokens
oidc.audience=herddb-api
# Optional — claim containing the HerdDB principal (default: preferred_username, fallback: sub)
oidc.username.claim=preferred_username
# Optional — explicit JWKS URI (skips discovery)
# oidc.jwks.uri=https://keycloak.example.com/realms/herddb/protocol/openid-connect/certs
```

When `oidc.enabled=true`:
- The server fetches the provider's JWKS (cached and auto-refreshing).
- The OAUTHBEARER SASL mechanism is registered.
- Clients connecting with `client.auth.mech=OAUTHBEARER` are authenticated
  by validating the supplied bearer token; `UserManager` is **not**
  consulted.
- Other SASL mechanisms (PLAIN, DIGEST-MD5, GSSAPI) continue to work for
  password-based users — the server picks based on the client's
  `client.auth.mech`.
- If `oidc.enabled=true` but `oidc.issuer.url` is empty, the server
  **fails fast** at startup.

### 6.2 JDBC client configuration

Option A — the client obtains tokens itself via client_credentials:

```
jdbc:herddb:server:herddb.example.com:7000?\
client.auth.mech=OAUTHBEARER&\
oidc.issuer.url=https://keycloak.example.com/realms/herddb&\
oidc.client.id=herddb-jdbc&\
oidc.client.secret=<secret>&\
oidc.scope=herddb
```

Option B — pass a pre-acquired token directly (useful when an external
agent manages token lifecycle):

```
jdbc:herddb:server:herddb.example.com:7000?\
client.auth.mech=OAUTHBEARER&\
oidc.token=<jwt>
```

The same properties are accepted via `java.util.Properties` passed to
`DriverManager.getConnection(url, props)`.

### 6.3 Server-to-server authentication

For leader/follower and inter-cluster connections, each HerdDB server
acts as an OIDC client with its own client_credentials:

```properties
oidc.enabled=true
oidc.issuer.url=https://keycloak.example.com/realms/herddb
server.oidc.client.id=herddb-server-1
server.oidc.client.secret=<secret>
```

### 6.4 File server (gRPC)

```properties
# file-server.properties
auth.oidc.enabled=true
auth.oidc.issuer.url=https://keycloak.example.com/realms/herddb
auth.oidc.audience=herddb-file
auth.oidc.username.claim=preferred_username
```

Clients attach the token via a `JwtAuthClientInterceptor`:

```java
OidcConfiguration cfg = new OidcConfiguration(issuerUrl).discover();
OidcTokenProvider tp = new OidcTokenProvider(cfg, "file-client", "secret", null);
RemoteFileServiceClient client = new RemoteFileServiceClient(
        List.of("file-server-1:9845"),
        Map.of(),
        new JwtAuthClientInterceptor(() -> {
            try { return tp.getToken(); } catch (Exception e) { throw new RuntimeException(e); }
        }));
```

### 6.5 Indexing service (gRPC)

Identical setup to the file server, with `auth.oidc.*` keys in
`IndexingServerConfiguration`:

```properties
auth.oidc.enabled=true
auth.oidc.issuer.url=https://keycloak.example.com/realms/herddb
```

```java
IndexingServiceClient client = new IndexingServiceClient(
        List.of("index-1:9847"), 30,
        new JwtAuthClientInterceptor(() -> tp.getToken()));
```

### 6.6 Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Server returned ERROR during SASL negotiation` | Server rejected the token — check issuer/audience/expiry |
| `UNAUTHENTICATED: missing authorization header` | gRPC client interceptor not installed |
| `UNAUTHENTICATED: expected Bearer scheme` | Non-Bearer Authorization header attached |
| `token endpoint returned HTTP 401` | Wrong `oidc.client.id` / `oidc.client.secret` |
| `JWT iss claim has value ... required ...` | Token obtained from a different issuer than the server trusts |
| `Discovery issuer mismatch` | `iss` in `.well-known/openid-configuration` doesn't match `oidc.issuer.url` |
| Server startup: `OIDC authentication enabled but oidc.issuer.url is not set` | `oidc.enabled=true` without the companion URL |

---

## Demo: Keycloak with docker-compose

See `herddb-services/src/main/demo/keycloak/` for a docker-compose setup
that boots Keycloak and a ready-to-use `herddb` realm.
