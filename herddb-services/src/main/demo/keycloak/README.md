# Keycloak demo for HerdDB OIDC authentication

This directory contains a docker-compose setup that boots a Keycloak
instance with a pre-imported `herddb` realm, ready for experimenting
with HerdDB's OAUTHBEARER / OIDC authentication.

## Start

```bash
docker compose up -d
# watch startup
docker compose logs -f keycloak
```

Keycloak admin console: http://localhost:8080 (admin / admin)

## Provisioned clients

All clients are configured with **service accounts enabled** and use
the **client_credentials** grant.

| clientId       | secret          | Purpose                        |
|----------------|-----------------|--------------------------------|
| herddb-jdbc    | jdbc-secret     | JDBC driver                    |
| herddb-file    | file-secret     | Remote file service client     |
| herddb-index   | index-secret    | Indexing service client        |
| herddb-server  | server-secret   | HerdDB server-to-server auth   |

Issuer URL: `http://localhost:8080/realms/herddb`

## Acquire a token manually

```bash
curl -s -X POST \
  -u herddb-jdbc:jdbc-secret \
  -d grant_type=client_credentials \
  http://localhost:8080/realms/herddb/protocol/openid-connect/token | jq .
```

## Configure HerdDB server

Add to `server.properties`:

```properties
oidc.enabled=true
oidc.issuer.url=http://localhost:8080/realms/herddb
```

## Configure JDBC client

```
jdbc:herddb:server:localhost:7000?\
client.auth.mech=OAUTHBEARER&\
oidc.issuer.url=http://localhost:8080/realms/herddb&\
oidc.client.id=herddb-jdbc&\
oidc.client.secret=jdbc-secret
```

## Stop

```bash
docker compose down
```

To also wipe Keycloak state (it runs in dev mode with an in-memory H2
database by default so state is lost on container restart anyway):

```bash
docker compose down -v
```

See the top-level [AUTHENTICATION.md](../../../../../AUTHENTICATION.md)
for the full reference.
