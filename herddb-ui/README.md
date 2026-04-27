# herddb-ui — HerdDB Web UI v2

Single-module home for the HerdDB Web UI:

* `src/main/java/org/herddb/ui/...` — Java REST surface (JAX-RS, mounted
  at `/api/v2/...` on the same Jetty embedded by `ServerMain`).
* `src/frontend/` — React + TypeScript SPA built with Vite and tested
  with Vitest + React Testing Library.
* `index.html`, `vite.config.ts`, `tsconfig.json`, `package.json` —
  Vite/TS configuration at the module root (Vite expects them there).

The legacy AngularJS UI was removed in phase 1 of issue
[#301](https://github.com/eolivelli/herddb/issues/301); the new SPA
talks to the server through internal `DBManager` APIs (no JDBC) and
runs side-by-side with the REST endpoints inside the same WAR.

## Build

`mvn install` from the repository root builds everything. Inside the
`herddb-ui` module it runs:

1. `frontend-maven-plugin install-node-and-npm` — downloads Node + npm
   into `target/node-toolchain/` (pinned versions live in `pom.xml`).
2. `npm install` — installs the JS dependencies into `node_modules/`.
3. `npm run build` — `tsc -b && vite build`, output goes to
   `target/web-build/spa/`.
4. `maven-war-plugin` — packages `WEB-INF/web.xml` (from
   `src/main/webapp/WEB-INF`) and the SPA bundle (overlaid from
   `target/web-build/spa`) into the WAR.
5. During the `test` phase, `npm test` runs `vitest run` and the Java
   surefire tests boot a real embedded `Server` to exercise each REST
   resource.

To run only the herddb-ui module:

```
mvn -pl herddb-ui -am install -Dmaven.repo.local=$MAVEN_REPO
```

## Frontend dev loop

For fast iteration on the SPA against a live HerdDB server:

```
# 1. start a HerdDB server (any flavour, e.g. herddb-services zip).
#    It must serve /ui/api/v2/... — that is the default.
bin/service server start

# 2. in another terminal, run vite dev:
cd herddb-ui
npm run dev
```

Vite serves the SPA at <http://localhost:5173/ui/> and proxies
`/ui/api/v2/*` to the backend at `http://localhost:9845` (override with
the `HERDDB_DEV_SERVER_URL` environment variable). Hot-reload works for
React components and the CSS theme.

## Testing

Backend (Java):

```
mvn -pl herddb-ui test -Dmaven.repo.local=$MAVEN_REPO
```

This boots an in-process `Server` per test class. The hybrid test
strategy (CLAUDE.md / phase-3 plan) combines fast direct-call resource
tests with a small layer of HTTP smoke tests that exercise the full
JAX-RS + Jackson + Jetty stack via `jersey-test-framework-provider-jetty`.

Frontend (Vitest + React Testing Library):

```
cd herddb-ui
npm test          # one-shot
npm run dev       # watch mode, used during development
```

`npm test` also runs as part of `mvn test` via `frontend-maven-plugin`.

## REST surface (v2)

All endpoints live under `/api/v2/...`:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe (status, node id, timestamp). |
| GET | `/server-info` | Node id, deployment mode, JDBC URL, default tablespace. |
| GET | `/tablespaces` | List all visible tablespaces. |
| GET | `/tablespaces/{name}` | Single tablespace, 404 if absent. |
| GET | `/tablespaces/{ts}/tables` | List of tables (joins `systables` + `systablestats`). |
| GET | `/tablespaces/{ts}/tables/{name}` | Detail (columns, indexes, FKs, stats). |
| GET | `/tablespaces/{ts}/tables/{name}/data-pages` | Active-page snapshot for a table. |
| GET | `/tablespaces/{ts}/tables/{name}/primary-index` | Primary-key index summary. |
| GET | `/tablespaces/{ts}/tables/{name}/indexes/{idx}` | Index detail (BRIN block layout when applicable). |
| GET | `/indexing-services` | Vector indexes overview (parses `sysindexstatus.properties`). |
| GET | `/indexing-services/{indexName}` | Vector-index detail. |

User-supplied path segments are validated against `[A-Za-z0-9_]+`;
unknown identifiers return 404; illegal characters return 400. SQL is
issued through `org.herddb.ui.internal.QueryService`, which only
permits `SELECT` statements.

## Wiring

`ServerMain` (in `herddb-services`) starts the embedded Jetty and
exposes the SPA at `/ui` (a `WebAppContext` rooted at `web/ui` after
the assembly unpacks the WAR). The running `Server` is bound on the
webapp's `ServletContext` under the attribute `herddb.server`, and the
v2 resources resolve it via HK2 in
`org.herddb.ui.api.v2.ApplicationConfigV2`.
