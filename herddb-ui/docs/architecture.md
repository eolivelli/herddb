# HerdDB Web UI v2 — Architecture & REST API Reference

This document describes how the HerdDB Web UI is implemented, how it is
plugged into the running server, and the full REST surface it exposes.
The code lives entirely under the `herddb-ui` Maven module and is
shipped inside the `herddb-services` zip — there is no separate
process, no extra port and no JDBC driver embedded in the WAR.

> **Audience:** maintainers extending the UI or its REST surface,
> packagers debugging the bundled assets, and operators who need to
> understand what is exposed at `/ui/...` on a running HerdDB server.

---

## 1. Module layout

```
herddb-ui/
├── pom.xml                          # WAR packaging + frontend-maven-plugin
├── package.json                     # SPA build scripts and JS dependencies
├── package-lock.json
├── tsconfig.json                    # TypeScript strict config
├── vite.config.ts                   # Vite + Vitest configuration
├── index.html                       # Vite entry HTML (lives at the module root)
├── README.md
├── docs/
│   └── architecture.md              # this document
├── src/
│   ├── main/
│   │   ├── java/org/herddb/ui/
│   │   │   ├── api/v2/              # JAX-RS resources (this is the public REST API)
│   │   │   ├── dto/                 # Jackson DTOs (wire shapes)
│   │   │   └── internal/            # ServerLocator + QueryService gateway
│   │   └── webapp/
│   │       └── WEB-INF/web.xml      # Servlet + Jersey wiring
│   ├── frontend/                    # React + TypeScript SPA sources
│   │   ├── App.tsx                  # Top-level routing
│   │   ├── main.tsx                 # ReactDOM bootstrap
│   │   ├── api/client.ts            # fetch() wrapper + DTOs
│   │   ├── components/              # Header, AsyncResource, TablespaceSelector
│   │   ├── contexts/                # TablespaceContext (global selector state)
│   │   ├── pages/                   # Route-level components
│   │   ├── visualizations/          # Visx-powered charts
│   │   ├── theme/theme.css          # Dark CSS theme
│   │   ├── test/setup.ts            # Vitest setup (ResizeObserver polyfill)
│   │   ├── test/fixtures.ts         # Reusable mock data
│   │   └── vite-env.d.ts            # `/// <reference types="vite/client" />`
│   └── test/java/org/herddb/ui/     # JUnit 4 tests (direct-call + Jersey-Jetty smoke)
└── target/
    ├── node-toolchain/              # Node + npm downloaded by frontend-maven-plugin
    ├── web-build/spa/               # Vite output, copied into the WAR
    ├── herddb-ui-…war               # production WAR
    └── herddb-ui-…war-no-libs.war   # WAR without WEB-INF/lib (used by herddb-services)
```

The dev workflow is documented in `herddb-ui/README.md`. The rest of
this document focuses on the architecture and the REST surface.

---

## 2. How the UI is plugged into the running server

### 2.1 Embedded Jetty in `ServerMain`

`ServerMain` (in `herddb-services`) starts a single embedded Jetty
listener on `http.host:http.port` (default `0.0.0.0:9845`) with two
context handlers:

| Context path | Handler | Contents |
|--------------|---------|----------|
| `/`          | `ServletContextHandler` (gzip) | `/metrics` Prometheus servlet |
| `/ui`        | `WebAppContext`                | Unpacked `herddb-ui` WAR — SPA assets and the JAX-RS application |

The `/ui` `WebAppContext` is created from `web/ui` (a directory inside
the `herddb-services` zip — the assembly descriptor unpacks the WAR
there at build time). When that directory is missing (e.g. the bare
`herddb-core` jar is run by hand), `ServerMain` logs a warning and
skips registering the UI; the rest of the server is unaffected.

### 2.2 Binding the running `Server` to the webapp

The v2 REST resources need a reference to the running `Server` so they
can talk to `DBManager` and the table managers without going through
the network or JDBC. `ServerMain` makes that reference available by
binding it on the webapp's `ServletContext` attribute map:

```java
WebAppContext webApp = new WebAppContext(new File("web/ui").getAbsolutePath(), "/ui");
webApp.setAttribute("herddb.server", server);
contexts.addHandler(webApp);
```

The attribute key matches `org.herddb.ui.internal.ServerLocator.SERVLET_ATTRIBUTE`.
A `ServerLocator` factory then exposes that lookup to JAX-RS via HK2
dependency injection:

```java
@ApplicationPath("/api/v2")
public class ApplicationConfigV2 extends ResourceConfig {

    public ApplicationConfigV2() {
        register(JacksonFeature.class);
        register(HealthResource.class);
        register(TablespacesResource.class);
        register(ServerInfoResource.class);
        register(TablesResource.class);
        register(IndexesResource.class);
        register(IndexingServicesResource.class);
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bindFactory(ServletContextServerLocatorFactory.class)
                        .to(ServerLocator.class)
                        .in(Singleton.class);
            }
        });
    }
}
```

`ServletContextServerLocatorFactory` reads the attribute on demand and
returns a `ServerLocator` wrapping the live `Server`. Resources that
need it declare it as an `@Inject` constructor parameter — see for
example `HealthResource(ServerLocator)`.

### 2.3 Servlet wiring

`herddb-ui/src/main/webapp/WEB-INF/web.xml` registers the Jersey
container at `/api/v2/*`:

```xml
<servlet>
  <servlet-name>HerdDB-WebUI-v2</servlet-name>
  <servlet-class>org.glassfish.jersey.servlet.ServletContainer</servlet-class>
  <init-param>
    <param-name>javax.ws.rs.Application</param-name>
    <param-value>org.herddb.ui.api.v2.ApplicationConfigV2</param-value>
  </init-param>
  <load-on-startup>1</load-on-startup>
</servlet>
<servlet-mapping>
  <servlet-name>HerdDB-WebUI-v2</servlet-name>
  <url-pattern>/api/v2/*</url-pattern>
</servlet-mapping>
```

So the public surface is **`http://<host>:9845/ui/api/v2/...`**. SPA
asset URLs (`index.html`, `assets/*.js`, `assets/*.css`) are served by
Jetty's default servlet at `http://<host>:9845/ui/`.

### 2.4 Read-only gateway: `QueryService`

Most endpoints fetch their data by issuing `SELECT` statements against
HerdDB's virtual `sys*` tables. Rather than hand each resource a raw
`DBManager`, the UI module funnels every query through
`org.herddb.ui.internal.QueryService`, which:

* runs the query through `DBManager.executeSimpleQuery` with
  `TransactionContext.NO_TRANSACTION` and a default
  `StatementEvaluationContext`;
* caps the result set at 10 000 rows by default;
* materialises the `DataAccessor`s into
  `List<Map<String, Object>>` with column names lower-cased so DTOs do
  not have to worry about case-folding rules;
* **rejects any non-`SELECT` statement** — comment-only inputs, blank
  text, `INSERT`/`UPDATE`/`DELETE`/`DROP`/etc. all throw
  `IllegalArgumentException`. This is defence-in-depth on top of the
  fact that the resource layer never emits anything but
  `SELECT`s itself.

This gateway is the single choke point that the unit-test suite leans
on: any future endpoint that goes through `QueryService` automatically
gets the SELECT-only guarantee.

### 2.5 Path-segment validation

Every resource that interpolates a user-supplied path segment into an
SQL identifier (for example `/api/v2/tablespaces/{ts}/tables/{name}`)
runs that segment through the regex `[A-Za-z0-9_]+` before using it.
Illegal characters return HTTP 400 `Bad Request`; unknown valid
identifiers return HTTP 404 `Not Found`. Combined with the SELECT-only
guard in `QueryService`, this closes the SQL-injection surface that
would otherwise come from string-concatenation.

---

## 3. Frontend implementation

### 3.1 Stack

| Concern | Choice | Notes |
|---------|--------|-------|
| Framework | React 18 + TypeScript (strict mode) | `tsconfig.json` enables `noUnusedLocals`, `noUnusedParameters`, `noFallthroughCasesInSwitch`. |
| Build tool | Vite 5 (`base: '/ui/'`) | Output lives at `target/web-build/spa/`. |
| Routing | React Router v6 (`BrowserRouter` with `basename={import.meta.env.BASE_URL}`) | Production base is `/ui/`, so client-side routes live under `/ui/...`. |
| State | React context (`TablespaceContext`) | Global tablespace selector + the lazy `useAsync` hook. |
| Charts | Visx (`@visx/group`, `@visx/responsive`, `@visx/scale`, `@visx/shape`, `@visx/text`, `@visx/hierarchy`) + `d3-hierarchy` | D3 building blocks wrapped in React components. |
| Styling | Hand-written CSS variables in `theme/theme.css` (no Tailwind, no Bootstrap) | Dark theme inspired by the slate / sky palette. |
| Testing | Vitest 2 + React Testing Library + jsdom 25 | `src/frontend/test/setup.ts` polyfills `ResizeObserver` for Visx components. |

### 3.2 Routing

The entire route table lives in `src/frontend/App.tsx`:

| Path | Component | Phase |
|------|-----------|-------|
| `/` | `DashboardPage` | 2 |
| `/tablespaces` | `TablespacesPage` | 2 |
| `/tablespaces/:tablespace/tables` | `TablesPage` | 3 |
| `/tablespaces/:tablespace/tables/:name` | `TableDetailPage` | 3 |
| `/tablespaces/:tablespace/tables/:name/data-pages` | `DataPagesPage` | 4 |
| `/tablespaces/:tablespace/tables/:name/primary-index` | `PrimaryIndexPage` | 4 |
| `/tablespaces/:tablespace/tables/:name/indexes/:index` | `IndexDetailPage` | 4 |
| `/indexing-services` | `IndexingServicesPage` | 5 |
| `*` | `<Navigate to="/" replace />` | 2 |

### 3.3 Tablespace selector

`TablespaceContext` is the single source of truth for the currently
selected tablespace.

* It loads `GET /api/v2/tablespaces` on mount and exposes the list,
  the selected name, and a `setSelected` setter.
* The default selection is `"herd"` (`DEFAULT_TABLESPACE`). If the
  configured default is not visible at load time, the context falls
  back to the first tablespace in the response.
* `<TablespaceSelector />` (rendered inside `<Header />`) reads the
  context and renders a `<select>`; navigating between routes does not
  reset the selection.
* For tests, the provider takes an optional `loader` prop so unit
  tests inject deterministic fixture lists without touching `fetch`.

### 3.4 API client (`src/frontend/api/client.ts`)

The client is intentionally tiny — it is just a typed wrapper around
`fetch` plus one helper, `apiUrl`, which composes the right URL from
Vite's `BASE_URL`:

```ts
export function apiUrl(path: string): string {
    const base = import.meta.env.BASE_URL.replace(/\/$/, '');
    const suffix = path.startsWith('/') ? path : `/${path}`;
    return `${base}/api/v2${suffix}`;
}
```

In production `BASE_URL` is `/ui/`, so `apiUrl('/health')` resolves to
`/ui/api/v2/health`. In development the same pattern works because
`vite.config.ts` proxies `/ui/api/*` to the backend running locally on
`http://localhost:9845` (override via `HERDDB_DEV_SERVER_URL`).

The client exposes a single `HerdDbApi` object whose methods map 1-to-1
to the REST endpoints. Every method returns a `Promise` of a strongly
typed DTO; the DTO interfaces are intentionally co-located with the
client so a single import block (`import { HerdDbApi, type FooDTO }`)
covers both the call and the wire shape.

### 3.5 The `useAsync` hook

`src/frontend/components/AsyncResource.ts` provides a 30-line
"load this once on mount, cancel stale results" hook used by every
data-driven page:

```ts
export function useAsync<T>(loader: () => Promise<T>, deps: ReadonlyArray<unknown>):
    AsyncState<T>
```

Re-running depends on an explicit `deps` array (the React-Hooks lint
rule is suppressed at the call site because the caller controls the
dependency list). The hook returns `{ data, loading, error }` and
guarantees that an early `loader()` whose promise resolves after a
later one cannot overwrite the latter — by tracking a local
`cancelled` flag in the effect closure.

### 3.6 Visualisations (Visx)

There are three reusable visualisation components under
`src/frontend/visualizations/`:

* **`DataPagesView`** — heatmap over the active data pages of a
  table. Cell size auto-fits the container; cell colour encodes the
  page size on a linear scale; a yellow border distinguishes pages
  resident in the in-memory page cache from those that have been
  evicted. Hovering a cell shows a tooltip with the page id, average
  record size, dirt counter and loaded flag.
* **`BrinIndexView`** — bar chart of BRIN block entry counts. Bars
  are ordered by `blockId`; height encodes `entries`; fill colour is
  blue (clean) or red (dirty); border colour is yellow (loaded) or
  slate (evicted).
* **`VectorCountsView`** — bar chart of vector counts per index in
  the indexing-services overview. Fill colour is colour-coded by
  status — green for `ready`, blue for any other in-progress status,
  red when an `error` field is present.

All three use `@visx/responsive`'s `<ParentSize />` so they re-flow
smoothly on container resize. The Vitest setup provides a no-op
`ResizeObserver` polyfill (jsdom does not implement it) so these
components also render in unit tests.

### 3.7 Theme

`src/frontend/theme/theme.css` defines a single CSS-variable palette:

```css
:root {
    --herd-bg: #0f172a;          /* page background */
    --herd-panel: #1e293b;        /* header / cards */
    --herd-text: #e2e8f0;
    --herd-muted: #94a3b8;
    --herd-accent: #38bdf8;       /* primary action / chart fill */
    --herd-accent-strong: #0ea5e9;
    --herd-error: #ef4444;
    --herd-border: #334155;
    --herd-success: #4ade80;
    --herd-mono: ui-monospace, …;
}
```

There is no global CSS reset beyond `box-sizing: border-box` and
`html, body, #root { height: 100%; margin: 0 }`. Components apply
`herd-*` BEM-ish class names (`.herd-page`, `.herd-table`,
`.herd-error`, `.herd-badge`, `.herd-viz`, `.herd-selector`, …).

### 3.8 Build & packaging

`herddb-ui/pom.xml` orchestrates everything via three Maven plugins:

1. **`com.github.eirslett:frontend-maven-plugin`** runs four
   executions during `generate-resources`:
   * `install-node-and-npm` (Node 20.11.1, npm 10.2.4 — pinned in
     `pom.xml` properties).
   * `npm install --no-audit --no-fund`.
   * `npm run build` (runs `tsc -b && vite build`).
   * a fifth execution at the `test` phase runs `vitest run`.
2. **`org.apache.maven.plugins:maven-war-plugin`** packages the WAR
   with a `<webResources>` overlay copying
   `target/web-build/spa/` (the Vite output) into the WAR root,
   alongside the `WEB-INF/web.xml` from `src/main/webapp/`. Two WARs
   are produced: the regular one and a `-war-no-libs` classifier (no
   `WEB-INF/lib/`) used by the `herddb-services` assembly.
3. **`org.apache.rat:apache-rat-plugin`** is configured to skip
   `node_modules/**`, `package-lock.json` and `*.tsbuildinfo` so the
   license-header check does not fail on Vite/npm artefacts.

The `herddb-services` assembly descriptor unpacks the
`-war-no-libs` artifact into `web/ui/` inside the zip; runtime classes
needed by the JAX-RS application (the `-classes` JAR) are added to the
services classpath instead of `WEB-INF/lib/`.

### 3.9 Testing

Frontend tests live next to the components they cover and run under
Vitest. There are 17 tests across:

* `api/client.test.ts` — `apiUrl` URL composition and `fetch` error
  handling;
* `components/TablespaceSelector.test.tsx` — default selection,
  fallback when `herd` is missing, user-driven selection change,
  loader error;
* `pages/TablesPage.test.tsx` — list rendering, empty state, error
  state;
* `pages/TableDetailPage.test.tsx` — full detail rendering, loader
  error;
* `pages/DataPagesPage.test.tsx`,
  `pages/IndexDetailPage.test.tsx`,
  `pages/IndexingServicesPage.test.tsx` — heading + key wire-shape
  values present, empty + error states.

`src/frontend/test/setup.ts` imports
`@testing-library/jest-dom/vitest` (so `toBeInTheDocument` etc. are
available) and polyfills `ResizeObserver`. `src/frontend/test/fixtures.ts`
provides reusable sample DTO arrays used across multiple test files.

---

## 4. Backend implementation

### 4.1 Java packages

| Package | Role |
|---------|------|
| `org.herddb.ui.api.v2` | JAX-RS resource classes (`@Path`-annotated) |
| `org.herddb.ui.dto` | Jackson-serialisable POJOs (wire shapes) |
| `org.herddb.ui.internal` | `ServerLocator`, `QueryService` (not part of the public API) |

### 4.2 Resource classes

| Class | Mounted at | Phase |
|-------|------------|-------|
| `HealthResource` | `/api/v2/health` | 1 |
| `ServerInfoResource` | `/api/v2/server-info` | 2 |
| `TablespacesResource` | `/api/v2/tablespaces` | 1 / 2 |
| `TablesResource` | `/api/v2/tablespaces/{ts}/tables` | 3 |
| `IndexesResource` | `/api/v2/tablespaces/{ts}/tables/{table}` | 4 |
| `IndexingServicesResource` | `/api/v2/indexing-services` | 5 |

All resources are stateless; Jersey instantiates them per request.
They take their dependencies (`ServerLocator`, `QueryService`) as
`@Inject` constructor parameters wired by the HK2 binder registered in
`ApplicationConfigV2`. Every resource also exposes a test-friendly
constructor that accepts an explicit dependency, which lets the unit
test suite bypass HK2 entirely and call methods directly.

### 4.3 DTOs (`org.herddb.ui.dto`)

* **`HealthDTO`** — `{ status, nodeId, timestamp }`.
* **`ServerInfoDTO`** — `{ nodeId, mode, jdbcUrl, defaultTablespace }`.
* **`TablespaceDTO`** — one row of `systablespaces`.
* **`TableSummaryDTO`** — `systables` joined with `systablestats`.
* **`TableDetailDTO`** — `summary`, `columns`, `indexes`,
  `foreignKeys`.
* **`ColumnDTO`**, **`IndexSummaryDTO`**, **`ForeignKeyDTO`** — used
  inside `TableDetailDTO`.
* **`DataPageInfoDTO`** + nested **`PageLayoutDTO`** — per-page
  snapshot plus aggregated counters.
* **`PrimaryIndexDTO`** — `{ type, entries, loadedNodes,
  usedMemoryBytes }`.
* **`IndexDetailDTO`** — `summary` + (for BRIN) a list of nested
  **`BrinBlockDTO`**s.
* **`IndexingServiceIndexDTO`** + container
  **`IndexingServicesOverviewDTO`** — vector-index status.

All DTOs ship default constructors and getters/setters so Jackson can
both serialise (server) and deserialise (tests, JerseyClient) them
without custom mixins.

### 4.4 Internal helpers added on `herddb-core` and `herddb-utils`

The phase-4 visualisation endpoints required three new read-only
snapshot helpers. None of them mutate state, none of them block, and
none of them force a checkpoint:

* **`TableManager.snapshotPagesLayout()`** — walks
  `PageSet.getActivePagesView()` (an unmodifiable, weakly-consistent
  view backed by a `ConcurrentHashMap`) and returns a list of
  `DataPageLayoutInfo` records (`pageId`, `sizeBytes`,
  `averageRecordSize`, `dirtBytes`, `loaded`). The "loaded" bit is
  derived from the live `pages` map.
* **`PageSet.DataPageMetaData`** — three new public accessors
  (`getSize`, `getAverageRecordSize`, `getDirtBytes`) so the snapshot
  builder above can read them without exposing the internal
  `LongAdder`.
* **`BLinkKeyToPageIndex.snapshotInfo()`** and
  **`IncrementalBLinkKeyToPageIndex.snapshotInfo()`** — return a
  `PrimaryIndexSnapshot` with `entries`, `loadedNodes`,
  `usedMemoryBytes`. Both implementations exist because the in-memory
  `local`-mode storage uses the former and the file-backed
  `standalone`/`cluster` modes use the latter.
* **`BlockRangeIndex.snapshotBlocks()`** and
  **`BRINIndexManager.snapshotBlocks()`** — return a list of
  `BlockSnapshot`s with `blockId`, `pageId`, `entries`, `loaded`,
  `dirty` derived from a non-locking walk of the `blocks`
  `ConcurrentNavigableMap`.

The hammer suite was run twice (per the CLAUDE.md guidance) after these
helpers landed; both passes were green.

### 4.5 Backend testing strategy

The backend is verified by **35 JUnit 4 tests** organised as:

* **Direct-call tests** that boot a real `Server` (in `local` or
  `standalone` mode via the `EmbeddedHerdDbServerRule` JUnit rule),
  instantiate the resource with explicit dependencies and assert
  return values. These are the bulk of coverage and run in seconds.
* **HTTP smoke tests** (`HealthResourceHttpSmokeTest`) using
  `jersey-test-framework-provider-jetty` to spin up a real Jetty
  container and hit each major endpoint via a `WebTarget`. These
  catch JAX-RS / Jackson / HK2 regressions that direct-call tests
  miss.

The fixture-based path used by `IndexingServicesResourceTest` is
worth noting: it extends `QueryService` so it can return
hand-crafted `sysindexstatus` rows, which lets us exercise the
JSON-property parser without requiring a real indexing-service tier
in the CI environment.

---

## 5. REST API reference

> Base path: `/api/v2/...`
> All endpoints produce `application/json` and are GET-only — there
> are no write endpoints in v2.

### 5.1 Conventions

* **Identifiers** in path segments must match `[A-Za-z0-9_]+`.
  Otherwise the response is `400 Bad Request` with a plain-text
  message.
* **Unknown identifiers** return `404 Not Found` with a plain-text
  message naming the missing object.
* **Internal failures** (e.g. `DataScannerException` while reading a
  system table) return `500 Internal Server Error` with the
  exception's `getMessage()` in the body.
* **Authentication** is not enforced in v2 (per issue #301); deploy
  behind a firewall, a reverse-proxy auth layer, or whatever is
  appropriate for your environment.

### 5.2 `GET /api/v2/health`

Liveness probe. Useful for container readiness/liveness checks and
quick "is the UI alive?" troubleshooting.

```json
{
  "status": "ok",
  "nodeId": "pecora",
  "timestamp": 1777229702101
}
```

### 5.3 `GET /api/v2/server-info`

Server-level metadata used by the SPA header.

```json
{
  "nodeId": "pecora",
  "mode": "standalone",
  "jdbcUrl": "jdbc:herddb:server:host:7000",
  "defaultTablespace": "herd"
}
```

* `mode` is one of `local`, `standalone`, `cluster`, `shared-storage`
  — read directly from `DBManager.getMode()`.
* `defaultTablespace` is the constant `herddb.model.TableSpace.DEFAULT`
  (value `"herd"`).

### 5.4 `GET /api/v2/tablespaces`

Lists every tablespace visible from this server, derived from
`SELECT * FROM systablespaces`.

```json
[
  {
    "name": "herd",
    "uuid": "c04a9aa350484bd8bdd1afb6e283c464",
    "leader": "pecora",
    "replicas": ["pecora"],
    "expectedReplicaCount": 1,
    "maxLeaderInactivityTime": 0
  }
]
```

`replicas` is the comma-separated `replica` column split into a list;
empty strings produce an empty array.

### 5.5 `GET /api/v2/tablespaces/{name}`

Returns one `TablespaceDTO`, or `404` if no tablespace with that name
is visible. `400` if `{name}` is empty.

### 5.6 `GET /api/v2/tablespaces/{ts}/tables`

Lists the tables of `{ts}`, joining `systables` with `systablestats`
in Java (the planner does not always optimise joins over virtual
tables, and the result set is small).

```json
[
  {
    "tablespace": "herd",
    "name": "child_t",
    "uuid": "…",
    "systemTable": false,
    "tableSize": 1,
    "loadedPages": 1,
    "loadedPagesCount": 1,
    "unloadedPagesCount": 0,
    "dirtyPages": 0,
    "dirtyRecords": 0,
    "maxLogicalPageSize": 1048576,
    "keysMemory": 32,
    "buffersMemory": 128,
    "dirtyMemory": 0
  }
]
```

The list is sorted by name (case-insensitive). System tables (the
virtual `sys*` ones) appear in the list and are flagged with
`systemTable: true`.

### 5.7 `GET /api/v2/tablespaces/{ts}/tables/{name}`

Full per-table detail.

```json
{
  "summary": { /* TableSummaryDTO, see above */ },
  "columns": [
    {
      "name": "id",
      "ordinalPosition": 0,
      "nullable": false,
      "dataType": "INTEGER",
      "typeName": "INTEGER",
      "autoIncrement": false,
      "defaultValue": null
    }
  ],
  "indexes": [
    {
      "name": "child_t_amount_idx",
      "uuid": "…",
      "type": "BRIN",
      "unique": false,
      "columns": ["amount"]
    }
  ],
  "foreignKeys": [
    {
      "name": "fk_child_parent",
      "parentTable": "parent_t",
      "childColumns": ["parent_id"],
      "parentColumns": ["id"],
      "onDeleteAction": "NO ACTION",
      "onUpdateAction": "NO ACTION"
    }
  ]
}
```

* `columns` is sorted by `ordinalPosition`.
* `indexes` joins `sysindexes` with `sysindexcolumns` by
  `index_uuid`; the column list inside each entry is sorted by
  `ordinal_position`.
* `foreignKeys` groups `sysforeignkeys` rows by
  `child_table_cons_name` and inlines per-column links sorted by
  `ordinal_position`.

### 5.8 `GET /api/v2/tablespaces/{ts}/tables/{name}/data-pages`

Active-page snapshot for a regular user table — the data behind the
heatmap visualisation.

```json
{
  "totalPages": 4,
  "loadedPages": 2,
  "totalSizeBytes": 12345,
  "totalDirtBytes": 5,
  "pages": [
    {
      "pageId": 1,
      "sizeBytes": 4096,
      "averageRecordSize": 64,
      "dirtBytes": 0,
      "loaded": true
    }
  ]
}
```

Pages are ordered by ascending `pageId`. The endpoint returns `400` if
the named table is not a regular user table (e.g. a virtual system
table) and `404` if no such table exists.

### 5.9 `GET /api/v2/tablespaces/{ts}/tables/{name}/primary-index`

Primary-key index summary.

```json
{
  "type": "blink",
  "entries": 50,
  "loadedNodes": 3,
  "usedMemoryBytes": 4321
}
```

* For `BLinkKeyToPageIndex` and `IncrementalBLinkKeyToPageIndex` the
  `type` is `"blink"`.
* For other implementations (notably the in-memory
  `ConcurrentMapKeyToPageIndex` used in `local` mode) the `type` is
  the class's simple name; `entries` is still populated from
  `KeyToPageIndex.size()`, while `loadedNodes` and `usedMemoryBytes`
  are reported as `0` because those concepts do not apply to a flat
  hash map.

### 5.10 `GET /api/v2/tablespaces/{ts}/tables/{name}/indexes/{index}`

Per-index detail. For BRIN indexes the `blocks` array is populated;
for other index types (e.g. vector indexes) it is empty and the UI
falls back to the summary table only.

```json
{
  "summary": {
    "name": "child_t_amount_idx",
    "uuid": "…",
    "type": "BRIN",
    "unique": false,
    "columns": ["amount"]
  },
  "blocks": [
    {
      "blockId": 1,
      "pageId": 100,
      "entries": 25,
      "loaded": true,
      "dirty": false
    }
  ]
}
```

### 5.11 `GET /api/v2/indexing-services`

Vector-index overview. Reads `sysindexstatus`, filters to rows whose
`index_type` contains the substring `vector`, and parses the JSON
`properties` column with Jackson into structured fields.

```json
{
  "totalIndexes": 2,
  "totalVectorCount": 12345,
  "indexes": [
    {
      "tablespace": "herd",
      "tableName": "embeddings_t",
      "indexName": "embeddings_idx",
      "indexType": "vector",
      "indexUuid": "…",
      "status": "ready",
      "vectorCount": 10000,
      "segmentCount": 4,
      "lastLsnLedger": 7,
      "lastLsnOffset": 113,
      "rawProperties": "{\"vectorCount\":10000,…}",
      "error": null
    }
  ]
}
```

* The list is sorted by tablespace, then table, then index name
  (case-insensitive).
* If the `properties` JSON is malformed for a particular row, that
  row's `error` field carries the parse-failure message and the
  numeric fields default to `0` — the rest of the response is still
  returned. This means a single misbehaving indexing service cannot
  break the dashboard.

### 5.12 `GET /api/v2/indexing-services/{indexName}`

Returns one `IndexingServiceIndexDTO` matching by case-insensitive
`indexName`. `404` if not found, `400` if `{indexName}` is empty.

---

## 6. Operational notes

* **Disabling the UI.** Set `http.enable=false` in the server
  configuration to skip Jetty entirely (this also disables the
  `/metrics` Prometheus endpoint).
* **Changing the bind address.** Set `http.host` and `http.port`. The
  defaults are `network.host` (typically `0.0.0.0`) and `9845`.
* **Reverse proxying.** Mount the SPA at the same path as the API
  (`/ui` covers both). Vite's `base: '/ui/'` ensures every internal
  asset URL is `/ui/`-prefixed, so the SPA also works behind a path
  rewrite as long as the rewrite preserves the `/ui/` prefix.
* **Logs.** All resources delegate to Jersey's default exception
  mappers; uncaught exceptions become 500s with the message in the
  body. Server-side stack traces appear in the regular HerdDB log
  (`server.service.log`).
* **Performance.** Every endpoint is read-only and short-lived. The
  most expensive call is `data-pages` on tables with millions of
  pages, because it copies metadata into a list; in practice that
  cost is dominated by JSON serialisation. There is no caching layer
  — the SPA hits the backend fresh on each navigation.

---

## 7. Extending the UI

To add a new endpoint:

1. Define the wire shape under `org.herddb.ui.dto` (POJO with default
   constructor + getters/setters; Jackson serialises automatically).
2. Add or extend a JAX-RS resource under `org.herddb.ui.api.v2`. Use
   `QueryService` whenever the data is reachable through SQL on a
   `sys*` table; reach into `Server`/`DBManager` only when SQL is not
   enough (the existing helpers in `IndexesResource` are the model).
3. Register the resource class in `ApplicationConfigV2`.
4. Mirror the wire shape in TypeScript inside
   `src/frontend/api/client.ts` and add a method to `HerdDbApi`.
5. Build a page or visualisation component, and add a route in
   `App.tsx` plus a link from the appropriate parent page.
6. Cover the new code with a direct-call backend test (and a smoke
   HTTP test if it touches Jersey/Jackson glue) and a Vitest test for
   the page.

To add a new system-table column or to expose a new internal
read-only helper from `herddb-core`:

1. Keep the helper read-only and non-blocking; never force a
   checkpoint or take a lock that the table-manager uses for DML.
2. Run the hammer suite (`DirectMultipleConcurrentUpdates*`) twice
   after the change — that suite is the primary regression gate for
   anything that touches the page set or the primary-key index.
3. Document the helper in `architecture.md` (this file) so future
   maintainers know which guarantees the UI relies on.
