# Plan: React App with Entra ID (PKCE) + Lambda Authorizer

## Phase 1 — Project Scaffolding

1. **Create React app** with Vite + TypeScript
   ```
   my-entra-app/
   ├── frontend/          # React + Vite
   ├── infra/             # CDK stacks
   ├── lambdas/           # API handlers + authorizer
   └── CLAUDE.md
   ```
2. **CDK app** with TypeScript (same pattern as env-management-system)
3. **Lambda functions** folder with shared utilities

---

## Phase 2 — Entra ID App Registration

1. **Register app in Azure Portal** → App Registrations → New
2. **Platform**: Single-page application (SPA)
3. **Redirect URIs**: `http://localhost:5173` (dev), `https://your-domain.com` (prod)
4. **Enable Authorization Code Flow with PKCE** — SPA platform does this automatically (no client secret needed)
5. **Expose an API** → Set Application ID URI (e.g., `api://<client-id>`)
6. **Add scopes**: `api://<client-id>/access_as_user`
7. **Record these values**:
   - Client ID (Application ID)
   - Tenant ID (Directory ID)
   - Application ID URI

---

## Phase 3 — Frontend Authentication (React + MSAL)

**Dependencies**: `@azure/msal-browser`, `@azure/msal-react`

1. **MSAL Configuration** (`authConfig.ts`)
   - `clientId` — from Entra app registration
   - `authority` — `https://login.microsoftonline.com/<tenant-id>`
   - `redirectUri` — `http://localhost:5173`
   - PKCE is automatic with `@azure/msal-browser` (it generates code_verifier + code_challenge internally)

2. **Auth Provider** — wrap `<App>` in `<MsalProvider>`

   ```
   index.tsx
   └── <MsalProvider instance={msalInstance}>
         └── <App />
   ```

3. **Login Flow**
   - Use `useMsal()` hook → `instance.loginRedirect()` or `instance.loginPopup()`
   - Request scopes: `["openid", "profile", "api://<client-id>/access_as_user"]`
   - MSAL handles the full PKCE flow: generates code_verifier, sends code_challenge to `/authorize`, exchanges code at `/token`

4. **Token Acquisition for API Calls**
   - `instance.acquireTokenSilent({ scopes: ["api://<client-id>/access_as_user"] })`
   - Falls back to `acquireTokenRedirect` if silent fails
   - Returns an **access token** (JWT) — this is what you send to the API

5. **Protected Routes**
   - Use `<AuthenticatedTemplate>` / `<UnauthenticatedTemplate>` from `@azure/msal-react`
   - Or create a `<ProtectedRoute>` wrapper component

6. **API Client** (`apiClient.ts`)
   - Axios/fetch interceptor that acquires token silently before every request
   - Attaches `Authorization: Bearer <access_token>` header
   - Base URL points to API Gateway

---

## Phase 4 — Lambda Authorizer (Token Validation)

**This is the core security layer — validates Entra JWTs at the API Gateway level.**

1. **Create `lambdas/src/authorizer.ts`**
   - Receives the `Authorization` header from API Gateway (TOKEN type authorizer)
   - Extracts the Bearer token

2. **JWT Validation Steps** (in order):
   - **Decode header** → get `kid` (key ID) and `alg` (RS256)
   - **Fetch JWKS** from `https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys` (cache this — use a module-scoped variable so it persists across warm Lambda invocations)
   - **Find matching public key** by `kid`
   - **Verify signature** using `jsonwebtoken` or `jose` library with the RSA public key
   - **Validate claims**:
     - `iss` = `https://login.microsoftonline.com/<tenant-id>/v2.0`
     - `aud` = `api://<client-id>` (your Application ID URI)
     - `exp` > current time (not expired)
     - `nbf` < current time (not before)
     - `scp` contains `access_as_user` (scope check)

3. **Return IAM Policy**
   - **Allow**: return `{ principalId, policyDocument: { Statement: [{ Effect: "Allow", Action: "execute-api:Invoke", Resource: "*" }] } }`
   - **Deny**: throw `"Unauthorized"` (API Gateway returns 401)
   - Include `context` with user info (oid, name, email) so downstream Lambdas can access it

4. **Dependencies**: `jose` (lightweight, no native deps — preferred over `jsonwebtoken` for Lambda)

---

## Phase 5 — API Gateway + Lambda Handlers

1. **CDK Stack** (`infra/api-stack.ts`):

   ```
   API Gateway (REST API)
   ├── Authorizer: Lambda TOKEN authorizer (caches for 300s)
   ├── GET  /api/profile     → profile handler
   ├── GET  /api/items       → list items handler
   ├── POST /api/items       → create item handler
   └── CORS: configured for frontend origin
   ```

2. **Lambda Authorizer CDK construct**:
   - `TokenAuthorizer` with `identitySource: 'method.request.header.Authorization'`
   - `resultsCacheTtl: Duration.seconds(300)` — caches auth result per token
   - Attach to every method on the API

3. **Sample API handlers** — simple CRUD (DynamoDB or in-memory)
   - Each handler receives user identity from `event.requestContext.authorizer` (set by the authorizer's context)
   - No need to re-validate the token — the authorizer already did it

4. **CORS configuration**:
   - `allowOrigins`: your frontend URL
   - `allowHeaders`: `['Content-Type', 'Authorization']`
   - `allowMethods`: `['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']`

---

## Phase 6 — CDK Infrastructure

```
my-entra-app/infra/
├── app.ts
├── api-stack.ts          # API GW + Lambda Authorizer + handlers
└── frontend-stack.ts     # (optional) S3 + CloudFront for prod
```

**api-stack.ts resources**:

| Resource                | Purpose                       |
| ----------------------- | ----------------------------- |
| `RestApi`               | API Gateway with CORS         |
| `Function` (authorizer) | Validates Entra JWT           |
| `TokenAuthorizer`       | Attaches authorizer to API GW |
| `Function` (handlers)   | Business logic endpoints      |
| `DynamoDB Table`        | (optional) Sample data store  |

**Environment variables for authorizer Lambda**:

- `TENANT_ID` — Entra tenant
- `CLIENT_ID` — Entra app client ID
- `EXPECTED_AUDIENCE` — `api://<client-id>`

---

## Phase 7 — End-to-End Flow

```
User clicks Login
    │
    ▼
MSAL redirects to Entra /authorize
    │  (with code_challenge, response_type=code)
    ▼
User authenticates → Entra returns auth code to redirect URI
    │
    ▼
MSAL exchanges code for tokens at /token
    │  (sends code_verifier — PKCE proof)
    ▼
MSAL stores access_token + refresh_token in browser
    │
    ▼
React calls API: GET /api/items
    │  Header: Authorization: Bearer <access_token>
    ▼
API Gateway → Lambda Authorizer
    │  Validates JWT signature, issuer, audience, expiry, scope
    │  Returns Allow policy + user context
    ▼
API Gateway → Business Lambda
    │  event.requestContext.authorizer has user info
    ▼
Response → React app
```

---

## Phase 8 — File Checklist

| File                                         | Purpose                                  |
| -------------------------------------------- | ---------------------------------------- |
| `frontend/src/authConfig.ts`                 | MSAL config (clientId, tenantId, scopes) |
| `frontend/src/main.tsx`                      | MsalProvider wrapper                     |
| `frontend/src/components/LoginButton.tsx`    | Login/logout UI                          |
| `frontend/src/components/ProtectedRoute.tsx` | Route guard                              |
| `frontend/src/api/apiClient.ts`              | Axios instance with token interceptor    |
| `frontend/src/pages/Dashboard.tsx`           | Sample protected page                    |
| `lambdas/src/authorizer.ts`                  | JWT validation + IAM policy generation   |
| `lambdas/src/items-handler.ts`               | Sample CRUD handler                      |
| `lambdas/shared/jwks-client.ts`              | JWKS fetching + caching                  |
| `infra/app.ts`                               | CDK app entry                            |
| `infra/api-stack.ts`                         | API GW + authorizer + handlers           |

---

## Key Decisions

- **MSAL v2** (`@azure/msal-browser` v3.x) — handles PKCE automatically, no custom crypto needed
- **`jose` library** for Lambda JWT validation — zero native dependencies, works in Lambda without layers
- **TOKEN authorizer** (not REQUEST) — extracts token from `Authorization` header directly
- **Cache authorizer result** for 5 minutes — reduces cold-start impact, validated tokens aren't re-checked on every call
- **No client secret** — PKCE flow for SPAs never uses a secret (public client)

---

---

# Plan: Migrate Existing Monolith (`environment-management-app.html`) to React

> The existing app is a **~12,600-line single HTML file** with **283 inline functions**, all CSS inline, and 9 navigation tabs. This plan converts it into the React + Vite + TypeScript scaffold from the Entra auth plan above.

---

## Migration Phase 1 — Service Layer Extraction

Extract pure-logic functions into typed service modules (no DOM, no React yet).

### 1.1 API Client (`services/apiClient.ts`)

- `makeApiCall(url, method, body)` — centralized fetch with retry & error handling
- `getApiKeyForUrl(url)` — API key resolution from config
- `retryOperation(fn, retries)` — retry wrapper
- Replace hardcoded API endpoints with env-var-driven config (`services/config.ts`)

### 1.2 SPARQL Service (`services/sparqlService.ts`)

- `executeRawSparqlQuery(query)` — raw SPARQL execution
- `executeStructuredQuery(params)` — structured query builder
- `flattenResultToRows(result)` — result normalization
- `populateBlastRadiusQuery()`, `populateSPOFQuery()`, `populateQuery(template)` — query templates
- `exportQueryResults(results, format)` — CSV/JSON export

### 1.3 Entity Service (`services/entityService.ts`)

- `createEntity(data)` — create via Neptune API
- `loadAvailableEntities()` — fetch all entities
- `deleteEntity(entityId, name, type)` — delete entity
- `editEntity(entityId)` → `getEntity()` + `saveEntityEdit(data)` — edit flow
- `getStats()` — entity statistics
- `getEntityCounts()` — count by type
- `deleteAllEntities()` — admin nuke

### 1.4 Relationship Service (`services/relationshipService.ts`)

- `createStandaloneRelationship(data)`
- `loadExistingRelationships(entityId)`
- `deleteRelationship(subject, predicate, object)`
- `reverseRelationship(subject, predicate, object)`
- `filterRelationships(criteria)` — client-side filtering logic

### 1.5 Import Service (`services/importService.ts`)

- `parseCsvToEntities(csvContent)` — CSV parser
- `parseCSVRows(content)` — row parser
- `handlePceCsvUpload(csv, fileName)` — PCE-specific parser
- `submitBulkImport(entities)` — batch import
- `validateBulkJson()` — JSON schema validation
- `formatBulkJson()` — pretty-print JSON

### 1.6 Report Service (`services/reportService.ts`)

- `generateReport(params)` — report generation
- `downloadReportHTML(report)` — HTML export
- `loadReportHistory()` — fetch history
- `loadReportOwners()` — owner list for filters
- `loadReportEnvironments()` — environment list for filters

### 1.7 AI Query Service (`services/aiQueryService.ts`)

- `executeAiQuery(question)` — simulated AI via pattern matching
- `matchQueryToSparql(input)` — NL-to-SPARQL matching
- `loadAiQueryHistory()` / `saveAiQueryHistory()` — localStorage persistence

### 1.8 Health / Connection Service (`services/healthService.ts`)

- `checkConnections()` — aggregate health check
- `fallbackConnectionCheck()` — fallback strategy
- `testDirectApi(url)` — individual endpoint check
- `healthCheck()` — Neptune health

### 1.9 Utility Functions (`utils/`)

- `utils/format.ts` — `formatDisplayDate()`, `escapeHtml()`
- `utils/export.ts` — `exportGraph()`, `exportGraphAsImage()`
- `utils/csv.ts` — shared CSV parsing helpers
- `utils/toast.ts` — `showToastNotification()`, `showConfirmDialog()` (later replaced by React component)

---

## Migration Phase 2 — React Component Tree

Map the 9 existing tabs to React page components, plus shared UI components.

### 2.1 Layout & Navigation

```
App.tsx
├── AuthProvider (MSAL — from Entra plan above)
├── Layout.tsx
│   ├── Header (logo, connection status dots, health indicator)
│   ├── TabNavigation.tsx (9 tabs)
│   └── <Outlet /> or conditional tab rendering
└── ToastProvider (replace showToastNotification)
```

### 2.2 Page Components (one per tab)

| Existing Tab (`data-tab`) | React Page Component           | Key Child Components                                                                                 |
| ------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `add-entity`              | `pages/AddEntityPage.tsx`      | `EntityForm`, `ConfigFieldList`, `SearchableDropdown`                                                |
| `query-data`              | `pages/QueryPage.tsx`          | `QueryTemplateSelector`, `StructuredQueryBuilder`, `RawSparqlEditor`, `ResultsTable`, `AiQueryPanel` |
| `visualize`               | `pages/VisualizePage.tsx`      | `NetworkGraph` (D3 wrapper), `HealthStatusBar`, `NodeTooltip`, `EdgeLabelToggle`                     |
| `ai-query`                | _(merged into QueryPage)_      | `AiQueryPanel`, `AiHistoryList`                                                                      |
| `relationships`           | `pages/RelationshipsPage.tsx`  | `RelationshipForm`, `RelationshipList`, `RelationshipFilter`                                         |
| `delete-entity`           | `pages/ManageEntitiesPage.tsx` | `EntityList`, `EntitySearch`, `EditEntityModal`                                                      |
| `bulk-import`             | `pages/ImportPage.tsx`         | `FileUploader`, `JsonEditor`, `ImportPreview`, `PceUploader`                                         |
| `reports`                 | `pages/ReportsPage.tsx`        | `ReportConfigForm`, `MultiSelectDropdown`, `ReportViewer`, `ReportHistory`                           |
| `admin`                   | `pages/AdminPage.tsx`          | `ConnectionStatus`, `HealthCheck`, `EntityCounts`, `DangerZone`                                      |

### 2.3 Shared UI Components

| Component                 | Replaces                                                   | Notes                                          |
| ------------------------- | ---------------------------------------------------------- | ---------------------------------------------- |
| `SearchableDropdown.tsx`  | `initSearchableDropdown()` + DOM manipulation              | Controlled component with filter, keyboard nav |
| `MultiSelectDropdown.tsx` | `initMsDropdown()`, `renderMsOptions()`, `renderMsChips()` | Replaces 10+ `ms*` functions                   |
| `ConfirmDialog.tsx`       | `showConfirmDialog()`                                      | React portal modal                             |
| `Toast.tsx`               | `showToastNotification()`                                  | Context-based toast system                     |
| `ConfigFieldEditor.tsx`   | `addConfigField()`, `addEditConfigField()`                 | Dynamic key–value field list                   |
| `ResultsTable.tsx`        | Inline `<table>` rendering in multiple tabs                | Reusable table with sort/export                |

---

## Migration Phase 3 — React Hooks (State Management)

Replace global variables and DOM state with React hooks and context.

### 3.1 Custom Hooks

| Hook                         | Replaces                                                         | State Managed                           |
| ---------------------------- | ---------------------------------------------------------------- | --------------------------------------- |
| `useEntities()`              | `loadAvailableEntities()`, global entity arrays                  | Entity list, loading, error             |
| `useRelationships(entityId)` | `loadExistingRelationships()`, filtering state                   | Relationship list, filters              |
| `useQueryExecution()`        | `executeCustomQuery()`, `executeStructuredQuery()`               | Query text, results, history, export    |
| `useAiQuery()`               | `executeAiQuery()`, `matchQueryToSparql()`, localStorage history | Question, results, history              |
| `useConnectionStatus()`      | `checkConnections()`, `connectionStatus` global                  | Per-endpoint status, polling            |
| `useNetworkGraph()`          | `loadNetworkData()`, `renderGraph()`, SVG state                  | Graph data, filters, selection          |
| `useReports()`               | `generateReport()`, `loadReportHistory()`                        | Config, report data, history            |
| `useBulkImport()`            | `submitBulkImport()`, file parsing chain                         | File content, parsed entities, progress |
| `useEnvironmentFilter()`     | `populateEnvFilter()`, `toggleEnvOption()`                       | Selected envs, filtering                |

### 3.2 Context Providers

| Context             | Purpose                                                        |
| ------------------- | -------------------------------------------------------------- |
| `ConfigContext`     | API endpoints, API keys (replaces inline `CONFIG` block)       |
| `ConnectionContext` | Connection status shared across tabs (header dots + admin tab) |
| `ToastContext`      | Global toast notification queue                                |
| `AuthContext`       | MSAL auth state (from Entra plan)                              |

### 3.3 Global State Replaced

| Current Global                  | React Replacement                                  |
| ------------------------------- | -------------------------------------------------- |
| `connectionStatus` object       | `useConnectionStatus()` hook + `ConnectionContext` |
| `currentGraphData`              | `useNetworkGraph()` hook local state               |
| `aiQueryHistory` (localStorage) | `useAiQuery()` hook with localStorage sync         |
| `rptHistory*` variables         | `useReports()` hook local state                    |
| `msDropdowns` map               | Component-local state in `MultiSelectDropdown`     |
| `currentEnvFilter`              | `useEnvironmentFilter()` hook                      |

---

## Migration Phase 4 — D3 Visualization Integration

The graph visualization (D3 force-directed) requires special handling in React.

### 4.1 `NetworkGraph.tsx` Component

```tsx
// Approach: useRef for SVG container + useEffect for D3 bindingw
const svgRef = useRef<SVGSVGElement>(null);

useEffect(() => {
  if (!svgRef.current || !graphData) return;
  // Clear previous
  d3.select(svgRef.current).selectAll("*").remove();
  // Render — reuse existing renderGraph logic
  renderD3Graph(svgRef.current, graphData, options);
  return () => {
    /* cleanup simulation */
  };
}, [graphData, options]);
```

### 4.2 Functions to port into D3 helper (`utils/d3Graph.ts`)

- `renderGraph(data, container)` — main render
- `getNodeColor(type)` — node coloring
- `getNodeHealthState(node)` — health halo
- `setupTooltipEvents(simulation)` — tooltip binding
- `toggleEdgeLabels()` / `toggleEdgeLabelsFromGraph()`
- `resetZoom()`, `fitGraphToViewport()` — zoom controls
- `filterByHealth(state)`, `resetHealthFilter()` — health filtering
- `updateHealthStatusBar(data)` — health bar computation
- `exportGraphAsImage(svgEl)` — SVG-to-PNG export

### 4.3 D3 Dependency

- Replace CDN `<script>` with `npm install d3 @types/d3`
- Import only needed modules: `d3-selection`, `d3-force`, `d3-zoom`, `d3-drag`

---

## Migration Phase 5 — CSS Extraction

### 5.1 Strategy

- Extract all `<style>` blocks from HTML into CSS modules or a global stylesheet
- Use **CSS Modules** for component-scoped styles (e.g., `NetworkGraph.module.css`)
- Move shared styles (variables, reset, tab layout) into `styles/global.css`

### 5.2 CSS Variable Map

The existing app uses CSS custom properties (`--bg-primary`, `--text-primary`, etc.) — keep these as-is in `:root` and they become the design tokens for the React app.

### 5.3 Key Style Groups to Extract

- Tab / navigation styles → `styles/navigation.css`
- Form styles (inputs, buttons, fieldsets) → `styles/forms.css`
- Graph / visualization styles → `components/NetworkGraph.module.css`
- Modal / dialog styles → `components/Modal.module.css`
- Toast / notification styles → `components/Toast.module.css`
- Table / results styles → `components/ResultsTable.module.css`

---

## Migration Phase 6 — Incremental Migration Strategy

### Recommended Approach: **Tab-by-Tab Migration**

Migrate one tab at a time so the app remains functional throughout.

### 6.1 Migration Order (risk-sorted, simplest first)

| Order | Tab             | Complexity | Why This Order                                         |
| ----- | --------------- | ---------- | ------------------------------------------------------ |
| 1     | Admin           | Low        | Fewest dependencies, mostly read-only health checks    |
| 2     | Add Entity      | Low–Med    | Self-contained form, no cross-tab state                |
| 3     | Manage Entities | Medium     | Entity CRUD + edit modal, depends on entity service    |
| 4     | Relationships   | Medium     | Form + list + filter, depends on entity dropdowns      |
| 5     | Import          | Medium     | File handling + parsers, mostly self-contained         |
| 6     | Query Data      | Med–High   | Multiple query modes, results display, AI query panel  |
| 7     | Reports         | Med–High   | Multi-select dropdowns, history, HTML report rendering |
| 8     | Visualize       | High       | D3 integration, health overlays, zoom, export          |

### 6.2 Coexistence During Migration

- **Option A (recommended)**: Build React app fresh, migrate tab logic one at a time, keep old HTML as reference
- **Option B**: Embed React components inside existing HTML using `createRoot()` on individual containers — more brittle, not recommended

### 6.3 Testing Strategy During Migration

- Port corresponding Playwright E2E tests alongside each tab migration
- Existing 88 tests serve as regression baseline — same assertions, new selectors if needed
- Add React Testing Library unit tests for hooks and services
- Keep E2E tests running against the React app on `localhost:5173` (Vite dev server)

---

## Migration Phase 7 — File Structure (Final)

```
frontend/
├── public/
├── src/
│   ├── main.tsx                          # MsalProvider + App mount
│   ├── App.tsx                           # Router + Layout
│   ├── auth/
│   │   ├── authConfig.ts                 # MSAL config (from Entra plan)
│   │   └── AuthContext.tsx
│   ├── services/
│   │   ├── apiClient.ts                  # makeApiCall, retry, config
│   │   ├── config.ts                     # API endpoints, keys
│   │   ├── sparqlService.ts              # SPARQL queries
│   │   ├── entityService.ts              # Entity CRUD
│   │   ├── relationshipService.ts        # Relationship CRUD
│   │   ├── importService.ts              # CSV/JSON parsing, bulk import
│   │   ├── reportService.ts              # Report gen, history
│   │   ├── aiQueryService.ts             # Simulated AI query
│   │   └── healthService.ts              # Connection checks
│   ├── hooks/
│   │   ├── useEntities.ts
│   │   ├── useRelationships.ts
│   │   ├── useQueryExecution.ts
│   │   ├── useAiQuery.ts
│   │   ├── useConnectionStatus.ts
│   │   ├── useNetworkGraph.ts
│   │   ├── useReports.ts
│   │   ├── useBulkImport.ts
│   │   └── useEnvironmentFilter.ts
│   ├── components/
│   │   ├── Layout.tsx
│   │   ├── TabNavigation.tsx
│   │   ├── SearchableDropdown.tsx
│   │   ├── MultiSelectDropdown.tsx
│   │   ├── ConfigFieldEditor.tsx
│   │   ├── ConfirmDialog.tsx
│   │   ├── Toast.tsx
│   │   ├── ResultsTable.tsx
│   │   └── NetworkGraph.tsx              # D3 wrapper
│   ├── pages/
│   │   ├── AddEntityPage.tsx
│   │   ├── QueryPage.tsx                 # Includes AI query panel
│   │   ├── VisualizePage.tsx
│   │   ├── RelationshipsPage.tsx
│   │   ├── ManageEntitiesPage.tsx
│   │   ├── ImportPage.tsx
│   │   ├── ReportsPage.tsx
│   │   └── AdminPage.tsx
│   ├── utils/
│   │   ├── format.ts
│   │   ├── export.ts
│   │   ├── csv.ts
│   │   ├── d3Graph.ts                    # D3 render + helpers
│   │   └── toast.ts
│   └── styles/
│       ├── global.css                    # CSS variables, reset
│       ├── navigation.css
│       └── forms.css
├── tests/
│   ├── e2e/                              # Migrated Playwright tests
│   └── unit/                             # React Testing Library
├── index.html
├── vite.config.ts
├── tsconfig.json
└── package.json
```

---

## Migration Phase 8 — Function-to-Module Mapping (Reference)

Complete mapping of all **283 functions** from `environment-management-app.html`:

| Function Group                                  | Count (approx) | Target Module                                                     |
| ----------------------------------------------- | -------------- | ----------------------------------------------------------------- |
| API / fetch / retry                             | ~5             | `services/apiClient.ts`                                           |
| Entity CRUD (create, edit, delete, load, stats) | ~20            | `services/entityService.ts` + `hooks/useEntities.ts`              |
| Relationship CRUD + filtering                   | ~12            | `services/relationshipService.ts` + `hooks/useRelationships.ts`   |
| SPARQL query building + execution + export      | ~15            | `services/sparqlService.ts` + `hooks/useQueryExecution.ts`        |
| AI query (simulate, match, history)             | ~10            | `services/aiQueryService.ts` + `hooks/useAiQuery.ts`              |
| Graph visualization (D3, zoom, tooltip, export) | ~20            | `utils/d3Graph.ts` + `NetworkGraph.tsx`                           |
| Health / connection checks                      | ~8             | `services/healthService.ts` + `hooks/useConnectionStatus.ts`      |
| Import (CSV, JSON, Excel, PCE, bulk)            | ~15            | `services/importService.ts` + `hooks/useBulkImport.ts`            |
| Reports (generate, history, multi-select)       | ~20            | `services/reportService.ts` + `hooks/useReports.ts`               |
| Multi-select dropdown (ms\*)                    | ~12            | `components/MultiSelectDropdown.tsx`                              |
| Searchable dropdown                             | ~5             | `components/SearchableDropdown.tsx`                               |
| Environment filter                              | ~6             | `hooks/useEnvironmentFilter.ts`                                   |
| Tab / navigation                                | ~5             | `components/TabNavigation.tsx` (React Router)                     |
| Form helpers (config fields, etc.)              | ~8             | `components/ConfigFieldEditor.tsx`                                |
| Toast / confirm dialogs                         | ~3             | `components/Toast.tsx` + `components/ConfirmDialog.tsx`           |
| Utility (format, escape, export)                | ~5             | `utils/format.ts`, `utils/export.ts`                              |
| Admin (delete all, health, counts)              | ~5             | `pages/AdminPage.tsx`                                             |
| DOM initialization / event listeners            | ~30+           | **Removed** — replaced by React component lifecycle               |
| CSS class toggles / DOM manipulation            | ~80+           | **Removed** — replaced by React state + JSX conditional rendering |

---

## Key Migration Decisions

- **TypeScript throughout** — all services, hooks, and components are `.ts`/`.tsx`
- **No Redux** — React Context + custom hooks is sufficient for this app's complexity
- **React Router v6** — tab navigation becomes URL routes (`/add`, `/query`, `/visualize`, etc.)
- **Vite** — fast HMR, native TypeScript, simple config
- **D3 as a utility, not a framework** — React owns the DOM, D3 only computes layout and bindsinside `useEffect`
- **Existing Playwright tests** are the migration acceptance criteria — each tab is "done" when its E2E tests pass against the React version
- **CSS Modules** for component styles, global CSS for shared variables and reset
- **No server.js needed** — Vite dev server replaces Express for local development; production is static hosting (S3 + CloudFront)
- **Environment variables** via Vite's `import.meta.env` — replace hardcoded CONFIG block
