# EMS API Reference — Page-wise Guide

> **Base URLs** (replace with your deployed API Gateway endpoints):
> - **Query API** → `QUERY_LAMBDA_URL` (read operations)
> - **Ingestion API** → `INGESTION_LAMBDA_URL` (write operations)
> - **PCE Discovery API** → `PCE_LAMBDA_URL` (CSV upload + orphan management)
> - **Alias Manager API** → `ALIAS_LAMBDA_URL` (alias CRUD + cleanser)
> - **Report API** → `REPORT_LAMBDA_URL` (generate / fetch reports)
> - **Bedrock AI API** → `BEDROCK_LAMBDA_URL` (natural language queries)
>
> **All APIs accept JSON** (`Content-Type: application/json`) unless stated otherwise.
> The `operation` field is always sent in the **request body** (or as a query-string `?operation=`).
> Every successful 200 response is wrapped: `{ success: true, operation, data: <payload>, timestamp }`.
> Errors return `{ error: "<message>" }` with an appropriate HTTP status code.

---

## Page 1 — Network / Topology View

The topology canvas needs the full graph (nodes + edges) or a filtered sub-graph.

---

### 1.1 Get full network graph

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{
  "operation": "get-network"
}
```

**Response `data`:**
```json
{
  "networkData": {
    "nodes": [
      {
        "id": "uuid-string",
        "label": "MyEnvironment",
        "type": "Environment",
        "status": "active",
        "owner": "team-sre",
        "description": "Production environment",
        "stats": {
          "availability": "99.5",
          "cpu": "42",
          "memory": "68",
          "latency": "12",
          "currentState": "Healthy",
          "healthScore": "95"
        }
      }
    ],
    "edges": [
      {
        "id": "rel-uuid",
        "source": "node-uuid-A",
        "target": "node-uuid-B",
        "label": "integratesWith",
        "relationshipType": "integratesWith"
      }
    ]
  },
  "nodeCount": 42,
  "edgeCount": 18
}
```

---

### 1.2 Get sub-graph for selected environments

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{
  "operation": "get-network-for-envs",
  "envIds": ["uuid-1", "uuid-2"],
  "maxHops": 3
}
```

> `envIds`: array of environment UUIDs to center the graph on. Pass `[]` for all.
> `maxHops`: 1–20, default 3.

**Response `data`:**
```json
{
  "networkData": { "nodes": [...], "edges": [...] },
  "nodeCount": 8,
  "edgeCount": 5,
  "envIds": ["uuid-1", "uuid-2"]
}
```

---

### 1.3 Get flat environments list (dropdown / search)

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-environments-list" }
```

**Response `data`:**
```json
{
  "environments": [
    { "id": "uuid", "name": "CAS_QA", "type": "Environment", "owner": "sre-team" }
  ],
  "count": 14
}
```

---

## Page 2 — Environments List / Table View

---

### 2.1 Get all environments

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-environments" }
```

**Response `data`:**
```json
{
  "environments": [
    {
      "id": "uuid",
      "name": "CAS_QA",
      "type": "Environment",
      "owner": "sre-team",
      "description": "QA environment for CAS",
      "status": "active",
      "createdAt": "2025-01-10T08:00:00.000Z"
    }
  ],
  "count": 14
}
```

---

### 2.2 Get single entity by ID

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-entity", "id": "uuid-string" }
```
*(or as query string: `?operation=get-entity&id=uuid-string`)*

**Response `data`:**
```json
{
  "entity": {
    "id": "uuid",
    "name": "CAS_QA",
    "type": "Environment",
    "owner": "sre-team",
    "description": "...",
    "status": "active",
    "createdAt": "2025-01-10T08:00:00.000Z",
    "configs": {
      "region": "us-east-1",
      "endpoint": "https://cas-qa.internal",
      "sourceEnvName": "CAS_QA"
    },
    "stats": {
      "availability": "99.5",
      "cpu": "42",
      "currentState": "Healthy"
    }
  }
}
```

---

### 2.3 Get entity change history

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-entity-history", "entityId": "uuid-string" }
```

**Response `data`:**
```json
{
  "entityId": "uuid",
  "history": [
    {
      "id": "hist-uuid",
      "entityId": "uuid",
      "entityName": "CAS_QA",
      "action": "updated",
      "changedBy": "john.doe",
      "timestamp": "2025-04-01T12:00:00.000Z",
      "changes": {
        "owner": { "from": "old-team", "to": "sre-team" }
      }
    }
  ],
  "count": 5
}
```

---

### 2.4 Create environment

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "create",
  "type": "Environment",
  "name": "CAS_QA",
  "owner": "sre-team",
  "description": "QA environment for CAS",
  "region": "us-east-1",
  "endpoint": "https://cas-qa.internal",
  "configurations": {
    "tier": "non-prod"
  },
  "stats": {
    "availability": "99.5",
    "cpu": "40",
    "memory": "60",
    "currentState": "Healthy",
    "healthScore": "95"
  },
  "primaryUsage": "Integration testing",
  "currentUsage": "Sprint 42 regression",
  "collaborators": "alice, bob",
  "currentBuild": "v2.4.1",
  "scheduledUpdates": "Every Friday 22:00 UTC",
  "relationships": [
    { "type": "integratesWith", "target": "TargetEnvName" }
  ]
}
```

> **Required:** `type`, `name`
> **Optional:** everything else

**Response `data`:**
```json
{
  "id": "new-uuid",
  "name": "CAS_QA",
  "type": "Environment",
  "createdAt": "2025-05-12T10:00:00.000Z"
}
```

**Error — duplicate name (409):**
```json
{
  "error": "Environment with this name already exists",
  "name": "CAS_QA",
  "message": "Environment names must be unique."
}
```

---

### 2.5 Update environment (or any entity)

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "update-entity",
  "id": "uuid-string",
  "type": "Environment",
  "name": "CAS_QA_renamed",
  "owner": "new-team",
  "description": "Updated description",
  "status": "active",
  "primaryUsage": "Load testing",
  "currentUsage": "Sprint 43",
  "collaborators": "alice, charlie",
  "currentBuild": "v2.5.0",
  "scheduledUpdates": "Every Saturday"
}
```

> **Required:** `id`, `type`, `name`

**Response `data`:**
```json
{
  "success": true,
  "message": "Entity updated successfully",
  "id": "uuid-string",
  "name": "CAS_QA_renamed",
  "timestamp": "2025-05-12T10:05:00.000Z"
}
```

---

## Page 3 — Applications & Integrations

---

### 3.1 Get all applications

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-applications" }
```

**Response `data`:**
```json
{
  "applications": [
    { "id": "uuid", "name": "PaymentService", "type": "Application", "owner": "payments-team", "status": "active" }
  ],
  "count": 7
}
```

---

### 3.2 Get all integrations

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-integrations" }
```

**Response `data`:**
```json
{
  "integrations": [
    {
      "id": "uuid",
      "name": "CAS→Auth-int",
      "type": "Integration",
      "owner": "sre-team",
      "status": "active"
    }
  ],
  "count": 11
}
```

---

### 3.3 Create application

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "create",
  "type": "Application",
  "name": "PaymentService",
  "owner": "payments-team",
  "description": "Core payment processing service",
  "version": "3.1.0",
  "endpoint": "https://payments.internal/api",
  "configurations": { "language": "Java", "framework": "Spring Boot" }
}
```

**Response `data`:** same shape as environment create.

---

### 3.4 Create integration

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "create",
  "type": "Integration",
  "name": "CAS→Auth",
  "owner": "sre-team",
  "sourceService": "CAS_QA",
  "targetService": "AuthService_QA",
  "protocol": "REST",
  "configurations": { "timeout": "30s" }
}
```

**Response `data`:** same shape as environment create.

---

### 3.5 Get all entities by type (generic)

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request — all types at once:**
```json
{ "operation": "get-entities" }
```

**Request — single type:**
```json
{ "operation": "get-entities", "type": "Application" }
```
*(or query string: `?operation=get-entities&type=Application`)*

**Response `data` (all types):**
```json
{
  "entities": {
    "Environment": [...],
    "Application": [...],
    "Integration": [...]
  },
  "totalCount": 35
}
```

---

## Page 4 — Relationships

---

### 4.1 Get all relationships

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-relationships" }
```

**Response `data`:**
```json
{
  "relationships": [
    {
      "id": "rel-uuid",
      "relationshipType": "integratesWith",
      "sourceEntity": "CAS_QA",
      "targetEntity": "AuthService_QA",
      "sourceEntityId": "uuid-A",
      "targetEntityId": "uuid-B",
      "createdAt": "2025-02-01T09:00:00.000Z"
    }
  ],
  "count": 18
}
```

> **Relationship types:**
> - `hasIntegration` — Environment → Integration entity (pipeline-created)
> - `integratesWith` — Integration → resolved target Environment
> - `stubs` — Integration → unresolved/placeholder target
> - `calls`, `depends_on`, `feeds_into`, `syncs_with`, `authenticates`, `publishes_to`, `reads_from` — manual

---

### 4.2 Create relationship

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "create-relationship",
  "sourceEntityName": "CAS_QA",
  "relationshipType": "integratesWith",
  "targetEntityName": "AuthService_QA",
  "user": "john.doe"
}
```

**Response `data`:**
```json
{
  "message": "Relationship created successfully",
  "sourceEntityName": "CAS_QA",
  "relationshipType": "integratesWith",
  "targetEntityName": "AuthService_QA",
  "timestamp": "2025-05-12T10:00:00.000Z"
}
```

---

### 4.3 Reverse relationship direction

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "reverse-relationship",
  "relationshipId": "rel-uuid"
}
```

**Response `data`:**
```json
{
  "success": true,
  "message": "Relationship reversed successfully",
  "relationshipId": "rel-uuid",
  "timestamp": "2025-05-12T10:01:00.000Z"
}
```

---

### 4.4 Delete relationship

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "delete-relationship",
  "relationshipId": "rel-uuid",
  "user": "john.doe"
}
```

**Response `data`:**
```json
{
  "success": true,
  "message": "Relationship deleted successfully",
  "relationshipId": "rel-uuid"
}
```

---

## Page 5 — Health Dashboard

---

### 5.1 Get health overview (all entities with stats)

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-health-dashboard" }
```

**Response `data`:**
```json
{
  "entities": [
    {
      "id": "uuid",
      "name": "CAS_QA",
      "type": "Environment",
      "stats": {
        "availability": "99.5",
        "cpu": "42",
        "memory": "68",
        "latency": "12",
        "swap": "5",
        "storage": "55",
        "currentState": "Healthy",
        "healthScore": "95"
      }
    }
  ],
  "totalWithStats": 10,
  "totalEntities": 42
}
```

> **`currentState` values:** `Healthy`, `Warning`, `Critical`, `Notice`, `Major`

---

### 5.2 Get deployments for an environment

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-deployments", "environmentId": "uuid-string" }
```
*(or query string: `?operation=get-deployments&environmentId=uuid-string`)*

**Response `data`:**
```json
{
  "deployments": [
    {
      "applicationId": "app-uuid",
      "applicationName": "PaymentService",
      "deployedAt": "2025-04-20T14:00:00.000Z"
    }
  ],
  "environmentId": "uuid-string",
  "count": 3
}
```

---

### 5.3 Create deployment link (application → environment)

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "create-deployment",
  "applicationId": "app-uuid",
  "environmentId": "env-uuid"
}
```

**Response `data`:**
```json
{
  "message": "Deployment relationship created successfully",
  "applicationId": "app-uuid",
  "environmentId": "env-uuid"
}
```

---

## Page 6 — Configurations

---

### 6.1 Get entity configurations

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-configurations", "entityId": "uuid-string" }
```
*(or query string: `?operation=get-configurations&entityId=uuid-string`)*

**Response `data`:**
```json
{
  "configurations": [
    {
      "id": "config-uuid",
      "type": "EnvironmentConfiguration",
      "configurationMap": {
        "region": "us-east-1",
        "endpoint": "https://cas-qa.internal"
      }
    }
  ],
  "entityId": "uuid-string",
  "count": 1
}
```

---

### 6.2 Get config entries for a configuration

**Endpoint:** `POST QUERY_LAMBDA_URL`

**Request:**
```json
{ "operation": "get-config-entries", "configId": "config-uuid" }
```
*(or query string: `?operation=get-config-entries&configId=config-uuid`)*

**Response `data`:**
```json
{
  "configurationEntries": [
    {
      "id": "entry-uuid",
      "configurationKey": "region",
      "configurationValue": "us-east-1",
      "configurationType": "string"
    }
  ],
  "configurationId": "config-uuid",
  "count": 2
}
```

---

## Page 7 — PCE Discovery / CSV Upload

---

### 7.1 Upload CSV (raw or JSON envelope)

**Endpoint:** `POST PCE_LAMBDA_URL`

**Option A — raw CSV body:**
```
Content-Type: text/plain

envName,propertyKey,propertyValue,sourceFile
serviceA,url,http://serviceA.internal,my-pipeline.csv
serviceB,port,8080,my-pipeline.csv
```

**Option B — JSON envelope:**
```json
{
  "csv": "envName,propertyKey,propertyValue,sourceFile\nserviceA,url,http://serviceA.internal,my-pipeline.csv",
  "filename": "my-pipeline.csv"
}
```

**Response:**
```json
{
  "summary": {
    "environmentsCreated": ["serviceA", "serviceB"],
    "environmentsUpdated": ["existingEnv"],
    "integrationsCreated": 3,
    "recordsProcessed": 10,
    "errors": []
  },
  "runId": "run-uuid"
}
```

---

### 7.2 List orphan nodes

**Endpoint:** `POST PCE_LAMBDA_URL`

**Request:**
```json
{ "operation": "list-orphan-nodes" }
```

**Response:**
```json
{
  "orphans": [
    {
      "id": "node-uuid",
      "name": "serviceA",
      "createdAt": "2025-04-01T00:00:00.000Z",
      "createdByRun": "run-uuid"
    }
  ],
  "count": 3
}
```

---

### 7.3 Connect orphan node to a known environment

**Endpoint:** `POST PCE_LAMBDA_URL`

**Request:**
```json
{
  "operation": "connect-orphan-node",
  "nodeId": "node-uuid",
  "orphanName": "serviceA",
  "sourceEnvName": "CAS_QA",
  "rawHostname": "serviceA.internal",
  "propertyKey": "url",
  "fullUrl": "http://serviceA.internal",
  "protocol": "http",
  "port": "80",
  "path": "/api",
  "endpointHint": "api-gateway",
  "sourceFile": "my-pipeline.csv",
  "createdByRun": "run-uuid",
  "createdAt": "2025-04-01T00:00:00.000Z"
}
```

> **Required:** `nodeId`, `sourceEnvName`
> **Optional:** all others — stored as config properties on the node

**Response:**
```json
{ "success": true, "message": "Connected \"serviceA\" to \"CAS_QA\"" }
```

---

### 7.4 Delete orphan nodes

**Endpoint:** `POST PCE_LAMBDA_URL`

**Request — delete specific IDs:**
```json
{ "operation": "delete-orphan-nodes", "ids": ["node-uuid-1", "node-uuid-2"] }
```

**Request — delete all orphans:**
```json
{ "operation": "delete-orphan-nodes" }
```

**Response:**
```json
{ "deleted": 2, "errors": [] }
```

---

## Page 8 — Alias Manager

All alias manager calls go to `ALIAS_LAMBDA_URL`.

---

### 8.1 List all aliases

**Request:**
```json
{ "operation": "list-aliases" }
```

**Response:**
```json
[
  {
    "hostname": "serviceA.internal",
    "environmentName": "CAS_QA",
    "previousEnvironmentName": "serviceA",
    "updatedAt": "2025-04-10T08:00:00.000Z",
    "updatedBy": "frontend-user"
  }
]
```

---

### 8.2 Create / update alias

**Request:**
```json
{
  "operation": "upsert-alias",
  "hostname": "serviceA.internal",
  "environmentName": "CAS_QA",
  "updatedBy": "john.doe"
}
```

**Response:**
```json
{
  "success": true,
  "hostname": "serviceA.internal",
  "environmentName": "CAS_QA",
  "previousEnvironmentName": "serviceA"
}
```

---

### 8.3 Delete alias

**Request:**
```json
{ "operation": "delete-alias", "hostname": "serviceA.internal" }
```

**Response:**
```json
{ "success": true }
```

---

### 8.4 Revert alias to previous value

**Request:**
```json
{ "operation": "revert-alias", "hostname": "serviceA.internal" }
```

**Response — success:**
```json
{
  "success": true,
  "hostname": "serviceA.internal",
  "revertedTo": "serviceA",
  "previous": "CAS_QA"
}
```

**Response — no previous value:**
```json
{ "success": false, "reason": "no previous value to revert to" }
```

---

### 8.5 Preview cleanser (dry run)

**Request:**
```json
{
  "operation": "preview-cleanser",
  "triggeredBy": "john.doe"
}
```

**Response:**
```json
{ "runId": "run-uuid", "status": "pending", "isPreview": true }
```

> Poll `get-cleanser-status` with the returned `runId`.

---

### 8.6 Trigger cleanser (live run)

**Request:**
```json
{
  "operation": "trigger-cleanser",
  "triggeredBy": "john.doe"
}
```

**Response:**
```json
{ "runId": "run-uuid", "status": "pending", "isPreview": false }
```

---

### 8.7 Get cleanser run status

**Request:**
```json
{ "operation": "get-cleanser-status", "runId": "run-uuid" }
```

**Response:**
```json
{
  "runId": "run-uuid",
  "status": "success",
  "isPreview": false,
  "triggeredBy": "john.doe",
  "triggeredAt": "2025-05-12T10:00:00.000Z",
  "updatedAt": "2025-05-12T10:01:30.000Z",
  "stats": {
    "renamed": 2,
    "merged": 1,
    "errors": 0
  },
  "plan": [
    { "hostname": "serviceA.internal", "action": "rename", "currentName": "serviceA", "targetName": "CAS_QA" }
  ]
}
```

> **`status` values:** `pending`, `running`, `success`, `failed`

---

### 8.8 List recent reconciler runs

**Request:**
```json
{ "operation": "list-reconciler-runs", "limit": 25 }
```

**Response:**
```json
[
  {
    "runId": "run-uuid",
    "status": "success",
    "isPreview": false,
    "triggeredBy": "john.doe",
    "triggeredAt": "2025-05-12T10:00:00.000Z",
    "updatedAt": "2025-05-12T10:01:30.000Z",
    "stats": { "renamed": 2, "merged": 1, "errors": 0 },
    "plan": [...]
  }
]
```

---

## Page 9 — Reports

---

### 9.1 Generate daily report (for an owner)

**Endpoint:** `POST REPORT_LAMBDA_URL`

**Request:**
```json
{ "operation": "generate-daily", "owner": "sre-team" }
```

**Response:**
```json
{
  "success": true,
  "owner": "sre-team",
  "reportType": "daily",
  "reportDate": "2025-05-12T10:00:00.000Z",
  "s3Key": "reports/sre-team/daily/2025-05-12.html",
  "presignedUrl": "https://s3.amazonaws.com/...<signed-url>",
  "environmentCount": 6,
  "alertCount": 2,
  "insightCount": 5,
  "durationMs": 1240
}
```

> `presignedUrl` is valid for **7 days**. Open it in a browser or `<iframe>` to render the HTML report.

---

### 9.2 Generate weekly report (for an owner)

**Request:**
```json
{ "operation": "generate-weekly", "owner": "sre-team" }
```

**Response:** same shape as daily.

---

### 9.3 Generate report (flexible — by owner or env IDs, custom date range)

**Request — by owner:**
```json
{
  "operation": "generate-report",
  "owner": "sre-team",
  "reportType": "weekly",
  "startDate": "2025-05-01",
  "endDate": "2025-05-12"
}
```

**Request — by specific environment IDs:**
```json
{
  "operation": "generate-report",
  "environmentIds": ["uuid-1", "uuid-2"],
  "reportType": "daily"
}
```

**Response:** same shape as 9.1.

**Error — no environments found:**
```json
{ "success": false, "message": "No environments found for owner: sre-team" }
```

---

### 9.4 List reports (for an owner)

**Request:**
```json
{ "operation": "list-reports", "owner": "sre-team" }
```

**Response:**
```json
{
  "reports": [
    {
      "key": "reports/sre-team/weekly/2025-05-12.html",
      "lastModified": "2025-05-12T10:00:00.000Z",
      "size": 48200
    }
  ]
}
```

---

### 9.5 Get report presigned URL by key

**Request:**
```json
{ "operation": "get-report", "reportKey": "reports/sre-team/weekly/2025-05-12.html" }
```

**Response:**
```json
{
  "presignedUrl": "https://s3.amazonaws.com/...<signed-url>",
  "reportKey": "reports/sre-team/weekly/2025-05-12.html"
}
```

---

### 9.6 List owners with reports

**Request:**
```json
{ "operation": "list-owners" }
```

**Response:**
```json
{ "owners": ["sre-team", "platform-team", "payments-team"] }
```

---

## Page 10 — AI / Natural Language Query (Bedrock)

---

### 10.1 Natural language → SPARQL → results

**Endpoint:** `POST BEDROCK_LAMBDA_URL`

**Request:**
```json
{
  "question": "Which environments have CPU usage above 80%?",
  "sessionId": "optional-session-id-for-conversation-context"
}
```

**Response:**
```json
{
  "answer": "There are 3 environments with CPU usage above 80%: CAS_PROD (92%), AuthService_PROD (85%), PaymentService_PROD (81%).",
  "sparqlQuery": "PREFIX env: <http://neptune.aws.com/envmgmt/ontology/> SELECT ?name ?cpu WHERE { ?e env:type \"Environment\" ; env:name ?name ; env:cpu ?cpu . FILTER(xsd:float(?cpu) > 80) } ORDER BY DESC(xsd:float(?cpu)) LIMIT 100",
  "rawResults": {
    "results": {
      "bindings": [
        { "name": { "value": "CAS_PROD" }, "cpu": { "value": "92" } }
      ]
    }
  },
  "sessionId": "optional-session-id"
}
```

---

## Page 11 — Bulk Ingest

---

### 11.1 Bulk ingest entities

**Endpoint:** `POST INGESTION_LAMBDA_URL`

**Request:**
```json
{
  "operation": "bulk-ingest",
  "entities": [
    {
      "type": "Environment",
      "name": "EnvA",
      "owner": "team-a",
      "region": "us-east-1"
    },
    {
      "type": "Application",
      "name": "AppA",
      "owner": "team-a",
      "version": "1.0"
    }
  ]
}
```

**Response `data`:**
```json
{
  "created": ["EnvA", "AppA"],
  "skipped": [],
  "errors": [],
  "count": 2
}
```

---

## Shared Error Shapes

| HTTP Status | When |
|-------------|------|
| `400` | Missing required field / unknown operation |
| `403` | Forbidden operation (e.g. raw SPARQL UPDATE) |
| `404` | Entity / resource not found |
| `409` | Duplicate entity conflict |
| `500` | Internal server error (Neptune down, etc.) |

**All error bodies:**
```json
{ "error": "<human-readable message>" }
```

---

## Health Checks

Both the Query and Ingestion Lambdas expose a health operation:

```json
{ "operation": "health" }
```

```json
{
  "status": "healthy",
  "timestamp": "2025-05-12T10:00:00.000Z",
  "service": "neptune-query"
}
```
