# AOS-Farm API Integration Guide for DCL

**Last Updated:** November 24, 2025  
**Base URL:** `https://autonomos.farm` (production) or `http://localhost:5000` (development)

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [API Endpoints](#api-endpoints)
3. [Authentication & Tenant Isolation](#authentication--tenant-isolation)
4. [Request/Response Contracts](#requestresponse-contracts)
5. [Data Domains](#data-domains)
6. [Integration Pattern](#integration-pattern)
7. [Error Handling](#error-handling)

---

## Quick Start

### Health Check
```bash
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-24T06:23:36.170Z",
  "service": "aos-farm"
}
```

### Basic Flow
1. **Generate synthetic data** (POST to generation endpoint)
2. **Retrieve data** (GET from data endpoint)
3. **Transform to DCL schemas** (client-side mapping)

---

## API Endpoints

### 1. Schema Discovery & Data Catalog

#### 1.1 Enterprise Assets

**Important:** Assets are generated automatically during test runs. Use the GET endpoint to retrieve them.

**Retrieve Assets:**
```http
GET /api/synthetic
```

**Response:**
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "labTenantId": "tenant-uuid",
      "assetId": "APP-000001",
      "assetType": "application",
      "name": "Enterprise CRM",
      "environment": "production",
      "status": "running",
      "criticality": "high",
      "owner": "platform-team",
      "tags": ["crm", "customer-facing", "critical"],
      "metadata": {
        "version": "2.1.4",
        "framework": "Spring Boot",
        "language": "Java",
        "deployedAt": "2024-08-15T10:30:00Z"
      },
      "dependencies": ["SRV-000023", "SRV-000045"],
      "createdAt": "2025-11-24T06:00:00Z"
    }
  ],
  "total": 50
}
```

**Schema:**
- `assetId` (string) - Unique identifier (APP-XXXXXX, SRV-XXXXXX, HOST-XXXXXX)
- `assetType` (enum) - `application` | `service` | `host`
- `name` (string) - Human-readable name
- `environment` (enum) - `production` | `staging` | `development`
- `status` (enum) - `running` | `stopped` | `degraded`
- `criticality` (enum) - `low` | `medium` | `high` | `critical`
- `owner` (string) - Team/owner identifier
- `tags` (string[]) - Free-form tags
- `metadata` (object) - Type-specific metadata
- `dependencies` (string[]) - Array of related assetIds (FK relationships)

---

#### 1.2 Business Data (CRM/ERP)

**Auto-Generate & Retrieve Customers:**
```http
GET /api/synthetic/customers?generate=true&scale=small&limit=50&offset=0
```

**Parameters:**
- `generate=true` - Auto-generate customers if none exist
- `scale` - Generation scale: `small` (50), `medium` (200), `large` (1000)
- `limit` - Results per page (default: 50)
- `offset` - Pagination offset (default: 0)

**Retrieve Existing Customers:**
```http
GET /api/synthetic/customers?limit=50&offset=0
```

**Response:**
```json
{
  "data": [
    {
      "id": "customer-uuid",
      "labTenantId": "tenant-uuid",
      "customerId": "CUST-000001",
      "name": "John Smith",
      "email": "john.smith@example.com",
      "company": "Acme Corp",
      "industry": "technology",
      "tier": "premium",
      "status": "active",
      "metadata": {
        "signupDate": "2024-06-15T08:00:00Z",
        "ltv": 25000
      },
      "createdAt": "2025-11-24T06:00:00Z"
    }
  ],
  "total": 100
}
```

**Auto-Generate & Retrieve Invoices (with FK to Customers):**
```http
GET /api/synthetic/invoices?generate=true&limit=50&offset=0
```

**Note:** 
- Requires existing customers first
- Invoices automatically reference customers via `customerId` foreign key
- `generate=true` creates invoices for up to 20 existing customers

**Retrieve Existing Invoices:**
```http
GET /api/synthetic/invoices?limit=50&offset=0&customerId=CUST-000001
```

**Response:**
```json
{
  "data": [
    {
      "id": "invoice-uuid",
      "labTenantId": "tenant-uuid",
      "invoiceId": "INV-000001",
      "customerId": "CUST-000042",  // FK reference
      "amount": 1250.00,
      "status": "paid",
      "dueDate": "2025-01-15",
      "paidDate": "2025-01-10",
      "items": [
        { "description": "Premium Plan", "quantity": 1, "price": 999.00 },
        { "description": "Support Package", "quantity": 1, "price": 251.00 }
      ],
      "createdAt": "2025-11-24T06:00:00Z"
    }
  ],
  "total": 200
}
```

**Business Data Schema Relationships:**
```
customers (1) ──→ (N) invoices
customers (1) ──→ (N) subscriptions  
customers (1) ──→ (N) transactions
```

**Available Business Endpoints:**
- `GET /api/synthetic/customers?generate=true&scale={small|medium|large}`
- `GET /api/synthetic/invoices?generate=true&customerId={id}`
- `GET /api/synthetic/subscriptions?generate=true&customerId={id}`
- `GET /api/synthetic/transactions?generate=true&customerId={id}`

---

#### 1.3 Time-Series Events

**Auto-Generate & Retrieve Events:**
```http
GET /api/synthetic/events?generate=true&eventType=log&pattern=hourly&counts[logs]=1000&limit=100&offset=0
```

**Parameters:**
- `generate=true` - Auto-generate events if needed
- `eventType` - Filter: `log`, `network_flow`, `auth` (optional)
- `pattern` - Generation pattern: `hourly`, `daily`, `weekly` (optional)
- `counts[logs]`, `counts[flows]`, `counts[auth]` - Counts per type (optional)
- `startTime`, `endTime` - ISO 8601 time range filter (optional)

**Retrieve Existing Events:**
```http
GET /api/synthetic/events?eventType=log&limit=100&offset=0&startTime=2025-11-24T00:00:00Z&endTime=2025-11-24T23:59:59Z
```

**Response:**
```json
{
  "data": [
    {
      "id": "event-uuid",
      "labTenantId": "tenant-uuid",
      "eventId": "EVT-000001",
      "eventType": "log",
      "timestamp": "2025-11-24T06:15:32.123Z",
      "source": "APP-000015",
      "severity": "error",
      "message": "Database connection timeout after 30s",
      "metadata": {
        "correlationId": "req-abc-123",
        "userId": "user-456",
        "endpoint": "/api/orders"
      },
      "createdAt": "2025-11-24T06:00:00Z"
    }
  ],
  "total": 1000
}
```

**Event Types:**
- `log` - Application logs (info/warn/error/debug)
- `network_flow` - Network traffic (src/dst IP, ports, protocol, bytes)
- `auth` - Authentication events (login/logout/failed attempts)

---

### 2. Mock CRM/ERP API Endpoints

**Mock CRM Accounts Endpoint:**
```http
GET /api/synthetic/crm/accounts?scenarioId={id}
```

**Response:**
```json
{
  "data": [
    {
      "id": "acc-1",
      "name": "Enterprise CRM",
      "industry": "Technology",
      "revenue": 542891
    }
  ],
  "total": 50
}
```

**Mock ERP Invoices Endpoint:**
```http
GET /api/synthetic/erp/invoices
```

**Response:**
```json
{
  "data": [
    {
      "id": "inv-1",
      "amount": 5432,
      "status": "paid",
      "date": "2025-11-15T10:30:00Z",
      "tenantId": "tenant-uuid"
    }
  ],
  "total": 20
}
```

**Purpose:** These endpoints simulate external CRM/ERP systems for testing AAM connector data ingestion. The CRM endpoint derives accounts from synthetic assets. Both support chaos injection when `scenarioId` is provided.

---

## Authentication & Tenant Isolation

### Current Implementation (MVP)

**Authentication Method:** Session-based (IP-derived)

**How It Works:**
1. Farm derives tenant ID from your session (IP-based in MVP)
2. All data is automatically scoped to your tenant
3. No manual tenant ID passing required

**Headers:** None required for MVP

**Example:**
```bash
# All requests from your IP automatically get the same tenant
curl http://localhost:5000/api/synthetic/assets
# Returns only assets for your tenant

# Data generated in one session is isolated from other sessions
```

### Production Recommendations

For production DCL integration, consider:

**Option 1: API Key in Header**
```http
X-API-Key: dcl-farm-integration-key-abc123
```

**Option 2: Tenant ID in Header**
```http
X-Tenant-Id: dcl-production-tenant
```

**Current State:** No auth setup required for DCL testing. Farm auto-assigns tenant per session.

---

## Request/Response Contracts

### Common Patterns

**Pagination:**
- Default limit: 100
- Max limit: 1000
- Parameters: `?limit=100&offset=0`

**Filtering:**
- Type filters: `?assetType=application`
- Status filters: `?status=running`
- Time ranges: `?startTime=ISO8601&endTime=ISO8601`

**Response Structure:**
```json
{
  "data": [...],      // Array of records
  "total": 250        // Total count (for pagination)
}
```

**Error Response:**
```json
{
  "error": "Invalid asset type. Must be one of: application, service, host"
}
```

**HTTP Status Codes:**
- `200` - Success
- `400` - Bad request (validation error)
- `500` - Server error

---

## Data Domains

### Available Domains

| Domain | Tables | GET Endpoint (with auto-generation) |
|--------|--------|-------------------------------------|
| **Enterprise Assets** | assets | `/api/synthetic` |
| **Business Data** | customers, invoices, subscriptions, transactions | `/api/synthetic/{type}?generate=true` |
| **Time-Series Events** | events | `/api/synthetic/events?generate=true` |
| **Mock External APIs** | CRM accounts, ERP invoices | `/api/synthetic/crm/accounts`, `/api/synthetic/erp/invoices` |

### Field Metadata

**For Schema Discovery:**

All endpoints return data with:
- **Field names:** JSON keys
- **Field types:** Inferred from JSON types (string, number, boolean, object, array)
- **Nullable:** Check for `null` values in sample data
- **Foreign keys:** Look for fields ending in `Id` (e.g., `customerId`, `assetId`)
- **Semantic hints:**
  - `*Id` fields → IDs/foreign keys
  - `status`, `tier`, `criticality` → Enums
  - `metadata` → Flexible JSON object
  - `tags`, `dependencies` → Arrays

**Example Schema Inference:**
```json
{
  "customerId": "CUST-000001",  // Type: string, Semantic: ID
  "email": "john@example.com",  // Type: string, Semantic: email
  "tier": "premium",            // Type: enum (free|basic|premium|enterprise)
  "metadata": {...},            // Type: object, Semantic: flexible JSON
  "status": "active"            // Type: enum (active|inactive|churned)
}
```

---

## Integration Pattern

### Recommended Flow for DCL

```python
# 1. Generate synthetic data (auto-generation via query params)
def setup_farm_data():
    # Auto-generate customers (if needed)
    requests.get(f"{FARM_URL}/api/synthetic/customers", params={
        "generate": "true",
        "scale": "medium"  # Generates 200 customers
    })
    
    # Auto-generate invoices for existing customers
    requests.get(f"{FARM_URL}/api/synthetic/invoices", params={
        "generate": "true"
    })

# 2. Discover available sources
def discover_sources():
    sources = [
        {"id": "farm-assets", "name": "Enterprise Assets", "endpoint": "/api/synthetic"},
        {"id": "farm-customers", "name": "CRM Customers", "endpoint": "/api/synthetic/customers"},
        {"id": "farm-invoices", "name": "ERP Invoices", "endpoint": "/api/synthetic/invoices"},
        {"id": "farm-events", "name": "Time-Series Events", "endpoint": "/api/synthetic/events"},
        {"id": "farm-crm", "name": "Mock CRM", "endpoint": "/api/synthetic/crm/accounts"},
        {"id": "farm-erp", "name": "Mock ERP", "endpoint": "/api/synthetic/erp/invoices"}
    ]
    return sources

# 3. Fetch schema + sample data
def get_source_schema(endpoint):
    # Fetch sample records
    response = requests.get(f"{FARM_URL}{endpoint}?limit=5")
    sample_data = response.json()["data"]
    
    # Infer schema from sample data
    if sample_data:
        schema = infer_schema_from_sample(sample_data[0])
        return schema
    return None

# 4. Transform to DCL models
def transform_to_dcl(farm_data, source_id):
    source_system = SourceSystem(
        id=source_id,
        name=farm_data["name"],
        type="synthetic",
        connection_info={"base_url": FARM_URL}
    )
    
    tables = []
    for record in farm_data["sample"]:
        table = TableSchema(
            source_id=source_id,
            table_name=infer_table_name(record),
            fields=convert_fields_to_dcl(record)
        )
        tables.append(table)
    
    return source_system, tables
```

### Caching Strategy

**Recommendation:** Cache schemas for the duration of a test run

**Rationale:**
- Schemas are stable within a lab session
- Generated data doesn't change structure
- Fresh generation creates new data, not new schemas

**Change Notification:** None currently. Poll for new data as needed.

---

## Error Handling

### Common Errors

**Validation Error (400):**
```json
{
  "error": "Invalid asset type. Must be one of: application, service, host"
}
```

**Not Found (404):**
```json
{
  "error": "No assets found for the specified filters"
}
```

**Server Error (500):**
```json
{
  "error": "Database connection failed"
}
```

### Chaos Engineering

**Important:** Farm implements chaos injection at the response level:
- Random latency (50-500ms)
- Random errors (5-10% failure rate)
- Random timeouts

**Schema Discovery Stability:**
- Schema endpoints are NOT affected by chaos (stable)
- Data retrieval endpoints MAY experience chaos
- Health check (`/health`) is always stable

**Recommendation:** Implement retry logic with exponential backoff for data retrieval endpoints.

---

## Scale Expectations

### Typical Volumes

| Domain | Sources | Tables | Records |
|--------|---------|--------|---------|
| Enterprise Assets | 1 | 1 (assets) | 100-1,000 |
| Business Data | 4 | 4 (customers, invoices, subscriptions, transactions) | 100-10,000 each |
| Time-Series Events | 1 | 1 (events) | 1,000-100,000 |

### Rate Limits

**Current:** None (MVP)

**Future Production:**
- 100 requests/minute per tenant
- 1,000 requests/hour per tenant
- Concurrent connections: 10

---

## Example: Complete DCL Integration

```python
import requests
from typing import List, Dict

FARM_URL = "https://autonomos.farm"

class FarmIntegration:
    def __init__(self, base_url: str = FARM_URL):
        self.base_url = base_url
        
    def health_check(self) -> bool:
        """Verify Farm is available"""
        try:
            response = requests.get(f"{self.base_url}/health")
            return response.json()["status"] == "healthy"
        except:
            return False
    
    def generate_test_data(self):
        """One-time setup: generate synthetic data"""
        # Business data (auto-generates if needed)
        requests.get(f"{self.base_url}/api/synthetic/customers", params={
            "generate": "true",
            "scale": "medium"
        })
        requests.get(f"{self.base_url}/api/synthetic/invoices", params={
            "generate": "true"
        })
        requests.get(f"{self.base_url}/api/synthetic/events", params={
            "generate": "true",
            "pattern": "hourly"
        })
    
    def fetch_assets(self) -> List[Dict]:
        """Fetch enterprise assets"""
        response = requests.get(f"{self.base_url}/api/synthetic")
        return response.json() if isinstance(response.json(), list) else []
    
    def fetch_customers(self, limit: int = 100) -> List[Dict]:
        """Fetch CRM customers"""
        response = requests.get(f"{self.base_url}/api/synthetic/customers", params={"limit": limit})
        return response.json()["data"]
    
    def fetch_events(self, event_type: str = None, limit: int = 100) -> List[Dict]:
        """Fetch time-series events"""
        params = {"limit": limit}
        if event_type:
            params["eventType"] = event_type
        response = requests.get(f"{self.base_url}/api/synthetic/events", params=params)
        return response.json()["data"]

# Usage
farm = FarmIntegration()

# Check health
if farm.health_check():
    print("✓ Farm is healthy")
    
    # Generate test data
    farm.generate_test_data()
    
    # Fetch and transform
    assets = farm.fetch_assets(limit=50)
    customers = farm.fetch_customers(limit=50)
    events = farm.fetch_events(event_type="log", limit=100)
    
    # Transform to DCL schemas
    # ... your DCL transformation logic here ...
```

---

## Quick Reference

### All Available Endpoints

```
Health Check:
GET /health

Enterprise Assets:
GET /api/synthetic

Business Data (auto-generation via query params):
GET /api/synthetic/customers?generate=true&scale={small|medium|large}&limit={n}&offset={n}
GET /api/synthetic/invoices?generate=true&customerId={id}&limit={n}&offset={n}
GET /api/synthetic/subscriptions?generate=true&customerId={id}&limit={n}&offset={n}
GET /api/synthetic/transactions?generate=true&customerId={id}&limit={n}&offset={n}

Time-Series Events:
GET /api/synthetic/events?generate=true&eventType={type}&pattern={pattern}&startTime={iso8601}&endTime={iso8601}&limit={n}&offset={n}

Mock External APIs:
GET /api/synthetic/crm/accounts?scenarioId={id}
GET /api/synthetic/erp/invoices
```

---

## Next Steps for DCL Team

1. **Test connectivity:** `curl https://autonomos.farm/health`
2. **Generate test data:** POST to generation endpoints
3. **Fetch sample schemas:** GET with `?limit=5` to inspect structure
4. **Implement transformation:** Map Farm JSON to DCL `SourceSystem/TableSchema/FieldSchema`
5. **Deploy integration:** Replace hardcoded `load_farm_schemas()` with HTTP calls

**Questions?** Contact the Farm team or open an issue in the Farm repository.

---

**Version:** 1.0  
**Last Updated:** November 24, 2025  
**Maintained By:** AOS-Farm Team
