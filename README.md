# QMS ETL Pipeline

Automated ETL pipeline that reads SAP B1 Report Dispatcher emails from Microsoft 365, uploads raw CSVs to Azure Blob Storage (Bronze layer), transforms them to optimised Parquet files (Silver layer), and applies business-logic enrichment to dimension tables for direct Power BI consumption.

## Architecture

```
SAP B1 (CRON)
    │ daily email (07:30 UTC Mon–Fri)
    ▼
Microsoft 365 Mailbox
    │ Microsoft Graph API
    ▼
Bronze Layer  (azure blob: bronze/)
    │  raw CSVs, state-tracked per pipeline
    ▼
Silver Layer  (azure blob: bronze/silver/)
    │  columnar Parquet (zstd), deduplicated
    ▼
Enriched Silver  (latest_enriched.parquet)
    │  market_group, channel, region, product_category, sku_type, …
    ▼
Power BI / downstream analytics
```

**Storage account:** `stqmssaledatalakeprod`  
**Container:** `bronze`  
**State tracking:** blob JSON at `bronze/state/{pipeline}_state.json`

### Blob Layout

```
bronze/
├── cold_extract/{date}/            ← Monthly full SAP sales extract (fact)
├── fact_sales_daily/{date}/        ← Daily incremental SAP sales extract (fact)
├── dim_tables/{date}/              ← All dimension CSVs (customer, product, salesperson)
└── state/                          ← Processed email ID tracking

bronze/silver/
├── cold_extract/{date}/            ← Per-entity Parquet files
│   └── fact_sales_{entity}_{year}.parquet
├── fact_sales_daily/{date}/        ← Per-entity daily Parquet files
│   └── fact_sales_daily_{entity}.parquet
├── dim_customer/
│   ├── {date}/                     ← Per-entity Parquet files
│   ├── latest.parquet              ← Combined, deduplicated (4,410 rows)
│   └── latest_enriched.parquet     ← + market_group, channel, region, company_group
├── dim_product/
│   ├── {date}/
│   ├── latest.parquet              ← Combined, deduplicated (5,478 rows)
│   └── latest_enriched.parquet     ← + product_line_clean, product_category, sku_type, is_sellable
└── dim_salesperson/
    ├── {date}/
    └── latest.parquet              ← Combined, deduplicated (132 rows)
```

---

## Project Structure

```
etl_pipeline/
├── src/
│   ├── core/
│   │   ├── graph_client.py         ← Graph API auth, mail folder search, email fetch
│   │   ├── blob_client.py          ← ContainerClient factory, upload/list, state R/W
│   │   └── pipeline_runner.py      ← Generic email → blob engine (auto_transform support)
│   ├── pipelines/
│   │   └── config.py               ← All pipeline definitions (cold_extract, fact_sales_daily, dim_tables)
│   └── transforms/
│       ├── cold_extract_to_parquet.py      ← Monthly sales CSV → Silver Parquet
│       ├── fact_sales_daily_to_parquet.py  ← Daily incremental CSV → Silver Parquet
│       ├── dim_tables_to_parquet.py        ← Orchestrator: routes dim CSVs to sub-transforms
│       ├── dim_customer_to_parquet.py      ← Customer master CSV → Parquet (v2 SAP schema)
│       ├── dim_product_to_parquet.py       ← Product master CSV → Parquet (v2 SAP schema)
│       ├── dim_salesperson_to_parquet.py   ← Salesperson CSV → Parquet
│       ├── enrich_dim_customer.py          ← Enrichment: market_group, channel, region
│       └── enrich_dim_product.py           ← Enrichment: product_category, sku_type, is_sellable
├── azure_functions/
│   └── function_app.py             ← 13 Azure Functions (timers + HTTP manual triggers)
├── tests/
│   ├── conftest.py                         ← Shared fixtures (v2 SAP schema CSV strings)
│   ├── test_pipeline_config.py             ← Pipeline config merging
│   ├── test_transforms_dim_customer.py     ← Customer CSV parsing
│   ├── test_transforms_dim_product.py      ← Product CSV parsing
│   ├── test_enrich_dim_customer.py         ← Customer enrichment rules (26 tests)
│   └── test_enrich_dim_product.py          ← Product enrichment rules (37 tests)
├── .env                            ← Local dev secrets (gitignored)
├── .env.template                   ← Credential template
├── requirements.txt
└── README.md
```

---

## Pipelines

### 1. cold_extract — Monthly Sales Fact
- **SAP schedule:** 1st of month, 06:00 UTC
- **Azure ingest:** 1st of month, 09:00 UTC (3-hr buffer for large query)
- **Bronze prefix:** `cold_extract/{date}/`
- **Format:** `=` separated CSV, German decimal notation (`1.234,56`), UTF-8/latin-1
- **Silver output:** `silver/cold_extract/{date}/fact_sales_{entity}_{year}.parquet`
- **Deduplication:** by `(entity, doc_entry, line_num)` — latest wins

### 2. fact_sales_daily — Daily Incremental Sales Fact
- **SAP schedule:** Mon–Fri, 06:00 UTC
- **Azure ingest:** Mon–Fri, 07:30 UTC
- **Bronze prefix:** `fact_sales_daily/{date}/`
- **Format:** comma-separated, CHAR(34) quoted, period decimals
- **Silver output:** `silver/fact_sales_daily/{date}/fact_sales_daily_{entity}.parquet`
- **Deduplication:** by `(entity, doc_entry, line_num)` — covers a 30-day rolling window to bridge monthly cold extracts
- **Stale cleanup:** old silver parquets for a date are automatically deleted after each run

### 3. dim_tables — Dimension Tables (Orchestrated)
- **SAP schedule:** Mon–Fri, 06:00 UTC (single email with all dim CSVs attached)
- **Azure ingest:** Mon–Fri, 07:30 UTC
- **Bronze prefix:** `dim_tables/{date}/`
- **Routing:** filename pattern → sub-transform
  - `dim_customer_*` → `dim_customer_to_parquet`
  - `dim_product_*` / `product_master` → `dim_product_to_parquet`
  - `dim_salesperson*` → `dim_salesperson_to_parquet`
- **Silver outputs:**
  - `silver/dim_customer/latest.parquet` — 4,410 rows (4 entities: GmbH, UK, AG, US)
  - `silver/dim_product/latest.parquet` — 5,478 rows
  - `silver/dim_salesperson/latest.parquet` — 132 rows

---

## Dimension Enrichment

After the Silver parquet is written, two enrichment transforms add business-context columns that are not present in the raw SAP extract. These write to `latest_enriched.parquet`.

### enrich_dim_customer

Adds four columns to `dim_customer`:

| Column | Description | Example values |
|---|---|---|
| `market_group` | Top-level market segment | `Core Markets`, `Export`, `UK`, `USA` |
| `channel` | Sales channel | `B2C Online`, `B2B Trade`, `B2B Distributor`, `Spa`, `Internal`, `Interco` |
| `region` | Geographic sub-region | `Germany`, `Switzerland`, `Benelux`, `Nordics`, `France`, `Spain`, `Italy` |
| `company_group` | QMS legal entity | `QMS Medicosmetics GmbH`, `Descomed Ltd`, `QMS Medicosmetics AG`, `QMS Medicosmetics Inc.` |

**Resolution order (first match wins):**
1. `entity_mappings.csv` — explicit card_code lookup (highest trust, 146 accounts explicitly mapped)
2. Entity shortcut — `US` → USA, `AG` → Core Markets (Switzerland region), `UK` → UK
3. Card-code prefix — numeric ranges encode entity sub-types (e.g. `40xx`/`41xx` = B2C Online, `10xx` = B2B Distributor)
4. `group_name` field — SAP group label (e.g. `Endverbraucher` → B2C Online, `Mitarbeiter` → Internal)
5. `territory_id` — SAP territory hierarchy (18 territory IDs mapped)
6. `bill_to_country` — ISO-2 country code fallback (50 countries mapped)
7. Final fallback → `Export / International`

**Market group taxonomy (from entity_mappings):**

| market_group | Regions |
|---|---|
| Core Markets | Germany, Switzerland, France, Benelux, Spain, Italy, Nordics, UK |
| Export | Distributor-APAC, Distributor-Russia, Distributor-South Africa, Eastern Europe, Export-Direct, Other ROW |
| UK | UK (Descomed Ltd) |
| USA | West, Northeast, Southeast, Central, Retail, Americas |

**Current coverage:** 100% (4,410 / 4,410 rows enriched)  
**Distribution:** Core Markets 3,802 · Export 248 · UK 228 · USA 132

### enrich_dim_product

Adds five columns to `dim_product`:

| Column | Description | Example values |
|---|---|---|
| `product_line_clean` | Normalised product line (consistent casing) | `Hydromax`, `Derma Expert`, `Age Prevent`, `Packaging` |
| `product_category` | High-level category | `Skincare Treatment`, `Promotional`, `Packaging`, `Accessories`, `Merchandising`, `Uncategorised` |
| `sku_type` | Functional SKU classification | `product`, `packaging`, `internal`, `promotional`, `accessories`, `service`, `provisional` |
| `is_sellable` | True if active + not internal/packaging/service | `True` / `False` |
| `item_code_prefix` | First 2 characters of item code | `10`, `11`, `A2` |

**SKU type rules:**
- `item_code` starts with `A` → `internal` (SAP discount/scheme codes)
- `item_code` ≤ 3 digits → `service` (short numeric codes)
- Description contains service keywords (`shipment cost`, `Versandkosten`, `Rückerstattung`) → `service`
- `product_line` contains packaging keywords → `packaging`
- `product_line` contains promotional/set keywords → `promotional`
- `product_line` contains accessories keywords → `accessories`
- `is_provisional = Y` → `provisional`
- Otherwise → `product`

**`is_sellable`** = `is_active == "Y"` AND `sku_type NOT IN (internal, packaging, service)`

**Current coverage:** 5,478 rows  
**Sellable SKUs:** 1,114 (20.3%)  
**Distribution:** Skincare Treatment 3,657 · Uncategorised 697 · Packaging 464 · Promotional 445 · Merchandising 122 · Accessories 93

---

## Azure Functions

All functions deployed to `func-qms-etl-prod` (Germany West Central, Flex Consumption plan).  
Schedules are aligned to SAP Report Dispatcher dispatch timing.

| # | Function | Trigger | Schedule (UTC) | Description |
|---|---|---|---|---|
| 1 | `cold_extract_timer` | Timer | `0 0 9 1 * *` (1st of month) | Cold extract email → Bronze |
| 2 | `cold_extract_http` | HTTP | Manual | Same, on demand |
| 3 | `fact_sales_daily_timer` | Timer | `0 30 7 * * 1-5` (Mon–Fri) | Daily sales email → Bronze |
| 4 | `fact_sales_daily_http` | HTTP | Manual | Same, on demand |
| 5 | `dim_tables_timer` | Timer | `0 30 7 * * 1-5` (Mon–Fri) | Dim tables email → Bronze |
| 6 | `dim_tables_http` | HTTP | Manual | Same, on demand |
| 7 | `parquet_dim_tables_timer` | Timer | `0 35 7 * * 1-5` (Mon–Fri) | Dim CSVs → Silver Parquet |
| 8 | `parquet_dim_tables_http` | HTTP | Manual | Same, on demand |
| 9 | `parquet_cold_timer` | Timer | `0 5 9 1 * *` (1st of month) | Cold CSV → Silver Parquet |
| 10 | `parquet_cold_http` | HTTP | Manual | Same, on demand |
| 11 | `parquet_daily_timer` | Timer | `0 35 7 * * 1-5` (Mon–Fri) | Daily CSV → Silver Parquet |
| 12 | `parquet_daily_http` | HTTP | Manual | Same, on demand |
| 13 | `health` | HTTP GET | Always | Health check / status |

### Deploy

```bash
az functionapp publish func-qms-etl-prod --python
```

Or push to `main` — GitHub Actions (`.github/workflows/azure-functions-deploy.yml`) deploys automatically via OIDC.

---

## Quick Start

### 1. Setup

```bash
cd etl_pipeline
pip install -r requirements.txt
cp .env.template .env
# Fill in .env with your credentials
```

### 2. CLI

```bash
# List all configured pipelines
python -m src.cli list

# Test connectivity (Graph API + Blob)
python -m src.cli test cold_extract

# Ingest emails → Bronze blob (state-tracked, skips already-processed)
python -m src.cli ingest cold_extract
python -m src.cli ingest fact_sales_daily
python -m src.cli ingest dim_tables

# Flags
python -m src.cli ingest cold_extract --dry-run      # Preview, no upload
python -m src.cli ingest cold_extract --all          # Reprocess all emails
python -m src.cli ingest cold_extract --no-transform # Ingest only, skip Silver step

# Transform Bronze CSV → Silver Parquet
python -m src.cli transform cold_extract
python -m src.cli transform fact_sales_daily
python -m src.cli transform dim_tables
python -m src.cli transform all
python -m src.cli transform cold_extract --date 2026-02-17
```

### 3. Run Enrichment (manual)

The enrichment transforms read from Silver and write `latest_enriched.parquet` back to the same Silver folder.

```python
from dotenv import load_dotenv
load_dotenv()

from src.transforms import enrich_dim_customer, enrich_dim_product

# Preview only
enrich_dim_customer.transform(dry_run=True)
enrich_dim_product.transform(dry_run=True)

# Write enriched parquets
enrich_dim_customer.transform()
enrich_dim_product.transform()
```

### 4. Testing

```bash
# Run all 123 unit tests
python -m pytest tests/ -v

# Run specific suites
python -m pytest tests/test_enrich_dim_customer.py -v   # 26 enrichment tests
python -m pytest tests/test_enrich_dim_product.py -v    # 37 enrichment tests
```

**Test coverage:**

| Module | Tests | What's tested |
|---|---|---|
| `test_pipeline_config.py` | 5 | `get_pipeline()` merging, `list_pipelines()`, unknown pipeline error |
| `test_transforms_dim_customer.py` | 8 | CSV parsing, column normalisation, v2 schema fields |
| `test_transforms_dim_product.py` | 15 | Boolean normalisation, numeric downcasting, v2 19-column schema |
| `test_enrich_dim_customer.py` | 26 | `_derive_row` (entity shortcuts, CC prefixes, territory IDs, country fallbacks, group_name→channel), `enrich()` integration, CSV-wins-over-rules, whitespace stripping |
| `test_enrich_dim_product.py` | 37 | `_normalise_product_line`, `_derive_product_category`, `_derive_sku_type`, `_is_sellable`, `enrich()` integration |

---

## Environment Variables

| Variable | Description |
|---|---|
| `GRAPH_TENANT_ID` | Azure AD Tenant ID |
| `GRAPH_CLIENT_ID` | App Registration Client ID |
| `GRAPH_CLIENT_SECRET` | App Registration Client Secret |
| `PA_MAILBOX_ADDRESS` | Mailbox to read SAP emails from |
| `DATALAKE_ACCOUNT_NAME` | Storage account name (default: `stqmssaledatalakeprod`) |
| `DATALAKE_ACCOUNT_KEY` | Storage account key |
| `DATALAKE_CONNECTION_STRING` | Alternative to account key (connection string) |

Set these in `.env` for local development. In Azure, set them as **Application Settings** in `func-qms-etl-prod → Configuration`.
