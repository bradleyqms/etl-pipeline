# QMS ETL Pipeline

Automated ETL pipeline that reads SAP B1 Report Dispatcher emails from Microsoft 365, uploads raw CSVs to Azure Blob Storage (Bronze layer), and transforms them to optimised Parquet files (Silver layer).

## Architecture

```
SAP B1 (CRON) → Email → Graph API → Bronze (CSV) → Silver (Parquet)
                                          ↕
                              State tracking (blob JSON)
```

**Storage:** `stqmssaledatalakeprod` / container `bronze`

```
bronze/
├── cold_extract/{date}/     ← Sales transactions (fact)
├── warm_extract/{date}/     ← Recent sales [PLANNED]
├── hot_extract/{date}/      ← Intraday sales [PLANNED]
├── dim_customer/{date}/     ← Customer master
├── dim_product/{date}/      ← Product master
├── dim_salesperson/{date}/  ← Salesperson master [PLANNED]
├── silver/
│   ├── cold_extract/{date}/ ← Parquet files
│   ├── dim_customer/{date}/ ← Per-entity + latest.parquet
│   └── dim_product/{date}/  ← Per-file + latest.parquet
└── state/                   ← Processed email ID tracking
```

## Project Structure

```
etl_pipeline/
├── src/
│   ├── core/
│   │   ├── graph_client.py     ← Graph API auth + mail helpers
│   │   ├── blob_client.py      ← Blob upload/list/state management
│   │   └── pipeline_runner.py  ← Generic email → blob engine
│   ├── pipelines/
│   │   ├── config.py           ← All pipeline definitions
│   │   ├── cold_extract.py     ← Sales transaction ingest
│   │   ├── dim_customer.py     ← Customer master ingest
│   │   └── dim_product.py      ← Product master ingest
│   ├── transforms/
│   │   ├── cold_extract_to_parquet.py  ← Sales CSV → Parquet
│   │   ├── dim_customer_to_parquet.py  ← Customer CSV → Parquet
│   │   └── dim_product_to_parquet.py   ← Product CSV → Parquet
│   └── cli.py                  ← Unified CLI entry point
├── azure_functions/
│   ├── function_app.py         ← 13 Azure Functions
│   ├── host.json
│   ├── local.settings.json     ← Local dev secrets (gitignored)
│   └── requirements.txt
├── .github/workflows/
│   └── azure-functions-deploy.yml
├── tests/
│   ├── conftest.py                 ← Shared fixtures & mock helpers
│   ├── test_pipeline_config.py     ← Pipeline config tests
│   ├── test_graph_client.py        ← Graph API logic tests
│   ├── test_transforms_cold_extract.py
│   ├── test_transforms_dim_customer.py
│   └── test_transforms_dim_product.py
├── .env                        ← Local dev secrets (gitignored)
├── .env.template               ← Template for .env
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Setup
```bash
cd etl_pipeline
pip install -r requirements.txt
cp .env.template .env
# Fill in .env with your credentials
```

### 2. CLI Usage
```bash
# List all pipelines
python -m src.cli list

# Test connectivity
python -m src.cli test cold_extract

# Ingest emails → Bronze blob
python -m src.cli ingest cold_extract              # New emails only (state-tracked)
python -m src.cli ingest cold_extract --dry-run    # Preview
python -m src.cli ingest cold_extract --all        # Reprocess all (oldest-first, newest wins)
python -m src.cli ingest dim_customer
python -m src.cli ingest dim_product

# Transform Bronze → Silver
python -m src.cli transform cold_extract           # Sales → Parquet
python -m src.cli transform dim_customer           # Customer → Parquet
python -m src.cli transform dim_product            # Product → Parquet
python -m src.cli transform all                    # All transforms
python -m src.cli transform cold_extract --date 2026-02-17
```

### 3. Testing
```bash
# Run all 55 unit tests
python -m pytest tests/ -v

# Run a specific test module
python -m pytest tests/test_transforms_cold_extract.py -v
python -m pytest tests/test_graph_client.py -v
```

Tests cover:
- **Pipeline config** — `get_pipeline()` merging, `list_pipelines()`, unknown pipeline errors
- **Graph API** — email sort order (oldest-first), subject filtering, pagination, processed-ID exclusion
- **cold_extract transform** — `=` separator parsing, German decimal conversion, date parsing, dtype casting
- **dim_customer transform** — RFC 4180 quoted CSV parsing (embedded commas/quotes), column normalisation
- **dim_product transform** — boolean flag normalisation (`Y`/`N`/`/`), numeric downcasting

## Azure Functions (13 functions)

| Function | Trigger | Schedule | Description |
|---|---|---|---|
| `cold_extract_timer` | Timer | `0 30 */6 * * *` | Sales email → Bronze |
| `cold_extract_http` | HTTP | On-demand | Same, manual |
| `dim_customer_timer` | Timer | `0 35 */6 * * *` | Customer email → Bronze |
| `dim_customer_http` | HTTP | On-demand | Same, manual |
| `dim_product_timer` | Timer | `0 40 */6 * * *` | Product email → Bronze |
| `dim_product_http` | HTTP | On-demand | Same, manual |
| `parquet_cold_timer` | Timer | `0 30 6 * * *` | Sales CSV → Parquet |
| `parquet_cold_http` | HTTP | On-demand | Same, manual |
| `parquet_dim_customer_timer` | Timer | `0 35 6 * * *` | Customer CSV → Parquet |
| `parquet_dim_customer_http` | HTTP | On-demand | Same, manual |
| `parquet_dim_product_timer` | Timer | `0 40 6 * * *` | Product CSV → Parquet |
| `parquet_dim_product_http` | HTTP | On-demand | Same, manual |
| `health` | HTTP | Always | Health check |

### Deploy
```bash
az functionapp publish func-qms-etl-prod --python
```
Or push to `main` branch — GitHub Actions deploys automatically.

## Environment Variables

| Variable | Description |
|---|---|
| `GRAPH_TENANT_ID` | Azure AD Tenant ID |
| `GRAPH_CLIENT_ID` | App Registration Client ID |
| `GRAPH_CLIENT_SECRET` | App Registration Client Secret |
| `PA_MAILBOX_ADDRESS` | Mailbox to read SAP emails from |
| `DATALAKE_ACCOUNT_NAME` | Storage account (default: `stqmssaledatalakeprod`) |
| `DATALAKE_ACCOUNT_KEY` | Storage account key |
