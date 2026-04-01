# ETL Pipeline SQL Queries

This folder contains all SQL queries used by the QMS Medicosmetics ETL pipeline to extract, transform, and load data from SAP B1 across four regional entities (GmbH, UK, AG/CH, INC/US).

## Overview

The queries folder is organized into three main categories:

1. **Fact Tables** – Sales transactions and incremental updates
2. **Dimension Tables** – Master data for customers, products, and salespeople
3. **Cold Extracts** – Historical bulk exports by year and entity

All queries output CSV-format text with `CHAR(34)` quoting to ensure compatibility with data lakes and ETL frameworks.

---

## Query Categories

### 1. Fact Sales Queries

#### `FACT_SALES_DAILY_INCREMENTAL.sql`

**Purpose:**  
Captures all modified or newly created sales transactions from the previous 30 days across all four entities (GmbH, UK, CH/AG, USA/INC).

**Scope:**  
- **Entities:** GmbH (default DB), UK (A20180_DES_P01), CH/AG (A20180_QMSCH_P01), USA/INC (A20180_QMSUSA_P01)
- **Document Types:** Invoices (ObjType=13) and Credit Notes (ObjType=19, ORIN table)
- **Time Filter:** `UpdateDate >= CAST(GETDATE()-30 AS DATE)`
- **Status Filter:** Excludes canceled documents (`CANCELED = 'N'`)

**Output Columns:**
```
Entity, DocEntry, DocNum, DocDate, DocType, Line_ID, Card Code, Item Code, 
Description, Quantity, Net Revenue, SlpCode, UpdateDate
```

**Key Logic:**
- **Net Revenue Calculation:**  
  ```
  (T1."TotalSumSy" - T1."VatSumSy") 
  * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100))  [Document-Level Discount]
  * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END) [Invoice vs Credit Note Sign]
  ```
- Applies document-level discount percent (`DiscPrcnt`) from the invoice header to net revenue
- Credit notes are inverted (multiplied by -1) for proper P&L flow
- Text fields are escaped with `REPLACE(CHAR(34), '""')` for CSV safety

**Usage:**  
Typically used by the Python ETL pipeline to build the "silver/fact_sales_daily" parquet layer. Results are then reconciled against cold extract history to build the gold star schema.

---

### 2. Cold Extract Queries – Historical Bulk Loads

#### `COLD_EXTRACT_{ENTITY}_{YEAR}.sql` Files

**Standard Files:**
- `COLD_EXTRACT_GMBH_2023.sql`, `COLD_EXTRACT_GMBH_2024.sql`, `COLD_EXTRACT_GMBH_2025.sql`, `COLD_EXTRACT_GMBH_2026.sql`
- `COLD_EXTRACT_UK_2023.sql`, `COLD_EXTRACT_UK_2024.sql`, `COLD_EXTRACT_UK_2025.sql`, `COLD_EXTRACT_UK_2026.sql`
- `COLD_EXTRACT_AG_2023.sql`, `COLD_EXTRACT_AG_2024.sql`, `COLD_EXTRACT_AG_2025.sql`, `COLD_EXTRACT_AG_2026.sql`
- `COLD_EXTRACT_INC_2023.sql`, `COLD_EXTRACT_INC_2024.sql`, `COLD_EXTRACT_INC_2025.sql`, `COLD_EXTRACT_INC_2026.sql`

**Purpose:**  
Provide year-to-date (YTD) or year-specific historical snapshots of sales transactions for each entity. Used to establish baseline fact data before incremental daily updates are applied.

**Scope:**
- **2023–2025 files:** Often use simple arithmetic (no CSV quoting) for backward compatibility or earlier pipeline versions
- **2026 files:** Use `CHAR(34)` quoting for consistent CSV safety across all entities

**Sample Output (2026 Format):**
```
"Entity", "DocEntry", "DocNum", "DocDate", "DocType", "Line_ID", "Card Code", "Item Code", 
"Description", "Quantity", "Net Revenue", "SlpCode", "UpdateDate"
```

**Key Differences by Year/Entity:**
- **2023 files:** May lack `CHAR(34)` quoting; included for historical data consistency
- **2024–2025 files:** Standard format, simple arithmetic in some regions
- **2026 files:** Full CSV quoting with document-level discount applied to all queries

**Date Filter Examples:**
```sql
WHERE T0."DocDate" BETWEEN '{YEAR}-01-01' AND CAST(GETDATE() AS DATE)
  AND T0."CANCELED" = 'N'
```

**Usage:**  
These are the source truth for historical reporting. The Python pipeline:
1. Reads each COLD_EXTRACT query result
2. Saves to silver layer as parquet (bronze → silver)
3. Reconciles with daily incremental updates to build gold fact tables
4. Uses these for year-over-year budget comparisons and audit trails

---

### 3. Dimension Table Queries

#### `dim_customer_ag_extract.sql`, `dim_customer_gmbh_extract.sql`, `dim_customer_uk_extract.sql`, `dim_customer_usa_extract.sql`

**Purpose:**  
Extract master customer data from SAP B1 across all four entities.

**Output Columns (Sample):**
```
Entity, CardCode, CardName, GroupName, 
BillToStreet, BillToCity, BillToZip, BillToCountry,
ShipToStreet, ShipToCity, ShipToZip, ShipToCountry,
TerritoryID, SlpCode, CreateDate, UpdateDate, IsActive
```

**Key Logic:**
- Filters for customer type (`CardType = 'C'`)
- Excludes null card codes
- Left-joins group/territory lookup tables for cleaner names
- Handles multi-entity variant naming (e.g., 'CH' vs 'AG')

**Usage:**  
Loaded into silver layer as `dim_customer/latest.parquet`, then enriched by Python transforms with:
- Market group and regional classification
- Hierarchy normalization (channel, market_group, company_group)
- Salesperson key joins for drilldown queries

---

#### `dim_product_master.sql`

**Purpose:**  
Extract product master data with SKU metadata, packaging, logistics, and web-shop flags.

**Output Columns (Sample):**
```
Entity, ItemCode, Description, ItemGroup, IsActive,
Webshop_Active, WS_Active_Flag, Is_Prov, Status, Parent_Item,
Weight_SU_kg, Weight_Primary_g, Weight_Secondary_g,
Content_ML, Content_GR, ProductLine, Name_EN, Variant_Dim1, CreateDate
```

**Key Logic:**
- Joins Item Master (OITM) with Item Group (OITB) for translated group names
- Captures custom fields (U_* columns) for webshop activation, variant dimensions, product lineup
- Includes packaging and content metadata for logistics/fulfillment

**Usage:**  
Loaded into silver layer as `dim_product/latest.parquet`, then enriched with:
- Product line cleanup and categorization
- SKU type and channel classification
- Sellable flag determination (based on webshop and wholesaler status)

---

#### `dim_salesperson.sql`

**Purpose:**  
Extract salesperson master data from all four SAP B1 instances.

**Output Columns:**
```
Entity, SlpCode, SlpName, Active
```

**Key Logic:**
- UNION across four entity databases
- Filters active salespeople (`Active = 'Y'`)

**Usage:**  
Loaded into silver layer as `dim_salesperson/latest.parquet`, then enriched by Python transforms with:
- Organization mapping (market group, region, sub-region)
- Display name normalization
- Territory and account assignment

---

## CSV Output Format & Data Safety

### CHAR(34) Quoting
All 2026 queries and recent cold extracts wrap text output with double-quote delimiters:
```sql
CHAR(34) + 'Some Text' + CHAR(34)  --> "Some Text"
```

### Escape Handling
Embedded quotes in string fields are doubled:
```sql
REPLACE(CAST(field AS NVARCHAR(MAX)), CHAR(34), '""')
```

**Example:**
- Input: `Customer's "Best" Store`
- Output: `"Customer's ""Best"" Store"`

This ensures compatibility with CSV readers and prevents parsing errors in the ETL pipeline.

---

## Revenue Calculation & Discount Logic

### Document-Level Discount (`DiscPrcnt`)

Starting **March 2026**, all fact sales queries apply a document-level discount percent from the invoice header:

```sql
Net Revenue = (Total Amount - VAT) 
            × (1 - (DiscPrcnt / 100)) 
            × (1 for Invoice; -1 for Credit Note)
```

**Example:**
- Invoice Line Total: 1,000 EUR (after line discounts)
- Invoice VAT: 190 EUR
- Header Discount: 5%
- **Net Revenue = (1000 - 190) × (1 - 0.05) × 1 = 765.50 EUR**

**Rationale:**
- SAP B1 line amounts already include line-item discounts
- Header discounts must be applied separately at calculation time
- This ensures P&L accuracy for promotional campaigns, volume rebates, and customer-level agreements

### Historical Notes
- **2023–2025 queries:** May not apply header discounts (check individual file comments)
- **2026 queries:** All fact tables and cold extracts apply full discount logic
- If you need pre-discount revenue, reverse-apply: `NetRevenue / (1 - DiscPrcnt/100)`

---

## Integration with Python ETL Pipeline

### Data Flow

```
SAP B1 Databases (4 entities)
        ↓
[SQL Queries in /queries folder]
        ↓
[Python: etl_pipeline/src/transforms/]
        ├─ silver/fact_sales_daily/{date}/fact_sales_daily_{entity}.parquet
        ├─ silver/cold_extract/*.parquet
        ├─ silver/dim_customer/latest_enriched.parquet
        ├─ silver/dim_product/latest_enriched.parquet
        ├─ silver/dim_salesperson/latest_enriched.parquet
        ↓
[Gold Star Schema: etl_pipeline/src/transforms/build_gold.py]
        ├─ fact_sales.parquet (reconciled incremental + historical)
        ├─ dim_customer.parquet (enriched with hierarchies)
        ├─ dim_product.parquet (enriched with categorization)
        ├─ dim_salesperson.parquet (enriched with org mapping)
        ├─ fact_budget.parquet (from external workbooks)
        └─ dim_date.parquet (calendar dimension)
```

### Key Python Transforms

| Module | Input Query | Output Table |
|--------|------------|--------------|
| `load_cold_extract_history()` | COLD_EXTRACT_*.sql | `silver/cold_extract/*.parquet` |
| `load_latest_daily_incremental()` | FACT_SALES_DAILY_INCREMENTAL.sql | `silver/fact_sales_daily/{date}/*.parquet` |
| `prepare_dim_customer()` | dim_customer_*_extract.sql | `gold/dim_customer.parquet` |
| `prepare_dim_product()` | dim_product_master.sql | `gold/dim_product.parquet` |
| `prepare_dim_salesperson()` | dim_salesperson.sql | `gold/dim_salesperson.parquet` |
| `prepare_fact_sales()` | cold + daily (merged) | `gold/fact_sales.parquet` |

---

## File Naming Conventions

### Pattern: `{TYPE}_{ENTITY}_{YEAR_or_SCOPE}.sql`

**Types:**
- `FACT_SALES_DAILY_INCREMENTAL` – Last 30 days of transactions
- `COLD_EXTRACT` – Year-to-date or full-year historical snapshot
- `dim_customer`, `dim_product`, `dim_salesperson` – Dimension master data

**Entities:**
- `GMBH` – Germany (QMS GmbH, default SAP instance)
- `UK` – United Kingdom (A20180_DES_P01)
- `AG` – Switzerland (A20180_QMSCH_P01, maps to Entity 'CH')
- `INC` – USA (A20180_QMSUSA_P01, maps to Entity 'US')

**Years:**
- `2023`, `2024`, `2025`, `2026` – Cold extracts by specific year

**Examples:**
- `COLD_EXTRACT_GMBH_2025.sql` → Germany, Year-to-date 2025
- `COLD_EXTRACT_UK_2026.sql` → UK, Year-to-date 2026
- `FACT_SALES_DAILY_INCREMENTAL.sql` → All entities, last 30 days
- `dim_customer_ag_extract.sql` → Switzerland customer master

---

## Maintenance & Updates

### When to Update Queries

1. **New SAP field added to B1:**
   - Add `T0."NewField"` to the SELECT list
   - Update corresponding Python transform to handle the column
   - Test against a cold extract before rolling to production

2. **Entity addition (new country/company):**
   - Create new `COLD_EXTRACT_{ENTITY}_{YEAR}.sql` for historical data
   - Add UNION clause to `FACT_SALES_DAILY_INCREMENTAL.sql`
   - Add `dim_customer_{entity}_extract.sql` for new master data

3. **Discount or revenue calculation change:**
   - Update BOTH `FACT_SALES_DAILY_INCREMENTAL.sql` AND all active `COLD_EXTRACT_{ENTITY}_{YEAR}.sql` files
   - Ensure consistent formula across all queries
   - Add comment with change date and reason

4. **CSV format or escaping changes:**
   - Apply consistently across all output columns
   - Test escaping with fields containing quotes, commas, line breaks
   - Verify downstream parquet readers can handle output

### Testing Checklist

- [ ] Run query in SAP B1 Query Manager → verify row counts
- [ ] Check for NULL values in key columns (Entity, DocDate, Card Code, Item Code)
- [ ] Verify document-level discount application on sample sales docs
- [ ] Compare 2026 cold extract totals vs. 2025 for trend sanity
- [ ] Load resulting CSV into Python pandas/pyarrow → ensure no parse errors
- [ ] Run gold model build and validate key fill rates (>99% for customer_key, product_key)

---

## Common Issues & Resolutions

### Issue: "Discount Percent Mismatch"
**Symptom:** Net revenue doesn't match SAP invoice detail screen  
**Cause:** Line discounts (INV1.DiscPrcnt) vs. header discounts (OINV.DiscPrcnt)  
**Resolution:** These queries use **header discount only**. If line discounts matter, calculate as: `(TotalSumSy - VatSumSy - LineTotalDiscount) × (1 - HeaderDiscount/100)`

### Issue: "Missing Recent Transactions"
**Symptom:** Daily incremental query returns 0 or old rows  
**Cause:** UpdateDate filter or incremental load overlap  
**Resolution:** Check `WHERE T0."UpdateDate" >= CAST(GETDATE()-30 AS DATE)`. Ensure Python pipeline runs daily and doesn't re-process overlapping dates.

### Issue: "CSV Parse Errors in Python"
**Symptom:** `ParsingError` or `UnicodeDecodeError` when loading CSV export  
**Cause:** Unescaped quotes or special characters in description fields  
**Resolution:** Ensure all text columns use `REPLACE(..., CHAR(34), '""')` before CHAR(34) wrapping.

### Issue: "Intercompany Transactions Inflated"
**Symptom:** Interco revenue doesn't match budget or management reports  
**Cause:** Channel classification not applied; all transactions included  
**Resolution:** Python `prepare_fact_sales()` excludes known interco customers. Verify `dim_customer.channel = 'Interco'` for all intra-company accounts.

---

## Related Documentation

- **ETL Pipeline Main README:** See `etl_pipeline/README.md` for overview
- **Build Gold Transform:** `etl_pipeline/src/transforms/build_gold.py` – Orchestrates all queries and enrichment logic
- **DAX Measures:** `etl_pipeline/DAX_MEASURES.md` – Defines how revenue, budget, and variance are calculated in Power BI
- **Data Dictionary:** Output columns and transforms documented in `gold/` layer Parquet schema

---

## Contact & Support

For query modifications, performance tuning, or adding new entities:

1. **Verify in TEST first** – Always test changes against a dev SAP instance
2. **Update both incremental and cold extracts** – Ensure consistency
3. **Document changes with comments** – Add date and reason to SQL header
4. **Run gold rebuild** – Verify downstream parquet schemas are valid
5. **Validate against reports** – Compare totals to management P&L and budget reports

---

**Last Updated:** March 31, 2026  
**Queries Version:** 2026-Q1  
**Key Change:** Document-level discount (`DiscPrcnt`) applied to all fact queries.
