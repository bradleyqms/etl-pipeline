# DAX Measures — QMS Sales Prep

Create a new table first (Modeling → New table):

```
_Measures = DATATABLE("Placeholder", STRING, {{""}})
```

Then right-click `_Measures` → **New measure** for each one below.

---

## 1. Net Revenue EUR

```
Net Revenue EUR = SUM(fact_sales[revenue_eur])
```

## 2. Budget EUR

```
Budget EUR = SUM(fact_budget[budget_amount_eur])
```

## 3. Net Revenue EUR PY

```
Net Revenue EUR PY = CALCULATE([Net Revenue EUR], SAMEPERIODLASTYEAR(dim_date[date]))
```

## 4. Units

```
Units = SUM(fact_sales[quantity])
```

## 5. vs Budget

```
vs Budget = [Net Revenue EUR] - [Budget EUR]
```

## 6. vs Budget %

```
vs Budget % = DIVIDE([vs Budget], [Budget EUR])
```

## 7. vs PY

```
vs PY = [Net Revenue EUR] - [Net Revenue EUR PY]
```

## 8. vs PY %

```
vs PY % = DIVIDE([vs PY], [Net Revenue EUR PY])
```

## 9. % of Total Revenue

```
% of Total Revenue = DIVIDE([Net Revenue EUR], CALCULATE([Net Revenue EUR], ALL(dim_product)))
```
