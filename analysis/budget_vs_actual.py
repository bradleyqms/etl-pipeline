"""
Budget vs Actual report — by salesperson by month (2025 & 2026)
Output: data/outputs/budget_vs_actual.html
"""
import io, os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from src.transforms.build_gold import GOLD_PATHS, get_container_client

client = get_container_client()

def rp(p):
    return pd.read_parquet(io.BytesIO(client.get_blob_client(p).download_blob().readall()))

fact_sales   = rp(GOLD_PATHS["fact_sales"])
fact_budget  = rp(GOLD_PATHS["fact_budget"])
dim_customer = rp(GOLD_PATHS["dim_customer"])
dim_slp      = rp(GOLD_PATHS["dim_salesperson"])

# ── Prep actuals ──────────────────────────────────────────────────────────────
fact_sales["month"] = fact_sales["doc_date"].dt.to_period("M")
fact_sales["year"]  = fact_sales["doc_date"].dt.year
sales = fact_sales[fact_sales["year"].isin([2025, 2026])].copy()

# Bring in hierarchy from dim_customer
hier = dim_customer[["customer_key", "market_group", "region", "sub_region", "channel"]].drop_duplicates("customer_key")
sales = sales.merge(hier, on="customer_key", how="left")

# Bring in salesperson display name
slp_names = dim_slp[["salesperson_key", "display_name", "sub_region"]].rename(
    columns={"sub_region": "slp_sub_region", "display_name": "salesperson"}
).drop_duplicates("salesperson_key")
sales = sales.merge(slp_names, on="salesperson_key", how="left")
sales["salesperson"] = sales["salesperson"].fillna("Unassigned")

# Aggregate actuals
act_by_slp_month = (
    sales.groupby(["salesperson", "month", "market_group", "region", "sub_region"], dropna=False)["revenue_eur"]
    .sum().reset_index().rename(columns={"revenue_eur": "actual"})
)
act_total_month = (
    sales.groupby("month")["revenue_eur"].sum().reset_index().rename(columns={"revenue_eur": "actual"})
)

# ── Prep budget ───────────────────────────────────────────────────────────────
fact_budget["month"]       = pd.to_datetime(fact_budget["budget_month"]).dt.to_period("M")
fact_budget["year"]        = pd.to_datetime(fact_budget["budget_month"]).dt.year
budget = fact_budget[fact_budget["year"].isin([2025, 2026])].copy()

# Use sales_person name from budget file directly (most granular)
budget["salesperson"] = budget["sales_person"].fillna("Unassigned")

bud_by_slp_month = (
    budget.groupby(["salesperson", "month", "market_group", "region", "sub_region"], dropna=False)["budget_amount_eur"]
    .sum().reset_index().rename(columns={"budget_amount_eur": "budget"})
)
bud_total_month = (
    budget.groupby("month")["budget_amount_eur"].sum().reset_index().rename(columns={"budget_amount_eur": "budget"})
)

# ── Merge actuals + budget at salesperson / month level ───────────────────────
# Use budget as the spine (budget defines the territory/salesperson assignment)
merged = bud_by_slp_month.merge(
    act_by_slp_month[["salesperson", "month", "actual"]].groupby(["salesperson", "month"])["actual"].sum().reset_index(),
    on=["salesperson", "month"], how="outer"
).fillna({"actual": 0.0, "budget": 0.0})

merged["variance"]   = merged["actual"] - merged["budget"]
merged["attainment"] = (merged["actual"] / merged["budget"].replace(0, float("nan")) * 100).round(1)
merged["month_str"]  = merged["month"].astype(str)
merged["year"]       = merged["month"].apply(lambda p: p.year)

# ── Overall monthly total summary ─────────────────────────────────────────────
total_summary = bud_total_month.merge(act_total_month, on="month", how="outer").fillna(0)
total_summary["variance"]   = total_summary["actual"] - total_summary["budget"]
total_summary["attainment"] = (total_summary["actual"] / total_summary["budget"].replace(0, float("nan")) * 100).round(1)
total_summary["month_str"]  = total_summary["month"].astype(str)
total_summary["year"]       = total_summary["month"].apply(lambda p: p.year)
total_summary = total_summary.sort_values("month")

# ── Salesperson pivot summary (all months, for table) ─────────────────────────
slp_summary = (
    merged.groupby(["salesperson", "market_group", "region", "sub_region", "year"])
    .agg(budget=("budget", "sum"), actual=("actual", "sum")).reset_index()
)
slp_summary["variance"]   = slp_summary["actual"] - slp_summary["budget"]
slp_summary["attainment"] = (slp_summary["actual"] / slp_summary["budget"].replace(0, float("nan")) * 100).round(1)

# ── Build HTML ────────────────────────────────────────────────────────────────
def fmt_eur(v):
    if pd.isna(v): return "—"
    return f"€{v:,.0f}"

def fmt_pct(v):
    if pd.isna(v): return "—"
    color = "green" if v >= 100 else ("orange" if v >= 80 else "red")
    return f'<span style="color:{color};font-weight:bold">{v:.1f}%</span>'

def fmt_var(v):
    if pd.isna(v): return "—"
    color = "green" if v >= 0 else "red"
    sign  = "+" if v > 0 else ""
    return f'<span style="color:{color}">{sign}€{v:,.0f}</span>'

# ─ Chart 1: Monthly total actuals vs budget bar chart ────────────────────────
months_all = total_summary["month_str"].tolist()
fig_summary = go.Figure()
fig_summary.add_bar(
    x=months_all, y=total_summary["budget"].tolist(),
    name="Budget", marker_color="#b0c4de", opacity=0.8,
)
fig_summary.add_bar(
    x=months_all, y=total_summary["actual"].tolist(),
    name="Actual", marker_color="#1f77b4",
)
fig_summary.add_scatter(
    x=months_all, y=total_summary["attainment"].tolist(),
    name="Attainment %", yaxis="y2",
    mode="lines+markers", line=dict(color="#ff7f0e", width=2),
    marker=dict(size=7),
)
fig_summary.update_layout(
    title="Total Revenue: Actual vs Budget by Month",
    barmode="group",
    yaxis=dict(title="€", tickprefix="€", tickformat=",.0f"),
    yaxis2=dict(title="Attainment %", overlaying="y", side="right", ticksuffix="%", range=[0, 150]),
    legend=dict(orientation="h", y=1.1),
    height=380, margin=dict(t=60, b=40),
)

chart_html = fig_summary.to_html(full_html=False, include_plotlyjs="cdn")

# ─ Table 1: Monthly summary ──────────────────────────────────────────────────
def monthly_summary_html():
    rows = []
    for _, r in total_summary.iterrows():
        rows.append(f"""
        <tr>
          <td>{r['month_str']}</td>
          <td class='num'>{fmt_eur(r['budget'])}</td>
          <td class='num'>{fmt_eur(r['actual'])}</td>
          <td class='num'>{fmt_var(r['variance'])}</td>
          <td class='num'>{fmt_pct(r['attainment'])}</td>
        </tr>""")
    return "".join(rows)

# ─ Table 2: Salesperson monthly detail ───────────────────────────────────────
def slp_monthly_html(year: int):
    d = merged[merged["year"] == year].sort_values(["salesperson", "month"])
    months = sorted(d["month"].unique())
    month_labels = [str(m) for m in months]
    
    # Get all salesperson/region combos
    sp_groups = (
        d.groupby(["salesperson", "market_group", "region", "sub_region"], dropna=False)
         .agg(budget=("budget", "sum"), actual=("actual", "sum")).reset_index()
         .sort_values(["market_group", "salesperson"])
    )
    
    header_months = "".join(f"<th colspan='2'>{m}</th>" for m in month_labels)
    sub_header = "".join("<th>Actual</th><th>Budget</th>" for _ in months)
    
    rows_html = []
    for _, sp in sp_groups.iterrows():
        sp_name = sp["salesperson"]
        mg      = sp["market_group"] if pd.notna(sp["market_group"]) else "—"
        rg      = sp["region"] if pd.notna(sp["region"]) else "—"
        sr      = sp["sub_region"] if pd.notna(sp["sub_region"]) else ""
        loc     = f"{rg}" + (f" › {sr}" if sr else "")
        
        att = sp["actual"] / sp["budget"] * 100 if sp["budget"] > 0 else None
        att_str = fmt_pct(att) if att is not None else "—"
        total_row = f"<td class='num'>{fmt_eur(sp['actual'])}</td><td class='num'>{fmt_eur(sp['budget'])}</td><td class='num'>{fmt_var(sp['actual']-sp['budget'])}</td><td class='num'>{att_str}</td>"
        
        sp_months = d[
            (d["salesperson"] == sp_name) &
            (d["market_group"].fillna("__NA__") == (sp["market_group"] if pd.notna(sp["market_group"]) else "__NA__")) &
            (d["region"].fillna("__NA__") == (sp["region"] if pd.notna(sp["region"]) else "__NA__"))
        ].set_index("month")
        
        month_cells = ""
        for m in months:
            if m in sp_months.index:
                row_m = sp_months.loc[m]
                act_v = row_m["actual"] if hasattr(row_m, '__len__') and len(row_m.shape) > 0 else row_m["actual"]
                bud_v = row_m["budget"] if hasattr(row_m, '__len__') and len(row_m.shape) > 0 else row_m["budget"]
                # handle duplicate rows
                if isinstance(act_v, pd.Series): act_v = act_v.sum()
                if isinstance(bud_v, pd.Series): bud_v = bud_v.sum()
            else:
                act_v, bud_v = 0.0, 0.0
            month_cells += f"<td class='num'>{fmt_eur(act_v)}</td><td class='num sm'>{fmt_eur(bud_v)}</td>"
        
        rows_html.append(f"""
        <tr>
          <td>{sp_name}</td>
          <td>{mg}</td>
          <td>{loc}</td>
          {total_row}
          {month_cells}
        </tr>""")
    
    return f"""
    <table class="data-table" id="slp-table-{year}">
      <thead>
        <tr>
          <th rowspan='2'>Salesperson</th>
          <th rowspan='2'>Market Group</th>
          <th rowspan='2'>Region / Sub Region</th>
          <th rowspan='2'>Actual YTD</th>
          <th rowspan='2'>Budget YTD</th>
          <th rowspan='2'>Variance</th>
          <th rowspan='2'>Attainment</th>
          {header_months}
        </tr>
        <tr>{sub_header}</tr>
      </thead>
      <tbody>{"".join(rows_html)}</tbody>
    </table>"""

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Budget vs Actual Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 12px; margin: 20px; background: #f8f9fa; color: #333; }}
  h1 {{ font-size: 20px; color: #1a1a2e; }}
  h2 {{ font-size: 15px; color: #333; margin-top: 30px; border-bottom: 2px solid #ddd; padding-bottom: 4px; }}
  .data-table {{ border-collapse: collapse; width: 100%; background: white; box-shadow: 0 1px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
  .data-table th {{ background: #2c3e50; color: white; padding: 6px 10px; text-align: center; font-size: 11px; white-space: nowrap; }}
  .data-table td {{ padding: 5px 10px; border-bottom: 1px solid #eee; white-space: nowrap; }}
  .data-table tr:hover {{ background: #f0f7ff; }}
  .num {{ text-align: right; font-family: monospace; }}
  .sm  {{ color: #888; }}
  .summary-table {{ width: 600px; }}
  .tab-bar {{ display: flex; gap: 8px; margin-bottom: 12px; }}
  .tab-btn {{ padding: 8px 18px; border: none; border-radius: 4px; cursor: pointer; background: #ddd; font-size: 12px; }}
  .tab-btn.active {{ background: #2c3e50; color: white; }}
  .tab-panel {{ display: none; }}
  .tab-panel.active {{ display: block; }}
  .overflow-x {{ overflow-x: auto; }}
</style>
</head>
<body>
<h1>Budget vs Actual Revenue Report</h1>
<p style="color:#666">Generated {pd.Timestamp.now().strftime('%d %b %Y %H:%M')} &nbsp;|&nbsp; 2025 full year &amp; 2026 YTD</p>

{chart_html}

<h2>Monthly Summary — All Markets</h2>
<table class="data-table summary-table">
  <thead>
    <tr>
      <th>Month</th><th>Budget</th><th>Actual</th><th>Variance</th><th>Attainment</th>
    </tr>
  </thead>
  <tbody>{monthly_summary_html()}</tbody>
</table>

<h2>By Salesperson &amp; Region — Monthly Detail</h2>
<div class="tab-bar">
  <button class="tab-btn active" onclick="showTab('y2026')">2026 YTD</button>
  <button class="tab-btn" onclick="showTab('y2025')">2025 Full Year</button>
</div>
<div id="y2026" class="tab-panel active overflow-x">
  {slp_monthly_html(2026)}
</div>
<div id="y2025" class="tab-panel overflow-x">
  {slp_monthly_html(2025)}
</div>

<script>
function showTab(id) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""

os.makedirs("data/outputs", exist_ok=True)
out_path = "data/outputs/budget_vs_actual.html"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html)
print(f"Saved: {out_path}")

# Quick summary print
print("\n=== 2026 YTD Summary ===")
s26 = total_summary[total_summary["year"] == 2026]
print(f"Actual:  €{s26['actual'].sum():,.0f}")
print(f"Budget:  €{s26['budget'].sum():,.0f}")
print(f"Attainment: {s26['actual'].sum() / s26['budget'].sum() * 100:.1f}%")

print("\n=== 2025 Full Year ===")
s25 = total_summary[total_summary["year"] == 2025]
print(f"Actual:  €{s25['actual'].sum():,.0f}")
print(f"Budget:  €{s25['budget'].sum():,.0f}")
print(f"Attainment: {s25['actual'].sum() / s25['budget'].sum() * 100:.1f}%")
