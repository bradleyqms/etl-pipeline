"""
Sankey diagram of net revenue YTD 2026
Flow: Company Group → Market Group → Region → Sub Region (where set) → Channel
"""
import io
import pandas as pd
import plotly.graph_objects as go
from src.transforms.build_gold import GOLD_PATHS, get_container_client

client = get_container_client()

def rp(p):
    return pd.read_parquet(io.BytesIO(client.get_blob_client(p).download_blob().readall()))

fact  = rp(GOLD_PATHS["fact_sales"])
dim_c = rp(GOLD_PATHS["dim_customer"])

# ── YTD 2026 ──────────────────────────────────────────────────────────────────
ytd = fact[fact["doc_date"].dt.year == 2026].copy()

hier = dim_c[["customer_key", "company_group", "market_group", "region", "sub_region", "channel"]].drop_duplicates("customer_key")
ytd = ytd.merge(hier, on="customer_key", how="left")

for col in ["company_group", "market_group", "region", "channel"]:
    ytd[col] = ytd[col].fillna("Unknown")
# sub_region stays NaN where not set — used to decide routing
ytd["sub_region"] = ytd["sub_region"].where(ytd["sub_region"].notna() & ytd["sub_region"].astype(str).str.strip().ne(""), None)

# ── Build a virtual "region|sub_region" node for rows that have a sub_region ──
# This lets us route: Region → SubRegion → Channel vs Region → Channel directly
ytd["_rg_node"]   = ytd["region"]
ytd["_srg_node"]  = ytd.apply(
    lambda r: f"{r['region']} › {r['sub_region']}" if pd.notna(r["sub_region"]) else None, axis=1
)

# ── Aggregate all needed link pairs ───────────────────────────────────────────
def agg(*cols):
    return ytd.groupby(list(cols), dropna=False)["revenue_eur"].sum().reset_index()

rev_cg_mg   = agg("company_group", "market_group")
rev_mg_rg   = agg("market_group", "region")
# Region → Sub Region (only rows that have a sub_region)
rev_rg_srg  = (
    ytd[ytd["_srg_node"].notna()]
    .groupby(["_rg_node", "_srg_node"], dropna=False)["revenue_eur"].sum().reset_index()
    .rename(columns={"_rg_node": "region", "_srg_node": "sub_region_node"})
)
# Sub Region → Channel (rows with sub_region)
rev_srg_ch  = (
    ytd[ytd["_srg_node"].notna()]
    .groupby(["_srg_node", "channel"], dropna=False)["revenue_eur"].sum().reset_index()
    .rename(columns={"_srg_node": "sub_region_node"})
)
# Region → Channel (rows WITHOUT sub_region — direct link)
rev_rg_ch   = (
    ytd[ytd["_srg_node"].isna()]
    .groupby(["region", "channel"], dropna=False)["revenue_eur"].sum().reset_index()
)

# ── Build node list ────────────────────────────────────────────────────────────
cg_nodes   = sorted(rev_cg_mg["company_group"].unique())
mg_nodes   = sorted(rev_cg_mg["market_group"].unique())
rg_nodes   = sorted(rev_mg_rg["region"].unique())
srg_nodes  = sorted(rev_rg_srg["sub_region_node"].unique()) if not rev_rg_srg.empty else []
ch_nodes   = sorted(
    set(rev_rg_ch["channel"].unique()) | set(rev_srg_ch["channel"].unique())
)

all_nodes = (
    [f"[CG] {n}" for n in cg_nodes] +
    [f"[MG] {n}" for n in mg_nodes] +
    [f"[RG] {n}" for n in rg_nodes] +
    [f"[SR] {n}" for n in srg_nodes] +
    [f"[CH] {n}" for n in ch_nodes]
)
node_idx = {n: i for i, n in enumerate(all_nodes)}

# ── Links ─────────────────────────────────────────────────────────────────────
sources, targets, values = [], [], []

def add_links(df, src_col, src_prefix, tgt_col, tgt_prefix):
    for _, row in df.iterrows():
        v = row["revenue_eur"]
        if v <= 0:
            continue
        s = node_idx.get(f"{src_prefix} {row[src_col]}")
        t = node_idx.get(f"{tgt_prefix} {row[tgt_col]}")
        if s is None or t is None:
            continue
        sources.append(s); targets.append(t); values.append(v)

add_links(rev_cg_mg,  "company_group",    "[CG]", "market_group",     "[MG]")
add_links(rev_mg_rg,  "market_group",     "[MG]", "region",           "[RG]")
add_links(rev_rg_srg, "region",           "[RG]", "sub_region_node",  "[SR]")
add_links(rev_srg_ch, "sub_region_node",  "[SR]", "channel",          "[CH]")
add_links(rev_rg_ch,  "region",           "[RG]", "channel",          "[CH]")

# ── Colours ───────────────────────────────────────────────────────────────────
PALETTE = {
    "Company 1":    "#1f77b4",
    "Company 2":    "#ff7f0e",
    "Company 3":    "#2ca02c",
    "Core Markets": "#6baed6",
    "UK":           "#fdae6b",
    "USA":          "#74c476",
    "Export":       "#d62728",
    "eCommerce":    "#9467bd",
    "Spa":          "#17becf",
    "Retail":       "#bcbd22",
    "Distributor":  "#e377c2",
    "Interco":      "#8c564b",
    "Unknown":      "#c7c7c7",
}

def hex_to_rgba(hex_color, alpha=0.6):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def node_color(label):
    raw = label.split("] ", 1)[-1]
    # sub_region nodes are "Region › Sub" — use parent region colour, softened
    if " › " in raw:
        parent = raw.split(" › ")[0]
        base = PALETTE.get(parent, "#aaaaaa")
        return hex_to_rgba(base, 0.65)
    return PALETTE.get(raw, "#c7c7c7")

node_colors = [node_color(n) for n in all_nodes]
display_labels = [n.split("] ", 1)[-1] for n in all_nodes]

total = ytd["revenue_eur"].sum()

fig = go.Figure(go.Sankey(
    arrangement="snap",
    node=dict(
        pad=16,
        thickness=22,
        line=dict(color="white", width=0.5),
        label=display_labels,
        color=node_colors,
        hovertemplate="%{label}<br>Net Revenue: €%{value:,.0f}<extra></extra>",
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        hovertemplate="€%{value:,.0f}<extra></extra>",
        color="rgba(180,180,180,0.30)",
    ),
))

fig.update_layout(
    title=dict(
        text=(
            f"<b>QMS Net Revenue YTD 2026</b>  —  Total: €{total:,.0f}"
            "  |  Company Group → Market Group → Region → Sub Region → Channel"
        ),
        font=dict(size=14),
    ),
    font=dict(size=11),
    height=920,
    paper_bgcolor="white",
)

out_path = "data/outputs/sankey_revenue_ytd_2026.html"
import os; os.makedirs("data/outputs", exist_ok=True)
fig.write_html(out_path, include_plotlyjs="cdn")
print(f"Saved: {out_path}  |  Total YTD €{total:,.0f}")
