# NeoBank Dashboard — Export & Import Guide

This repository contains the complete, validated definition of the **NeoBank_Dashboard** (Databricks AI/BI dashboard) as a workspace file:

```text
dashboards/NeoBank_Dashboard.lvdash.json   (75,790 bytes, sha256 a10973b69d47531d…)
```

`.lvdash.json` is the **native Lakeview dashboard file format** used by Databricks AI/BI — the same format the dashboard editor reads and writes, so this export preserves everything: 4 datasets, 6 pages, 36 widgets (counters, bar/line/pie charts, tables, text), all filters, measures, and layout.

**Export verification (17 Nov 2026):** the file was copied from the live workspace dashboard and verified byte-identical (sha256 match). Secret scan: **clean** — no passwords, tokens, connection strings, hostnames, or email addresses are embedded. The only data references are `banking.gold.customer_360` and `banking.gold.transaction_channel_summary`.

---

## 1. What the Dashboard Contains

| Page | Widgets | Source dataset |
| --- | --- | --- |
| Executive Dashboard | 4 KPI counters (Total Customers, Total Deposits, Total Transactions, High-Risk Customers) + 2 monthly trend lines + scope note | `customers_mv`, `channel_mv` |
| Customer Insights | Segment & risk pies, balance & transaction distributions, per-risk averages, Top-10 table | `customers_mv`, `top_customers_mv` |
| Branch Performance | Deposits / Customers / Transactions / Avg-balance by branch + summary table | `branch_perf_mv` (derived from `customer_360`) |
| Channel Performance | Gateway volume share, success/failure rates, device mix, processing times | `channel_mv` |
| Transaction & Risk Analysis | Monthly failure rate, device failure analysis, risk-grade transactions, high-risk by branch | `channel_mv`, `customers_mv`, `branch_perf_mv` |
| Global Filters | Risk Grade, Customer Segment, Payment Gateway, Branch | all datasets |

**Key measures (as defined in the export):** `success_rate = Σ successful / Σ total_transactions` → 90.21% (PENDING outcomes included in the denominator); `unclassified_transactions = Σ (total − successful − failed)` → 379; `avg_processing_time_ms` is volume-weighted.

> Note: the dashboard intentionally derives branch metrics from `gold.customer_360` rather than `gold.branch_performance` (a dataset comment references a historical duplication issue that has since been fixed — `gold.branch_performance` is now verified correct; both produce identical numbers today). `gold.daily_bank_kpi` and `gold.risk_customer_summary` are not used by the dashboard.

---

## 2. Prerequisites Before Importing

The target workspace needs, before the dashboard will render data:

1. **Unity Catalog:** a catalog named `banking` with a `gold` schema.
2. **Populated Gold tables:** `banking.gold.customer_360` and `banking.gold.transaction_channel_summary` must exist and contain data (they are created by the pipeline in this repository — see README §13).
3. **A SQL warehouse / serverless compute** available to the user opening the dashboard (queries run live when the dashboard loads).
4. **No secrets required** — the definition contains none; data access follows the viewer's Unity Catalog permissions.

---

## 3. How to Import Into Another Databricks Workspace

**Option A — Git folder (recommended, keeps it version-controlled):**
1. In the target workspace, create a Git folder: **Workspace → Add → Git folder**, and clone this repository (or pull it if it already exists).
2. The file appears at `NeoBank-Data-Engineering-Project/dashboards/NeoBank_Dashboard.lvdash.json`.
3. Click the file in the workspace browser — Databricks opens it directly in the AI/BI dashboard editor, fully functional.

**Option B — Workspace file import:**
1. Download `dashboards/NeoBank_Dashboard.lvdash.json` from the repository to your machine.
2. In the target workspace: **Workspace → (folder of your choice) → ⋮ / right-click → Import** (or drag the file into the workspace browser) and select the file.
3. Open the imported file — it renders as a dashboard, not as plain text.

**Option C — REST API / SDK (automation):** upload the file to the workspace via the Workspace API (`/api/2.0/workspace/import` or the SDK `w.workspace.upload` with format `AUTO`), then open it — identical to how this export was produced in reverse.

After import, click **Edit** and verify each page renders (queries execute against your `banking.gold.*` tables).

---

## 4. What Needs Recreating in the New Workspace

* The **catalog and Gold tables** (run the pipeline in this repo, or restore table copies).
* Nothing else: filters, parameters, layout, and visualizations are all contained in the `.lvdash.json`.
* Optionally: **scheduled refresh / subscriptions** (the export carries no schedule — refresh happens on dashboard load, and the master job's dashboard task/subscriptions were configured in the source workspace only).

---

*Exported and validated 17 Nov 2026. See `docs/VALIDATION_REPORT.md` for the full dashboard validation evidence.*