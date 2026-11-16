# NeoBank — Banking Data Engineering Pipeline (Databricks)

End-to-end, **metadata-driven** banking ELT pipeline on Databricks: Neon PostgreSQL (JDBC) and CSV sources → Bronze → Silver → Gold → email notification + dashboard, with per-run audit logging.

## Architecture

```text
Neon PostgreSQL (JDBC) ──┐
                          ├──> BRONZE (raw, append-only Delta) ──> SILVER (deduped, MERGE/APPEND/FULL) ──> GOLD (5 analytics marts)
Blob CSVs (Auto Loader) ──┘                                                                           │
                                                                                                       ├──> Audit summary email (Gmail SMTP)
                                                                                                       └──> Executive dashboard (6 pages)

<catalog>.metadata (tables / table_parameters / table_watermarks / pipeline_runs) drives every layer
```

**Layers**

* **Bronze** — raw landing zone, append-only. JDBC sources are read incrementally by watermark; CSV sources are ingested with Auto Loader (`cloudFiles`) using `trigger(availableNow=True)`.
* **Silver** — deduplication *before* audit stamping (`row_number()` over the primary key), then load per metadata config: `MERGE` (upsert with freshness guard), `APPEND` (strict `>` watermark), or `FULL` (overwrite). The watermark is updated only after a successful load.
* **Gold** — 5 analytics marts: `customer_360`, `branch_performance`, `transaction_channel_summary`, `daily_bank_kpi`, `risk_customer_summary`.
* **Metadata** — the control plane: table registry, load parameters (load_type / primary_key / watermark_column), incremental watermarks, and the `pipeline_runs` audit table.

## Repository structure

```text
├── README.md                 # this file
├── .gitignore
├── config/
│   ├── config.example.json   # placeholders — the only config committed
│   ├── dev.json              # local only (gitignored)
│   └── prod.json             # local only (gitignored)
├── docs/
│   └── EXCLUSIONS.md         # what is NOT committed and why
├── notebooks/
│   ├── admin/                # 00_Secrets_Admin — one-time, manual, never run by jobs
│   ├── setup/                # 01_Setup_Metadata — first-time catalog/metadata creation
│   ├── ingestion/            # 01_Read_Tables_List, 02_Read_Table_Parameters,
│   │                        # 03_Source_to_Bronze, 04_Bronze_to_Silver
│   ├── gold/                 # 01_Silver_to_Gold_Driver + gold_transformation/ (5 notebooks)
│   └── notify/               # 01_Send_Email
├── sql/
│   └── source_ddl/          # 01_Create_Tables.sql (source database schema)
└── dashboards/               # (empty) — dashboard export PENDING, see docs/EXCLUSIONS.md
```

## Secrets — set up FIRST

`notebooks/admin/00_Secrets_Admin` creates the secret scope `banking-scope` and stores three secrets interactively (`getpass`, never printed, never in code):

* `neon-username`, `neon-password` — Neon PostgreSQL login
* `gmail_app_password` — Gmail app password for the notification email

Run it manually as an administrator, once per workspace. No job references it. All pipeline notebooks read secrets at runtime with `dbutils.secrets.get`.

## Fresh-environment setup

1. Run `notebooks/admin/00_Secrets_Admin` (interactive, one-time).
2. Run `notebooks/setup/01_Setup_Metadata` — ⚠️ **WARNING:** its first SQL cell DROPs the four metadata tables. First-time setup only; re-running wipes watermarks and audit history.
3. Create the source tables on your database. Note: `sql/source_ddl/01_Create_Tables.sql` is a **reference DDL written in SQL Server / T-SQL syntax** (`sys.schemas`, `EXEC`, `GO` batch separators, `DATETIME2`, `SYSDATETIME()`) — it is **not directly executable on Neon PostgreSQL**. Its column names, order, and types match the live Neon source exactly; when applying to PostgreSQL, translate the syntax (e.g. `DATETIME2` → `TIMESTAMP`, `SYSDATETIME()` → `NOW()`, `DECIMAL(18,2)` → `NUMERIC(18,2)`, `CREATE SCHEMA banking;` directly without the `sys.schemas` guard).
4. Place CSV sources (credit bureau reports, payment gateway logs) under the source volume paths registered in `metadata.tables.source_path`.
5. Wire jobs — see "Job wiring (PENDING)" below.

## Required parameters (all fail fast if missing)

| Notebook | Required widgets | Supplied by |
| --- | --- | --- |
| 03_Source_to_Bronze | `run_id` (numeric), `table_metadata`, `table_parameters`, `catalog_name`, `neon_host`, `neon_port`, `neon_database`, `volume_base` (blob loads) | Inner job / manual run |
| 04_Bronze_to_Silver | `run_id` (numeric), `table_metadata`, `table_parameters`, `catalog_name` | Inner job / manual run |
| 01_Read_Tables_List | `source_system`, `catalog_name` | Source_to_Silver job / manual run |
| 02_Read_Table_Parameters | `table_metadata`, `catalog_name` | Inner job / manual run |
| 01_Silver_to_Gold_Driver | `run_id` (numeric), `table_metadata`, `catalog_name` | Silver_to_Gold job / manual run |
| 01_Send_Email | `run_id`, `sender_email`, `recipient_email`, `catalog_name` | Master job Email_Notification task / manual run |

All `run_id` widgets default to an **empty string** and validate numeric (the audit table stores BIGINT). Jobs inject real values; manual runs must supply a unique numeric run ID.

## Loading strategies (unchanged from production)

| Table | load_type | primary key | watermark column |
| --- | --- | --- | --- |
| customers | MERGE | customer_id | updated_at |
| accounts | MERGE | account_id | updated_at |
| transactions | APPEND | txn_id | txn_timestamp |
| branches | FULL | branch_code | — (no watermark row) |
| credit_bureau_reports | MERGE | customer_id | bureau_pull_date |
| payment_gateway_logs | APPEND | txn_id | processed_timestamp |

## Non-secret configuration

**Only `config/config.example.json` is committed.** It contains placeholders only.

To run the pipeline in your environment:

1. Copy `config/config.example.json` to `config/dev.json` or `config/prod.json` (both are gitignored — they never leave your machine).
2. Fill in your local, non-secret settings: `catalog_name`, `volume_base`, Neon `host`/`port`/`database`, secret scope name, and notification emails.
3. Pass those values to jobs/notebooks as parameters (see "Required parameters" and "Job wiring (PENDING)").

**Credentials are never in config files** — only in the `banking-scope` Databricks secret scope.

## Job wiring (PENDING — do NOT apply without approval)

The production jobs were not modified. To run these copies from jobs, the following job-parameter additions are required (marked PENDING):

* `NeoBank_Source_to_Silver` + `NeoBank_Source_to_Silver_Inner`: add `catalog_name`, `neon_host`, `neon_port`, `neon_database`, `volume_base`.
* `NeoBank_Master_Job` → Email_Notification task: add `sender_email`, `recipient_email`.
* Change `run_id` job defaults (`5678`, `8901`) to empty strings.
* Fix the legacy `source_system='sqlserver'` label (actual source: Neon PostgreSQL) — must change atomically in `metadata.tables`, the master job parameter, and `03_Source_to_Bronze`'s branch condition.

Long term, capture the 4 jobs as code with a Declarative Automation Bundle (DAB) with `dev`/`prod` targets.

## Known issues / remaining environment-specific references

* `source_system='sqlserver'` in `metadata.tables` and the ingestion branch is a **legacy label** — the actual JDBC source is Neon PostgreSQL. Rename PENDING (coordinated change).
* Gold transformation notebooks hardcode the catalog name `banking` (kept intentionally: the gold driver calls them by relative path via `dbutils.notebook.run`, which passes no arguments).
* The master job's Silver_to_Gold task uses `run_if: ALL_DONE` (gold rebuilds even if ingestion failed) — production hardening item, unchanged here.
* Source extraction filters with strict `>` — late-arriving source updates (watermark ≤ stored) are not ingested; backfill requires a manual watermark reset.
* Bronze is append-only with no retention policy.

## Security

* No credentials, tokens, or personal email addresses in any committed file.
* Secrets only in the Databricks secret scope `banking-scope`.
* Data files (CSVs, bulk seed scripts) excluded via `.gitignore` (see `docs/EXCLUSIONS.md`).
* Keep this repository **private**.
