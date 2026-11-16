# Files Excluded from the GitHub Repository

These items exist in the workspace but are intentionally NOT part of the GitHub-ready copy. Keep them out of the repository.

## 1. Data files (bulk synthetic data)
* `/Banking_Project/00_Source_Files/02_Blob/*.csv` — 4 CSVs (credit bureau reports, payment gateway logs, incl. incremental files). Blocked by `.gitignore` (`*.csv`).
* `02_Insert_Historical_data.sql` and `03_Incrementat_data.sql` (in `00_Source_Files/01_SQL_Server/`) — bulk seed scripts containing thousands of synthetic customer records (fake emails, PAN numbers, phone numbers). They are data, not code; only `01_Create_Tables.sql` (DDL, no data) is committed. If you want the seed scripts in the repo for reproducibility, request it explicitly — they are large and full of synthetic PII.

## 2. Notebook outputs
* All copied notebooks in this repo are output-free. Never export executed notebooks: outputs contain run IDs, rendered emails, audit tables, and table data.

## 3. Credentials and secret material
* Anything in the `banking-scope` secret scope (`neon-username`, `neon-password`, `gmail_app_password`) — values live only in Databricks secrets, never in files.
* Any `.env`, token, key, or credentials file — blocked by `.gitignore`.

## 4. Documentation with run evidence
* `/Banking_Project/NeoBank_Project_Documentation.md` — 900+ lines containing detailed run/audit evidence (run IDs, timestamps, row counts, historical figures). Replace with the curated `README.md` + `docs/` in this repo.

## 5. Runtime state
* Volume directories `_checkpoints/` and `_schema/` under `/Volumes/banking/source/volume/` — Auto Loader runtime state, never commit.

## 6. Not yet exported (PENDING — needs your action)
* `NeoBank_Dashboard` (`.lvdash.json`) — export from the dashboard UI (Dashboard → NeoBank_Dashboard → Export) into `dashboards/`. It contains only gold-table references, safe to commit once exported.
* Production job definitions (4 jobs) — recommended to capture as code via a Declarative Automation Bundle (requires separate approval; see README "Job wiring (PENDING)").

## 7. The production folder itself
* `/Banking_Project/**` is the live production workspace and is NOT the repo source. Only the sanitized copies under `notebooks/` are.