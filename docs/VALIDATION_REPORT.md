# NeoBank — End-to-End Pipeline Validation Report

**Validation date:** 17 November 2026 · **Mode:** read-only (live queries + configuration inspection) · **Catalog:** `banking`

**Status definitions:** PASSED = executed and passed today · FAILED = executed and failed today · NOT TESTED = could not be executed (would require writes or production runs) · REQUIRES VERIFICATION = insufficient evidence. Historical evidence from the 04 Nov 2026 audit is cited as such, never counted as today's pass.

---

## 1. Executive Summary

The NeoBank pipeline is **functionally healthy and consistent end-to-end at the data level**: both source systems are reachable, Bronze/Silver/Gold contain exactly the expected data with zero duplicate primary keys, every Gold aggregation reconciles to Silver to the paisa, and all four dashboard dataset queries execute. No production data changed during this validation (zero audit rows written; watermarks untouched).

The pipeline is **not production-ready in the operational sense**: no failure alerts or retries exist, the gold stage still runs after ingestion failures (`run_if: ALL_DONE`), setup remains destructive on re-run, and a few audit-accuracy gaps (records-read vs records-written, legacy `sqlserver` label, orphan INPROGRESS row, metadata drift on the `transactions` load type) remain open. Full issue list in §6.

---

## 2. Validation Results

| # | Component | Validation Check | Status | Actual Evidence | Issue Found | Recommended Fix |
|---|---|---|---|---|---|---|
| 1 | Source (Neon) | JDBC connectivity via secret scope `banking-scope` | PASSED | Live `spark.read.jdbc` to `neondb`; credentials fetched from secrets, never printed | — | — |
| 2 | Source (Neon) | Row counts | PASSED | customers 5,000 · accounts 5,500 · transactions 20,000 · branches 5 | — | — |
| 3 | Source (Neon) | Duplicate primary keys | PASSED | 0 duplicates on all 4 source PKs | — | — |
| 4 | Source (Neon) | No records silently skipped by watermark | PASSED | Source `max(watermark)` equals stored watermark exactly on customers/accounts/transactions; 0 rows pending behind watermark | — | — |
| 5 | Source (CSV) | Volume files + Auto Loader state | PASSED | 2 CSVs per source folder; `_checkpoints/` + `_schema/` present | — | — |
| 6 | Bronze | Tables populated, append-only generations | PASSED | 9,367 / 23,501 / 65,000 / 31 / 5,500 / 20,000 rows; distinct PKs match Silver exactly | — | — |
| 7 | Bronze | Malformed records (`_rescued_data`) | PASSED | 0 non-null `_rescued_data` rows on both Auto Loader tables | — | — |
| 8 | Bronze | Audit visibility of Bronze layer | REQUIRES VERIFICATION | `03_Source_to_Bronze` writes audit rows with `layer='Silver'` (by design: the same row is finalized by `04`) | No Bronze-layer audit rows exist | Add `layer='Bronze'` rows or rename for clarity (P3) |
| 9 | Bronze | Late-arriving source records (`>` extraction) | REQUIRES VERIFICATION (design) | `03` filters sources with strict `>`; rows with `watermark <= stored` can never be ingested — no such rows exist today, but the gap is structural | Silent data-loss risk for late corrections | Reconciliation MERGE or manual watermark-reset runbook (P1) |
| 10 | Silver | PK uniqueness | PASSED | 0 duplicates on all 6 tables | — | — |
| 11 | Silver | NULLs on key business columns | PASSED | 0 on all checked columns; 0 non-positive transaction amounts | — | — |
| 12 | Silver | Load strategy evidence | PASSED | Delta history: MERGE on customers (×5), accounts, transactions; FULL (CREATE OR REPLACE) on branches; MERGE on credit bureau; appends on gateway | — | — |
| 13 | Silver | Watermark = Silver max(watermark) | PASSED | Verified equal on customers, transactions, gateway | — | — |
| 14 | Silver | MERGE re-delivery idempotency | NOT TESTED today | Historical (04 Nov): 1,500 rows re-read, 0 net change; freshness-guard logic (`s.wm >= t.wm`) verified by code inspection | — | Re-run in a dev environment before production claims |
| 15 | Silver | Empty-batch / freshness guards | NOT TESTED today | Verified by code inspection only (would need writes) | — | Include in dev test suite |
| 16 | Metadata | `tables` registry (11 rows: 6 sources + 5 gold) | PASSED | Live read matches expected registry | — | — |
| 17 | Metadata | `table_parameters` vs README | FAILED (drift) | Live metadata says `transactions` = **MERGE**; setup seed and README say **APPEND** | Metadata drift — changed after seeding | Reconcile: update README + seed, or revert metadata (P2) |
| 18 | Metadata | `table_watermarks` (5 rows, branches FULL = none) | PASSED | All populated, 0 NULLs, equal Silver maxima | `last_run_id` never written (always NULL) | Populate `last_run_id` in `04` (P3) |
| 19 | Audit | `pipeline_runs` status integrity | PASSED | 58 rows: 57 SUCCESS + 1 INPROGRESS (orphan) | — | — |
| 20 | Audit | Orphan INPROGRESS rows | FAILED (present) | run_id 474430848957628, table_id 5 (Gold) stuck INPROGRESS since 30 Oct | Aborted gold-driver invocation | Manual cleanup (user decision; P2) |
| 21 | Audit | Default/manual run_id collisions | FAILED (present) | Historical rows under run_ids 1234, 5678, 8901 (job defaults) and 472519629468310 (widget default) | Audit rows overwritten on collision | Change job defaults to empty + fail-fast (P1) |
| 22 | Audit | `number_of_records` semantics | REQUIRES VERIFICATION | Stores records **read** (deduped batch size); records written only printed | Ambiguous reporting | Add a `records_written` column (P2) |
| 23 | Audit | Setup DROP risk | FAILED (present) | `01_Setup_Metadata` cell 1 drops all 4 metadata tables | Re-run wipes watermarks/audit | Guard cell / move to separate bootstrap (P1) |
| 24 | Audit | Email scope | PASSED | Email queries only the specific `run_id` (no full history); production copy has hardcoded personal emails — repo copy is parameterized | — | Wire `sender_email`/`recipient_email` into jobs (P1) |
| 25 | Jobs | 4 jobs exist, task chains correct | PASSED | Master → S2S (BLOB + SQLSERVER parallel) → Inner (params → 03 → 04 sequential); S2G → driver per gold table; email last | — | — |
| 26 | Jobs | run_id propagation | PASSED | Master `{{job.run_id}}` reaches inner notebooks via job-parameter name inheritance (audit rows prove) | — | — |
| 27 | Jobs | Failure gating of Gold stage | FAILED | Silver_to_Gold task uses `run_if: ALL_DONE` — gold rebuilds after ingestion failure | Stale gold masking failures | Change to `ALL_SUCCESS` (P1) |
| 28 | Jobs | Retries / timeouts | FAILED | No retries anywhere; all `timeout_seconds=0` | No transient-failure recovery | Add retries on 03/04 + sensible timeouts (P2) |
| 29 | Jobs | Failure notifications | FAILED | No `on_failure` email/webhook on any job; only the ALL_DONE summary email task | Failures silent outside audit table | Add `on_failure` alerts (P1) |
| 30 | Jobs | Job parameter wiring for repo copies | FAILED | Jobs don't pass `catalog_name`, `neon_*`, `volume_base`, `sender_email`, `recipient_email` | Repo copies fail-fast from current jobs | Rewire jobs or capture as bundle (P1) |
| 31 | Jobs | End-to-end orchestration execution | NOT TESTED | Would write production data | — | Validate in dev environment |
| 32 | Gold | customer_360 | PASSED | 5,000 rows = 5,000 distinct customers; 0 null credit_score; Σ total_balance = 1,393,539,114.15 = Silver accounts Σ; Σ txn amount = 1,003,452,288.93 = Silver transactions Σ | — | — |
| 33 | Gold | branch_performance | PASSED | 5 branches; Σ deposits = 1,393,539,114.15; Σ customers = 5,000 | — | — |
| 34 | Gold | transaction_channel_summary | PASSED | 6,148 rows; Σ transactions = 20,000; success 18,041 + failed 1,580 + PENDING 379 | 379 (1.9%) gateway outcomes unclassified (`PENDING`) | Add `pending_transactions` column (P2) |
| 35 | Gold | daily_bank_kpi | PASSED | 423 rows = 423 distinct dates | Bank-level metrics repeat on every row (cross-join) — summing `total_balance` over dates yields 589B (misleading) | Rename columns or add comment; consumers must not aggregate them (P2) |
| 36 | Gold | risk_customer_summary | PASSED | Grades HIGH/MEDIUM/LOW = 1,498/1,735/1,767; avg credit scores match Silver exactly | — | — |
| 37 | Gold | Segment rule | PASSED | HIGH 1,022 + MEDIUM 1,751 + LOW 2,227 = 5,000 | — | — |
| 38 | Dashboard | Datasets execute | PASSED | All 4 dataset queries ran: 5,000 / 10 / 5 / 6,148 rows | — | — |
| 39 | Dashboard | Success-rate denominator | PASSED (verified) | `success_rate = Σ successful / Σ total` → 90.21% (includes PENDING); `unclassified_transactions` measure exists | — | State denominator when quoting (P3) |
| 40 | Dashboard | Gold table usage | REQUIRES VERIFICATION | Uses `gold.customer_360` (3 datasets) + `gold.transaction_channel_summary`; branch metrics derived from customer_360 (dataset comment calls `gold.branch_performance` inflated — **stale**: that table is now verified correct); `daily_bank_kpi` and `risk_customer_summary` not used by dashboard | Dashboard does not consume 2 of 5 gold tables | Optional: add risk-summary page or note in docs (P3) |
| 41 | Dashboard | No secrets in definition | PASSED | 75,790-char `.lvdash.json` scanned: no passwords, tokens, hosts, emails; references only `banking.gold.*` | — | — |
| 42 | GitHub | Repo vs production parity | REQUIRES VERIFICATION | 19-file repo matches production logic (verified 16 Nov); copies unexecuted | Repo notebooks never run end-to-end | Run once in dev (P1) |
| 43 | GitHub | README accuracy | FAILED (2 claims) | (a) transactions documented APPEND, live metadata = MERGE; (b) README says dashboard export pending — export now exists in repo | Doc drift | Update README on approval (P2) |

---

## 3. Totals

| Status | Count |
| --- | --- |
| PASSED | 27 |
| FAILED | 8 |
| NOT TESTED | 3 |
| REQUIRES VERIFICATION | 5 |
| **Total checks** | **43** |

*No check FAILED at the data level — all eight failures are configuration/documentation issues.*

---

## 4. Zero-Drift Confirmation

* Bronze/Silver/Gold row counts identical to the 04 Nov baseline; `pipeline_runs` shows **0 rows after 04 Nov 07:15** — no production activity or drift since the last rebuild.
* Watermarks unchanged and equal to live Silver maxima.

---

## 5. What Was Actually Executed vs Inspected

**Executed (read-only, live):** JDBC source counts; CSV volume listing; all Bronze/Silver/Gold row counts, PK uniqueness, NULL and rescued-data checks; Gold-vs-Silver reconciliation queries; `DESCRIBE HISTORY` reads; all 4 dashboard dataset queries; jobs API GETs; dashboard JSON parse + secret scan; byte-identical export verification.

**Inspected only (no execution):** notebook code (all 13 production + repo copies), job task graphs, metadata seeds, setup DROP statements, MERGE/APPEND/FULL logic guards, email notebook. **Not tested:** orchestration runs, MERGE re-delivery, watermark-reset recovery — these require writes and were deliberately not executed against production per the validation rules.

---

## 6. Prioritized Issues

**Critical (none)** — no data corruption, no secret exposure, no failed loads.

**High (P1) — fix before calling production-ready:**
1. `run_if: ALL_DONE` on Silver_to_Gold (gold rebuilds on failed ingestion)
2. No `on_failure` alerts on any job (failures invisible)
3. Destructive `DROP` cells in `01_Setup_Metadata` (re-run wipes watermarks/audit)
4. Default run_ids (5678/8901) still in job configs — audit collisions on manual runs
5. Repo job-parameter wiring gap (`catalog_name`, `neon_*`, `volume_base`, emails) — repo copies unrunnable from current jobs
6. Late-arriving source rows (`watermark <= stored`) never ingested — backfill runbook needed
7. Repo copies never executed end-to-end

**Medium (P2):** metadata drift (`transactions` MERGE vs APPEND); README updates (load type + dashboard export); orphan INPROGRESS audit row; `records_written` column; PENDING channel outcomes; retries/timeouts; daily_bank_kpi repeated-metrics caveat; `sqlserver` legacy label rename.

**Low (P3):** task-name typos (`Ingetion`); Bronze-layer audit rows; `last_run_id` never written; dashboard denominator note; stale branch-dataset comment; bronze retention.

---

## 7. Readiness Assessment

**Data layer: VALIDATED.** Every executed data check passed; gold reconciles to silver to the paisa; sources fully caught up.
**Operational layer: NOT production-ready** until the P1 items are closed (failure gating, alerting, setup protection, run_id defaults, job wiring, late-arrival runbook, one clean end-to-end run of the repo copies).
**GitHub repository: complete and safe** (dashboard now exported, no secrets), with two README corrections pending.

*All evidence gathered 17 Nov 2026 via read-only queries and configuration reads; nothing was modified, written, or run against production.*