# Unified Agenda (UA) Compiler — Why‑First README

> **Modules covered (exact filenames):**
>
> - `UA_Parser_For_Single_xml_9_12_25.py`  *(single‑XML parser)*
>   Path: `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/src/UA_COMPILER/UA_Parser_For_Single_xml_9_12_25.py`
> - `Runs_UA_Compiler.py`  *(directory runner & dataset builder)*
>   Path: `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/src/Runner/Runs_UA_Compiler.py`
> - `run_ua_all_xmls_TEST_2019.py`  *(a.k.a. `Checking_with_2019_chiou_klingler.py` if you renamed it)*
>   Path: `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/Federal_Register_Compiler/tests/run_ua_all_xmls_TEST_2019.py`
>
> **Reference trees (schema guides):**
> - `ua_tree_fall_1995_ascii_CORRECTED.pdf`
> - `ua_tree_201810_ascii.pdf`

---

## What is the Unified Agenda (UA)?  *Why do this at all?*

The UA is a **biannual** (Spring/Fall) publication that lists regulatory actions across the U.S. Federal Government.
Each entry is keyed by a **Regulation Identifier Number (RIN)** and includes fields like **agency**, **rule stage**, **priority**, and a **timetable** of planned actions. The XML files are the **authoritative record**, but they are:
- **Heterogeneous** across time (1995–present schemas differ),
- **Verbose** and **nested** (lists, contact blocks, timetables),
- Sometimes **quirky** (namespaces, HTML blobs, partial dates).

**Goal:** Turn those XMLs into **reliable, analysis‑ready CSVs** without losing information, across all vintages, and then produce a **one‑row‑per‑RIN** view that pins each RIN to its **last appearance** (CK‑style).

---

## Why the project is built this way

### 1) lxml + streaming parse + namespace‑agnostic walking
- **Why:** Older XMLs have quirks; we need to **survive** imperfect markup and **stream** large files.
- **How:** `lxml.etree.iterparse(recover=True)` (falls back to stdlib if missing). We avoid brittle XPath predicates and compare **local tag names** instead.

### 2) Stable **superset schema** (1995 baseline + 2018 adds)
- **Why:** Columns vary by year (e.g., `EO_13771_DESIGNATION` appears in 2018+). Downstream code should not chase a moving schema.
- **How:** The parser **always emits the superset**; missing fields for earlier years are **blank**, not missing columns.

### 3) **Dynamic union of extra top‑level scalars**
- **Why:** Some files introduce simple top‑level scalars not in the trees. We don’t want silent data loss.
- **How:** Unknown top‑level leaf nodes are added as **new columns** on the fly.

### 4) Lists kept as **JSON strings**
- **Why:** Families like `CFR_LIST`, `LEGAL_AUTHORITY_LIST`, `RELATED_RIN_LIST`, `AGENCY_CONTACT_LIST`, and `TIMETABLE_LIST` are 1‑to‑many and vary in length.
- **How:** Serialize the full list as a **JSON string** in the CSV so each row is “one RIN in one issue,” with fidelity preserved for audits/explodes.

### 5) “Latest action” is **per‑issue**, not across history
- **Why:** A common mistake is to compute the “latest” timetable across *all* issues for a RIN, which leaks future plans into older issues.
- **How:** For each issue row, parse `TTBL_DATE` to ISO (`YYYY‑MM‑DD` when possible) and choose the **max within that issue** to derive `Latest_Action` and `latest_action_date`.

### 6) Last‑per‑RIN selection + **EO_13771** backfill
- **Why:** CK‑style analysis requires one row per RIN from its **last** issue, but some last issues **omit** administrative scalars like EO‑13771.
- **How:** Choose the max `PUBLICATION_ID` (fallback to **YYYYMM from filename**). If `EO_13771_DESIGNATION` is **blank** in that last row but found in **earlier** issues for the **same RIN**, backfill from the **latest prior** non‑blank and write a small **audit log**.

---

## Modules (what each one does, and why)

### `UA_Parser_For_Single_xml_9_12_25.py` — *single‑file parser*
**Why:** A single, resilient function that can parse **any** UA XML (1995–present) and emit a flat row per (RIN, issue).
**Key behaviors:**
- lxml streaming parse; namespace‑agnostic local‑name matching.
- Emits a **superset** schema (1995 + 2018 adds) *plus* any unknown top‑level scalars found.
- List families become **JSON strings** in the CSV.
- Derives `Latest_Action` and `latest_action_date` from **that issue’s** timetable only.
- Output per file: `<XML_BASENAME>_flat.csv`.

**Public API:**
```python
csv_path, df = build_ua_csv_from_xml(xml_path: str, out_dir: str)
# returns path written and a pandas DataFrame
```

---

### `Runs_UA_Compiler.py` — *directory runner & dataset builder*
**Why:** We need one **combined** dataset across all XMLs **and** a **last‑per‑RIN** view for analysis.
**What it does:**
1. Loads the parser (file‑path import so it always uses your exact file).
2. Scans the UA XML folder and parses every file → **combined** DataFrame.
3. Adds `source_xml` and writes `ua_all_flat.csv` (keeps **every** column observed).
4. Creates `ua_all_counts_by_file.csv` – quick sanity counts.
5. Builds **last‑per‑RIN**:
   - Determine latest `PUBLICATION_ID` (fallback: YYYYMM from filename).
   - Keep the **row from that last issue**.
   - **EO_13771_DESIGNATION** backfill if blank (from latest prior non‑blank for that RIN).
   - Write `ua_all_last_per_rin.csv` and `ua_all_last_per_rin_eo13771_backfill_log.csv`.

**Why this separation:** parsing keeps rows **faithful to their issue**; last‑per‑RIN is a **pure selection** step, not a merge of timetables or fields across issues.

---

### `run_ua_all_xmls_TEST_2019.py` *(a.k.a. `Checking_with_2019_chiou_klingler.py`)* — throwaway count check
**Why:** Reproduce the legacy **CK window** fast and without side effects.
**What it does:**
- Reads only files with `YYYYMM ≤ 201912`.
- Uses a `TemporaryDirectory`; **no outputs are saved**.
- Computes last‑per‑RIN count entirely in memory and **prints a single integer**.

Use this to verify that your distinct RIN count (≤2019) matches prior work (≈ **39,311**).

---

## Paths you may need to change

- **UA XML directory:**
  `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data`
- **Output directory (runner only):**
  `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA`
- **Parser file (dynamic import in runners):**
  `/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/src/UA_COMPILER/UA_Parser_For_Single_xml_9_12_25.py`

> **Tip:** Avoid leading `"/UA_COMPILER/..."` paths — those point to filesystem root and will fail on macOS (read‑only).

---

## Quickstart

1. **Build everything**
   Run `Runs_UA_Compiler.py`. Outputs:
   - `ua_all_flat.csv`
   - `ua_all_counts_by_file.csv`
   - `ua_all_last_per_rin.csv`
   - `ua_all_last_per_rin_eo13771_backfill_log.csv`

2. **Verify CK window quickly**
   Run `run_ua_all_xmls_TEST_2019.py` (or your file named `Checking_with_2019_chiou_klingler.py`).
   It prints one integer: last‑per‑RIN row count for files ≤ 2019.

---

## Acceptance checks / sanity

- Combined rows across all years ≈ **233k–265k** (varies with coverage).
- Distinct RINs in last‑per‑RIN (≤2019) ≈ **39k**.
- EO backfill log is modest; timetable “latest” dates match the **last issue’s** entries.

---

## Common pitfalls & fixes

- **Module import errors** in runners: use **file‑path import** (`importlib.util.spec_from_file_location`) to load the parser reliably from any CWD.
- **Permission / read‑only errors:** ensure paths begin with your **Dropbox absolute path**, not `/UA_COMPILER/...`.
- **Malformed XML:** keep `lxml` installed; it recovers from minor defects and streams large files.
- **HTML in ABSTRACT:** preserved intentionally (CDATA/HTML). Clean downstream if you need plain text.

---

## Why two schema trees in docs?

We include ASCII trees for **Fall 1995** and **2018** because they bracket the main schema drift:
- 1995 captures the **baseline** fields and list structures.
- 2018 adds fields like `EO_13771_DESIGNATION` that are important for later analysis
  (e.g., deregulatory/regulatory designation).
The parser handles both automatically; the trees exist to **verify** that the CSV mirrors the XML.

---

## Contact / maintenance notes

- Prefer **adding** new fields to the superset rather than renaming/removing columns.
- If a new UA vintage introduces a genuinely new list family, keep it as **JSON**.
- If you change date parsing rules, ensure “latest” remains **per issue only**.
- In docs, we have the xml trees which are useful for understandign the structure of the data
