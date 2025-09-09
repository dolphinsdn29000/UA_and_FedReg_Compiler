# UA_CHECKER.py
# -------------------------------------------------------------
# Verifies compiled UA CSVs are internally consistent and that
# selected values line up with their source UA XML files.
#
# It DOES NOT msodify your compiled outputs.
# It writes verification artifacts under <OUT_DIR>/_verify.
#
# Requires: pandas, lxml, python-dateutil e
# -------------------------------------------------------------

import os
import re
import json
from collections import defaultdict
from datetime import datetime
from dateutil import parser as dateparser

import pandas as pd
from lxml import etree


# =======================
# 1) PATHS (hard-coded)
# =======================
UA_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/NEW_ML_REGULATIONS_PAPER 2/Unified_Agenda_Download/ua_main_data"
OUT_DIR = os.path.join(UA_DIR, "_ck_out")

# Compiled CSVs produced by your compiler
PATH_ROWS       = os.path.join(OUT_DIR, "ua_rows.csv")
PATH_CK_LAST    = os.path.join(OUT_DIR, "ua_ck_last.csv")
PATH_TIMETABLES = os.path.join(OUT_DIR, "ua_timetables.csv")

# Where to write verification artifacts
VERIFY_DIR = os.path.join(OUT_DIR, "_verify")
os.makedirs(VERIFY_DIR, exist_ok=True)

# XML files to trace (add/remove YYYYMM as you like)
TRACE_PUB_YMS = [
    "199510",  # Fall 1995
    "200910",
    "201710",
    "202104",
    "202410",
]


# ================
# 2) UTILITIES
# ================
def pick_col(df: pd.DataFrame, candidates):
    """Return the first column name from candidates that exists in df, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def as_str_series(s: pd.Series) -> pd.Series:
    """Convert any series to string with safeties."""
    return s.astype("string").fillna(pd.NA)


def to_date_safe(x):
    """Parse date strings robustly; return pd.NaT on failure."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    # UA often has placeholders like '10/00/1995' or 'To Be Determined'
    txt = str(x).strip()
    if not txt or txt.lower().startswith("to be"):
        return pd.NaT
    # Normalize 'MM/00/YYYY' => 'MM/15/YYYY' to get an approximate month anchor
    m = re.match(r"^(\d{2})/00/(\d{4})$", txt)
    if m:
        txt = f"{m.group(1)}/15/{m.group(2)}"
    try:
        return pd.to_datetime(dateparser.parse(txt, fuzzy=True))
    except Exception:
        return pd.NaT


def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Expected file not found: {path}")
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")


def publication_id_from_filename(xml_path: str) -> str:
    """
    Extract 'YYYYMM' from a filename like REGINFO_RIN_DATA_199510.xml -> '199510'
    """
    m = re.search(r"REGINFO_RIN_DATA_(\d{6})\.xml$", os.path.basename(xml_path))
    return m.group(1) if m else ""


def localname(tag: str) -> str:
    """Return the local-name of an XML tag without namespace."""
    if tag is None:
        return ""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def text_of(elem):
    return (elem.text or "").strip() if elem is not None else ""


def first_nonempty(*vals):
    for v in vals:
        if v is not None and str(v).strip():
            return v
    return ""


def iter_xml_rin_records(xml_path: str, sample_limit: int = None):
    """
    Streaming parse of a UA XML yielding one dict per <RIN_INFO>.
    No use of local-name() in ElementPath to avoid InvalidPredicate errors.
    """
    pub_ym = publication_id_from_filename(xml_path)
    # We parse all end events and filter on localname == 'RIN_INFO'
    ctx = etree.iterparse(xml_path, events=("end",), recover=True)
    count = 0

    for ev, elem in ctx:
        if localname(elem.tag) != "RIN_INFO":
            # memory hygiene while skipping
            if elem.getparent() is not None:
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            continue

        # Now we are on a RIN_INFO container
        rin = text_of(elem.find("RIN"))
        if not rin:
            # Sometimes RIN might be nested oddly; try a safe scan
            cand = elem.find(".//RIN")
            rin = text_of(cand)
        if not rin:
            # clean and continue
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            if sample_limit and count >= sample_limit:
                break
            continue

        # Common fields (per examples in 199510 and 202410 files)
        # RULE_TITLE
        rule_title = first_nonempty(
            text_of(elem.find("RULE_TITLE")),
            text_of(elem.find(".//RULE_TITLE")),
            text_of(elem.find("TITLE")),
            text_of(elem.find(".//TITLE"))
        )

        # Agency names
        agency_name = first_nonempty(
            text_of(elem.find("AGENCY/NAME")),
            text_of(elem.find(".//AGENCY/NAME")),
            text_of(elem.find("AGENCY"))
        )
        parent_agency_name = first_nonempty(
            text_of(elem.find("PARENT_AGENCY/NAME")),
            text_of(elem.find(".//PARENT_AGENCY/NAME")),
            text_of(elem.find("PARENT_AGENCY"))
        )

        # Rule stage
        rule_stage = first_nonempty(
            text_of(elem.find("RULE_STAGE")),
            text_of(elem.find(".//RULE_STAGE")),
            text_of(elem.find("STAGE")),
            text_of(elem.find(".//STAGE"))
        )

        # Timetable list
        timetables = []
        for tt in elem.findall("TIMETABLE_LIST/TIMETABLE"):
            action = first_nonempty(text_of(tt.find("TTBL_ACTION")),
                                    text_of(tt.find("ACTION")))
            date_s = first_nonempty(text_of(tt.find("TTBL_DATE")),
                                    text_of(tt.find("DATE")))
            fr_cit = text_of(tt.find("FR_CITATION"))
            timetables.append({
                "action": action,
                "date": date_s,
                "fr_citation": fr_cit
            })

        yield {
            "pub_ym": pub_ym,
            "rin": rin,
            "xml_title": rule_title,
            "xml_agency": agency_name,
            "xml_parent_agency": parent_agency_name,
            "xml_rule_stage": rule_stage,
            "xml_timetable": timetables,
            "xml_path": xml_path
        }

        count += 1

        # memory hygiene after finishing a RIN_INFO container
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        if sample_limit is not None and count >= sample_limit:
            break


# =====================================
# 3) LOAD DATA (compiled CSV outputs)
# =====================================
rows = read_csv_safe(PATH_ROWS)
ck   = read_csv_safe(PATH_CK_LAST)
tt   = read_csv_safe(PATH_TIMETABLES)

# Normalize id columns across files
col_rin_rows = pick_col(rows, ["rin", "RIN"])
col_rin_ck   = pick_col(ck,   ["rin", "RIN"])
col_rin_tt   = pick_col(tt,   ["rin", "RIN"])

if not (col_rin_rows and col_rin_ck and col_rin_tt):
    raise RuntimeError("Could not find 'rin' column in one or more CSVs.")

rows[col_rin_rows] = as_str_series(rows[col_rin_rows]).str.strip()
ck[col_rin_ck]     = as_str_series(ck[col_rin_ck]).str.strip()
tt[col_rin_tt]     = as_str_series(tt[col_rin_tt]).str.strip()

# Publication ids (YYYYMM)
col_pub_rows = pick_col(rows, ["publication_id", "pub_ym", "pubym", "publicationId"])
col_pub_ck   = pick_col(ck,   ["last_pub_ym", "pub_ym", "publication_id"])
col_pub_tt   = pick_col(tt,   ["publication_id", "pub_ym"])

if not col_pub_rows:
    raise RuntimeError("Could not find publication_id column in ua_rows.csv.")

rows[col_pub_rows] = as_str_series(rows[col_pub_rows]).str.replace(r"\D", "", regex=True)
if col_pub_ck:
    ck[col_pub_ck] = as_str_series(ck[col_pub_ck]).str.replace(r"\D", "", regex=True)
if col_pub_tt:
    tt[col_pub_tt] = as_str_series(tt[col_pub_tt]).str.replace(r"\D", "", regex=True)

# Timetable date columns
col_tt_date   = pick_col(tt, ["ttbl_date", "date", "timetable_date"])
col_tt_action = pick_col(tt, ["ttbl_action", "action", "timetable_action"])
if col_tt_date:
    tt["_tt_date"] = tt[col_tt_date].apply(to_date_safe)


# ======================================
# 4) CHECK A: CK-last vs true last issue
# ======================================
# For each RIN, compute the true last (max) publication_id observed in ua_rows.csv
rows_last = (rows
             .groupby(col_rin_rows)[col_pub_rows]
             .max()
             .reset_index()
             .rename(columns={col_pub_rows: "rows_max_pub_ym"}))

ck_key = col_pub_ck or "pub_ym"  # fallback if older variant
if ck_key not in ck.columns:
    ck["last_pub_ym_in_ck"] = pd.NA
else:
    ck["last_pub_ym_in_ck"] = ck[ck_key]

ck_last_check = (ck[[col_rin_ck, "last_pub_ym_in_ck"]]
                 .merge(rows_last, left_on=col_rin_ck, right_on=col_rin_rows, how="left"))

ck_last_check["pub_ym_mismatch"] = ck_last_check["last_pub_ym_in_ck"].fillna("") != ck_last_check["rows_max_pub_ym"].fillna("")
mismatch_ck = ck_last_check[ck_last_check["pub_ym_mismatch"] == True].copy()

mismatch_ck_out = os.path.join(VERIFY_DIR, "ua_verify_ck_vs_rows.csv")
mismatch_ck.sort_values([col_rin_ck]).to_csv(mismatch_ck_out, index=False)


# =================================================
# 5) CHECK B: latest timetable vs latest_* in CK
# =================================================
tt_latest = None
if col_tt_date:
    tt_latest = (tt
                 .dropna(subset=["_tt_date"])
                 .sort_values(["_tt_date"])
                 .groupby(col_rin_tt)
                 .agg(tt_latest_date=("_tt_date", "max"),
                      tt_latest_action=(col_tt_action, "last"))
                 .reset_index())
    tt_latest["tt_latest_date"] = pd.to_datetime(tt_latest["tt_latest_date"]).dt.normalize()

col_latest_date_ck = pick_col(ck, ["latest_action_date", "last_action_date", "latest_date"])
col_latest_act_ck  = pick_col(ck, ["latest_action", "last_action", "latest_action_name"])

if tt_latest is not None and col_latest_date_ck:
    ck["_ck_latest_date"] = pd.to_datetime(ck[col_latest_date_ck].apply(to_date_safe)).dt.normalize()
    if col_latest_act_ck:
        ck["_ck_latest_action"] = ck[col_latest_act_ck].astype("string")

    tt_merge = (ck[[col_rin_ck, "_ck_latest_date", "_ck_latest_action"]]
                .merge(tt_latest, left_on=col_rin_ck, right_on=col_rin_tt, how="left"))

    tt_merge["latest_date_mismatch"] = (tt_merge["_ck_latest_date"].fillna(pd.NaT) != tt_merge["tt_latest_date"].fillna(pd.NaT))

    def _norm(s):
        return ("" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)).strip().lower()

    if col_latest_act_ck:
        tt_merge["latest_action_mismatch"] = tt_merge.apply(
            lambda r: _norm(r.get("_ck_latest_action")) != _norm(r.get("tt_latest_action")),
            axis=1
        )
    else:
        tt_merge["latest_action_mismatch"] = pd.NA

    tt_mismatch = tt_merge[(tt_merge["latest_date_mismatch"] == True) |
                           (tt_merge["latest_action_mismatch"] == True)].copy()
    tt_mismatch_out = os.path.join(VERIFY_DIR, "ua_verify_timetable_mismatch.csv")
    tt_mismatch.sort_values([col_rin_ck]).to_csv(tt_mismatch_out, index=False)
else:
    tt_mismatch_out = None


# =========================================
# 6) CHECK C: blank-rate for key columns
# =========================================
def blank_share(df: pd.DataFrame, cols):
    out = []
    n = len(df)
    for c in cols:
        if c in df.columns:
            blank = df[c].isna() | (df[c].astype("string").str.strip() == "")
            out.append({"column": c, "blank_count": int(blank.sum()), "blank_pct": float(blank.mean())})
    return pd.DataFrame(out)

agency_col_ck        = pick_col(ck,   ["agency", "AGENCY", "Agency"])
parent_agency_col_ck = pick_col(ck,   ["parent_agency", "Parent_Agency", "PARENT_AGENCY", "Parent-Agency"])
deadline_cols_ck     = [c for c in ck.columns if ("deadline" in c.lower()) or ("legal" in c.lower())]

blank_df = blank_share(ck, [c for c in [agency_col_ck, parent_agency_col_ck] if c] + deadline_cols_ck)
blank_out = os.path.join(VERIFY_DIR, "ua_verify_blank_rates.csv")
blank_df.to_csv(blank_out, index=False)


# ==========================================
# 7) CHECK D: XML trace-back (spot checks)
# ==========================================
def get_xml_path_for_pubym(pub_ym: str) -> str:
    return os.path.join(UA_DIR, f"REGINFO_RIN_DATA_{pub_ym}.xml")

xml_trace_rows = []
SAMPLE_PER_XML = 12

if col_pub_rows:
    # small in-memory indices to speed lookups
    ck_idx = ck.set_index(col_rin_ck, drop=False) if col_rin_ck in ck.columns else None

    tt_idx = None
    if col_tt_date:
        tt_idx = tt.copy()
        tt_idx["_tt_date"] = tt_idx[col_tt_date].apply(to_date_safe)
        tt_idx = tt_idx.set_index(col_rin_tt, drop=False)

    for pub_ym in TRACE_PUB_YMS:
        xml_path = get_xml_path_for_pubym(pub_ym)
        if not os.path.isfile(xml_path):
            xml_trace_rows.append({
                "pub_ym": pub_ym,
                "rin": None,
                "xml_path": xml_path,
                "trace_note": f"XML file not found for pub_ym={pub_ym}"
            })
            continue

        sample_rins = (rows[rows[col_pub_rows] == pub_ym][col_rin_rows]
                       .dropna()
                       .astype("string")
                       .str.strip()
                       .unique())
        sample_rins = list(sample_rins[:SAMPLE_PER_XML])

        # parse XML streaming and capture matches
        seen = set()
        for rec in iter_xml_rin_records(xml_path):
            r = rec["rin"]
            if r in sample_rins and r not in seen:
                seen.add(r)

                row_issue = rows[(rows[col_rin_rows] == r) & (rows[col_pub_rows] == pub_ym)].head(1)

                ck_row = ck_idx.loc[r] if (ck_idx is not None and r in ck_idx.index) else None

                # latest timetable from long file
                csv_latest_date, csv_latest_action = None, None
                if tt_idx is not None and r in tt_idx.index and "_tt_date" in tt_idx.columns:
                    tt_sub = tt_idx.loc[[r]] if isinstance(tt_idx.loc[r], pd.DataFrame) else tt_idx.loc[r].to_frame().T
                    tt_sub = tt_sub.dropna(subset=["_tt_date"]).sort_values("_tt_date")
                    if not tt_sub.empty:
                        csv_latest_date = tt_sub["_tt_date"].iloc[-1]
                        ac_col = col_tt_action or "ttbl_action"
                        csv_latest_action = tt_sub[ac_col].iloc[-1] if ac_col in tt_sub.columns else None

                xml_trace_rows.append({
                    "rin": r,
                    "pub_ym": pub_ym,
                    "xml_path": xml_path,
                    "xml_title": rec["xml_title"],
                    "xml_agency": rec["xml_agency"],
                    "xml_parent_agency": rec["xml_parent_agency"],
                    "xml_rule_stage": rec["xml_rule_stage"],
                    "xml_timetable_count": len(rec["xml_timetable"]),
                    "csv_agency": (row_issue.get(agency_col_ck).iloc[0] if (agency_col_ck and not row_issue.empty and agency_col_ck in row_issue.columns) else None),
                    "csv_parent_agency": (row_issue.get(parent_agency_col_ck).iloc[0] if (parent_agency_col_ck and not row_issue.empty and parent_agency_col_ck in row_issue.columns) else None),
                    "ck_latest_action_date": (str(ck_row[col_latest_date_ck]) if (ck_row is not None and col_latest_date_ck in ck_row) else None),
                    "ck_latest_action": (ck_row[col_latest_act_ck] if (ck_row is not None and col_latest_act_ck in ck_row) else None),
                    "csv_latest_ttbl_date": (str(csv_latest_date) if csv_latest_date is not None else None),
                    "csv_latest_ttbl_action": (str(csv_latest_action) if csv_latest_action is not None else None),
                    "trace_note": ""
                })

        # any sampled RIN not seen = potential parse miss
        for r in sample_rins:
            if r not in {row["rin"] for row in xml_trace_rows if row["pub_ym"] == pub_ym}:
                xml_trace_rows.append({
                    "rin": r, "pub_ym": pub_ym, "xml_path": xml_path,
                    "trace_note": "RIN not found in XML (parser miss or true absence)"
                })

# Write XML trace results
xml_trace_df = pd.DataFrame(xml_trace_rows)
xml_trace_out = os.path.join(VERIFY_DIR, "ua_verify_xml_trace.csv")
xml_trace_df.to_csv(xml_trace_out, index=False)


# ==========================================
# 8) HUMAN-READABLE SUMMARY
# ==========================================
n_issue_rows   = len(rows)
n_ck_rows      = len(ck)
n_tt_rows      = len(tt)
n_rin_issue    = rows[col_rin_rows].nunique()
n_rin_ck       = ck[col_rin_ck].nunique()

summary_lines = []
summary_lines.append("=== UA Verification Summary ===")
summary_lines.append(f"ua_rows.csv:       rows={n_issue_rows:,}  distinct RINs={n_rin_issue:,}")
summary_lines.append(f"ua_ck_last.csv:    rows={n_ck_rows:,}     distinct RINs={n_rin_ck:,}")
summary_lines.append(f"ua_timetables.csv: rows={n_tt_rows:,}")
summary_lines.append("")

# Mismatch counts
summary_lines.append(f"CK vs Rows (last pub_ym mismatches): {len(mismatch_ck):,}")
if len(mismatch_ck) > 0:
    summary_lines.append(f"  -> Details: {mismatch_ck_out}")

if tt_mismatch_out and os.path.isfile(tt_mismatch_out):
    tt_m = pd.read_csv(tt_mismatch_out)
    summary_lines.append(f"Timetable latest mismatches: {len(tt_m):,}")
    summary_lines.append(f"  -> Details: {tt_mismatch_out}")
else:
    summary_lines.append("Timetable latest mismatches: (check skipped – date columns missing)")

summary_lines.append("")
summary_lines.append("Blank-rate snapshot in ua_ck_last (selected columns):")
if not blank_df.empty:
    for _, r in blank_df.iterrows():
        summary_lines.append(f"  {r['column']}: blanks={int(r['blank_count']):,} ({r['blank_pct']:.1%})")
    summary_lines.append(f"  -> Details: {blank_out}")
else:
    summary_lines.append("  (no candidate blank columns found)")

summary_lines.append("")
summary_lines.append("XML spot-check trace:")
summary_lines.append(f"  XML trace rows: {len(xml_trace_df):,}")
summary_lines.append(f"  -> Details: {xml_trace_out}")

summary_txt = os.path.join(VERIFY_DIR, "ua_verify_summary.txt")
with open(summary_txt, "w", encoding="utf-8") as f:
    f.write("\n".join(summary_lines))

print("\n".join(summary_lines))
print(f"\nVerification artifacts written under: {VERIFY_DIR}")
