# UA_CHECKER.py
# -------------------------------------------------------------
# Verifies compiled UA CSVs:
# 1) The core check: for selected publication_ids, stream the XML,
#    flatten each <RIN_INFO>, and compare EVERY field to the matching
#    row in ua_rows.csv (same normalization as compiler).
# 2) CK-last vs rows: ensure last_pub_ym equals the max pub_ym IN-WINDOW.
# 3) Timetable latest: recompute from last issue only and compare.
# 4) Blank rates for key columns.
#
# Artifacts under <OUT_DIR>/_verify:
#   ua_verify_xml_vs_csv_detail.csv  (row-by-field diffs)
#   ua_verify_xml_missing_columns.csv (xml fields unseen in csv columns)
#   ua_verify_ck_vs_rows.csv
#   ua_verify_timetable_mismatch.csv
#   ua_verify_blank_rates.csv
#   ua_verify_summary.txt
#
# Requires: pandas, lxml, python-dateutil
# -------------------------------------------------------------

import os
import re
import json
from datetime import datetime

import pandas as pd
from lxml import etree
from dateutil import parser as dateparser

# =======================
# 1) PATHS (Tony’s env)
# =======================
UA_DIR  = "/Users/tonymolino/Dropbox/Mac/Desktop/NEW_ML_REGULATIONS_PAPER 2/Unified_Agenda_Download/ua_main_data"
OUT_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA"

os.makedirs(OUT_DIR, exist_ok=True)

# Compiled CSVs
PATH_ROWS       = os.path.join(OUT_DIR, "ua_rows.csv")
PATH_CK_LAST    = os.path.join(OUT_DIR, "ua_ck_last.csv")
PATH_TIMETABLES = os.path.join(OUT_DIR, "ua_timetables.csv")

VERIFY_DIR = os.path.join(OUT_DIR, "_verify")
os.makedirs(VERIFY_DIR, exist_ok=True)

# XML files to trace deeply (you can add 2020–2024 here as desired)
TRACE_PUB_YMS = [
    "199510", "200910", "201710", "201904", "202104", "202410",
]

# ==================
# 2) Normalization (match compiler)
# ==================
def t(x): return (x or "").strip()

def localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if tag else ""

def snake(s: str) -> str:
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()

_CANON = {
    "title": "title",
    "rule_title": "title",
    "stage": "stage",
    "rule_stage": "stage",
    "priority": "priority",
    "priority_category": "priority_category",
    "publication_id": "publication_id",
    "publication_title": "publication_title",
    "rin": "rin",
}
def canon(col: str) -> str:
    key = snake(col)
    return _CANON.get(key, key)

def parse_tt_date(raw):
    s = t(raw)
    if s == "" or s.lower().startswith(("to be","tbd")):
        return s, ""
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not (1 <= mm <= 12):
            return s, ""
        if dd == 0:
            dd = 1
        return s, f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    m2 = re.match(r"^(\d{1,2})/(\d{4})$", s)
    if m2:
        mm, yyyy = int(m2.group(1)), int(m2.group(2))
        if not (1 <= mm <= 12):
            return s, ""
        return s, f"{yyyy:04d}-{mm:02d}-01"
    try:
        dt = dateparser.parse(s, fuzzy=True, default=datetime(1900,1,1))
        return s, dt.strftime("%Y-%m-%d")
    except Exception:
        return s, ""

def publication_id_from_filename(xml_path: str) -> str:
    m = re.search(r"REGINFO_RIN_DATA_(\d{6})\.xml$", os.path.basename(xml_path))
    return m.group(1) if m else ""

def as_json_str(obj):
    def _default(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return str(o)
        return str(o)
    try:
        return json.dumps(obj, ensure_ascii=False, default=_default)
    except TypeError:
        return json.dumps(str(obj), ensure_ascii=False)

# ------------- XML flatteners (namespace-agnostic) -------------
_LIST_CONTAINERS = {
    "agency_contact_list","timetable_list","legal_dline_list","cfr_list",
    "legal_authority_list","small_entity_list","govt_level_list",
    "related_rin_list","unfunded_mandate_list"
}
def iter_desc(elem):
    yield elem
    for d in elem.iterdescendants():
        yield d

def has_ancestor_named(el, names_lower_set):
    p = el.getparent()
    while p is not None:
        if localname(p.tag).lower() in names_lower_set:
            return True
        p = p.getparent()
    return False

def first_desc_by_name(elem, name_lower, exclude_under=None):
    ex = set(n.lower() for n in (exclude_under or []))
    for d in elem.iter():
        if localname(d.tag).lower() == name_lower:
            if ex and has_ancestor_named(d, ex):
                continue
            return d
    return None

def all_desc_by_name(elem, name_lower, exclude_under=None):
    ex = set(n.lower() for n in (exclude_under or []))
    for d in elem.iter():
        if localname(d.tag).lower() == name_lower:
            if ex and has_ancestor_named(d, ex):
                continue
            yield d

def flatten_rin_info(rin_info, source_xml, pub_ym):
    rec = {"source_xml": os.path.basename(source_xml), "publication_id": pub_ym}

    # RIN
    rin_node = first_desc_by_name(rin_info, "rin")
    rec["rin"] = t(rin_node.text) if rin_node is not None else ""

    # Publication (optional inside)
    pub = first_desc_by_name(rin_info, "publication", exclude_under=_LIST_CONTAINERS)
    if pub is not None:
        pid = first_desc_by_name(pub, "publication_id")
        ptt = first_desc_by_name(pub, "publication_title")
        if pid is not None:
            rec["publication_id"] = t(pid.text)
        if ptt is not None:
            rec["publication_title"] = t(ptt.text)

    # Agency / Parent agency
    def _agency_block(block_name):
        out = {}
        blk = first_desc_by_name(rin_info, block_name, exclude_under={"agency_contact_list"})
        if blk is None:
            return out
        code = first_desc_by_name(blk, "code")
        name = first_desc_by_name(blk, "name")
        acr  = first_desc_by_name(blk, "acronym")
        out[f"{block_name}_code"]    = t(code.text) if code is not None else ""
        out[f"{block_name}_name"]    = t(name.text) if name is not None else ""
        out[f"{block_name}_acronym"] = t(acr.text)  if acr  is not None else ""
        # shorthand names
        if block_name == "agency" and "agency_name" in out:
            out["agency"] = out["agency_name"]
        if block_name == "parent_agency" and "parent_agency_name" in out:
            out["parent_agency"] = out["parent_agency_name"]
        return out

    rec.update(_agency_block("agency"))
    rec.update(_agency_block("parent_agency"))

    # Lists
    def _list_texts(list_tag, item_tag):
        arr = []
        for lst in all_desc_by_name(rin_info, list_tag):
            for d in lst.iter():
                if localname(d.tag).lower() == item_tag:
                    if has_ancestor_named(d, {list_tag}) and not has_ancestor_named(d, {"agency_contact_list"}):
                        arr.append(t(d.text))
        return arr

    def _related_rins():
        out = []
        for lst in all_desc_by_name(rin_info, "related_rin_list"):
            for rr in lst.iter():
                if localname(rr.tag).lower() != "related_rin":
                    continue
                rnode = first_desc_by_name(rr, "rin")
                rel   = first_desc_by_name(rr, "rin_relation")
                out.append({"rin": t(rnode.text) if rnode is not None else "",
                            "relation": t(rel.text) if rel is not None else ""})
        return out

    def _contacts():
        out = []
        for cl in all_desc_by_name(rin_info, "agency_contact_list"):
            for c in cl.iter():
                if localname(c.tag).lower() != "contact":
                    continue
                item = {}
                for tag in ["prefix","first_name","middle_name","last_name","title","phone","fax","email"]:
                    node = first_desc_by_name(c, tag)
                    if node is not None:
                        item[tag] = t(node.text)
                cag = first_desc_by_name(c, "agency")
                if cag is not None:
                    for tag in ["code","name","acronym"]:
                        node = first_desc_by_name(cag, tag)
                        item[f"contact_agency_{tag}"] = t(node.text) if node is not None else ""
                addr = first_desc_by_name(c, "mailing_address")
                if addr is not None:
                    for tag in ["street_address","city","state","zip"]:
                        node = first_desc_by_name(addr, tag)
                        item[f"address_{tag}"] = t(node.text) if node is not None else ""
                out.append(item)
        return out

    def _unfunded():
        arr = []
        for lst in all_desc_by_name(rin_info, "unfunded_mandate_list"):
            for u in lst.iter():
                if localname(u.tag).lower() == "unfunded_mandate":
                    arr.append(t(u.text))
        return arr

    def _legal_deadlines():
        out = []
        for lst in all_desc_by_name(rin_info, "legal_dline_list"):
            for info in lst.iter():
                if localname(info.tag).lower() != "legal_dline_info":
                    continue
                d = {}
                for tag in ["dline_type","dline_action_stage","dline_date","dline_desc"]:
                    node = first_desc_by_name(info, tag)
                    d[tag] = t(node.text) if node is not None else ""
                out.append(d)
        return out

    def _timetable():
        out = []
        for lst in all_desc_by_name(rin_info, "timetable_list"):
            for tt in lst.iter():
                if localname(tt.tag).lower() != "timetable":
                    continue
                act = first_desc_by_name(tt, "ttbl_action") or first_desc_by_name(tt, "action")
                dte = first_desc_by_name(tt, "ttbl_date")  or first_desc_by_name(tt, "date")
                fr  = first_desc_by_name(tt, "fr_citation")
                raw, iso = parse_tt_date(t(dte.text) if dte is not None else "")
                out.append({
                    "action": t(act.text) if act is not None else "",
                    "date_raw": raw,
                    "date_iso": iso,
                    "fr_citation": t(fr.text) if fr is not None else ""
                })
        return out

    rec["cfr_list"]              = as_json_str(_list_texts("cfr_list","cfr"))
    rec["legal_authority_list"]  = as_json_str(_list_texts("legal_authority_list","legal_authority"))
    rec["small_entity_list"]     = as_json_str(_list_texts("small_entity_list","small_entity"))
    rec["govt_level_list"]       = as_json_str(_list_texts("govt_level_list","govt_level"))
    rec["unfunded_mandate_list"] = as_json_str(_unfunded())
    rec["related_rins"]          = as_json_str(_related_rins())
    rec["contacts"]              = as_json_str(_contacts())

    ld = _legal_deadlines()
    rec["legal_deadline_list"] = as_json_str(ld)

    tts = _timetable()
    rec["timetable_all"] = as_json_str(tts)
    # latest within issue
    latest_iso, latest_action = "", ""
    if tts:
        tts_sorted = sorted(tts, key=lambda d: (d.get("date_iso",""), d.get("action","")))
        latest_iso    = tts_sorted[-1].get("date_iso","") or ""
        latest_action = tts_sorted[-1].get("action","")    or ""
    rec["latest_action_date_in_issue"] = latest_iso
    rec["latest_action_in_issue"]      = latest_action

    # Scalar leaves outside list/agency/publication
    exclude_anc = _LIST_CONTAINERS | {"agency","parent_agency","publication"}
    for el in rin_info.iter():
        if len(el) > 0:
            continue
        if has_ancestor_named(el, exclude_anc):
            continue
        ln = localname(el.tag)
        if not ln or ln.lower() == "rin":
            continue
        key = canon(ln)
        rec[key] = t(el.text)

    # Canonical synonyms
    if "rule_title" in rec and "title" not in rec:
        rec["title"] = rec["rule_title"]
    if "rule_stage" in rec and "stage" not in rec:
        rec["stage"] = rec["rule_stage"]

    return rec

def read_csv_safe(path):
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

# ==========================
# 3) Load compiled outputs
# ==========================
rows = read_csv_safe(PATH_ROWS)
ck   = read_csv_safe(PATH_CK_LAST) if os.path.isfile(PATH_CK_LAST) else pd.DataFrame()
tt   = read_csv_safe(PATH_TIMETABLES) if os.path.isfile(PATH_TIMETABLES) else pd.DataFrame()

# normalize identifiers
if "rin" in rows.columns:
    rows["rin"] = rows["rin"].astype(str).str.strip()
if "publication_id" in rows.columns:
    rows["publication_id"] = rows["publication_id"].astype(str).str.replace(r"\D","",regex=True)

if not tt.empty:
    tt["rin"] = tt["rin"].astype(str).str.strip()
    tt["publication_id"] = tt["publication_id"].astype(str).str.replace(r"\D","",regex=True)
    tt["_tt_date_iso"] = tt["ttbl_date_iso"].astype(str).fillna("")

# ==========================
# 4) XML vs CSV deep check
# ==========================
xml_vs_csv_rows = []
missing_columns_rows = []

for pub_ym in TRACE_PUB_YMS:
    xml_path = os.path.join(UA_DIR, f"REGINFO_RIN_DATA_{pub_ym}.xml")
    if not os.path.isfile(xml_path):
        xml_vs_csv_rows.append({
            "publication_id": pub_ym, "rin": None, "field": None,
            "xml_value": None, "csv_value": None,
            "status": "XML file not found", "xml_path": xml_path
        })
        continue

    # subset of rows for this issue, keyed by rin
    issue_rows = rows[rows["publication_id"] == pub_ym].copy()
    issue_idx  = issue_rows.set_index("rin") if not issue_rows.empty else pd.DataFrame()

    ctx = etree.iterparse(xml_path, events=("end",), recover=True, huge_tree=True)
    for ev, el in ctx:
        if localname(el.tag).lower() != "rin_info":
            continue

        pub_from_name = publication_id_from_filename(xml_path)
        rec_xml = flatten_rin_info(el, xml_path, pub_from_name)
        rin = rec_xml.get("rin","")
        if rin == "":
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]
            continue

        # Compare to CSV row: same (rin, publication_id)
        if not issue_rows.empty and rin in issue_idx.index:
            row = issue_idx.loc[rin]
            if isinstance(row, pd.DataFrame):  # rare duplicate rows (shouldn't happen)
                row = row.head(1).iloc[0]
            # For every XML field
            for k_xml, v_xml in rec_xml.items():
                if k_xml in {"source_xml"}:
                    continue
                if k_xml not in rows.columns:
                    missing_columns_rows.append({
                        "publication_id": pub_ym, "rin": rin, "missing_column": k_xml,
                        "xml_example_value": v_xml
                    })
                    status = "missing_in_csv_columns"
                    xml_vs_csv_rows.append({
                        "publication_id": pub_ym, "rin": rin, "field": k_xml,
                        "xml_value": v_xml, "csv_value": "",
                        "status": status, "xml_path": xml_path
                    })
                    continue
                v_csv = row.get(k_xml, "")
                # Normalize JSON lists for comparison
                if k_xml.endswith("_list") or k_xml in {"contacts","related_rins","timetable_all"}:
                    try:
                        A = json.loads(v_xml) if isinstance(v_xml, str) else v_xml
                    except Exception:
                        A = str(v_xml)
                    try:
                        B = json.loads(v_csv) if isinstance(v_csv, str) else v_csv
                    except Exception:
                        B = str(v_csv)
                    # Basic comparison; could be enhanced to ignore ordering if needed
                    equal = (A == B)
                    status = "match" if equal else "mismatch"
                else:
                    status = "match" if str(v_csv).strip() == str(v_xml).strip() else "mismatch"

                xml_vs_csv_rows.append({
                    "publication_id": pub_ym, "rin": rin, "field": k_xml,
                    "xml_value": v_xml, "csv_value": v_csv,
                    "status": status, "xml_path": xml_path
                })
        else:
            # No CSV row for this RIN/issue
            xml_vs_csv_rows.append({
                "publication_id": pub_ym, "rin": rin, "field": None,
                "xml_value": None, "csv_value": None,
                "status": "CSV row not found for (rin, publication_id)",
                "xml_path": xml_path
            })

        # memory hygiene
        el.clear()
        while el.getprevious() is not None:
            del el.getparent()[0]

# Write deep-compare artifacts
xml_vs_csv_df = pd.DataFrame(xml_vs_csv_rows)
xml_vs_csv_out = os.path.join(VERIFY_DIR, "ua_verify_xml_vs_csv_detail.csv")
xml_vs_csv_df.to_csv(xml_vs_csv_out, index=False)

missing_cols_df = pd.DataFrame(missing_columns_rows).drop_duplicates()
missing_cols_out = os.path.join(VERIFY_DIR, "ua_verify_xml_missing_columns.csv")
missing_cols_df.to_csv(missing_cols_out, index=False)

# ==========================
# 5) CK vs Rows (last issue)
# ==========================
ck_vs_rows_out = os.path.join(VERIFY_DIR, "ua_verify_ck_vs_rows.csv")
if not ck.empty and "last_pub_ym" in ck.columns:
    rows_last = (rows.groupby("rin")["publication_id"].max().reset_index()
                 .rename(columns={"publication_id":"rows_max_pub_ym"}))
    ck_check = ck[["rin","last_pub_ym"]].merge(rows_last, on="rin", how="left")
    ck_check["pub_ym_mismatch"] = ck_check["last_pub_ym"].fillna("") != ck_check["rows_max_pub_ym"].fillna("")
    ck_check[ck_check["pub_ym_mismatch"] == True].to_csv(ck_vs_rows_out, index=False)
else:
    pd.DataFrame().to_csv(ck_vs_rows_out, index=False)

# ============================================
# 6) Timetable latest (last issue only) check
# ============================================
tt_mismatch_out = os.path.join(VERIFY_DIR, "ua_verify_timetable_mismatch.csv")
if not ck.empty and not rows.empty and not tt.empty:
    # For each RIN in CK, take the last_pub_ym and compare tt latest to CK fields
    ck_use = ck[["rin","last_pub_ym","latest_action_last_issue","latest_action_date_last_issue"]].copy()
    # derive last-issue timetable from the long table
    join = tt.merge(ck_use[["rin","last_pub_ym"]], left_on=["rin","publication_id"], right_on=["rin","last_pub_ym"], how="inner")
    # rank by iso date then action to stabilize
    join = join.sort_values(["rin","ttbl_date_iso","ttbl_action"])
    last_by_rin = join.groupby("rin").tail(1)
    last_map_date = dict(zip(last_by_rin["rin"], last_by_rin["ttbl_date_iso"]))
    last_map_act  = dict(zip(last_by_rin["rin"], last_by_rin["ttbl_action"]))

    ck_use["tt_latest_date_from_long"]   = ck_use["rin"].map(last_map_date).fillna("")
    ck_use["tt_latest_action_from_long"] = ck_use["rin"].map(last_map_act).fillna("")

    ck_use["date_mismatch"]   = ck_use["latest_action_date_last_issue"].astype(str).str.strip() != ck_use["tt_latest_date_from_long"].astype(str).str.strip()
    ck_use["action_mismatch"] = ck_use["latest_action_last_issue"].astype(str).str.strip()       != ck_use["tt_latest_action_from_long"].astype(str).str.strip()

    ck_use[(ck_use["date_mismatch"] | ck_use["action_mismatch"])].to_csv(tt_mismatch_out, index=False)
else:
    pd.DataFrame().to_csv(tt_mismatch_out, index=False)

# ==========================
# 7) Blank-rate snapshot
# ==========================
def blank_share(df: pd.DataFrame, cols):
    out = []
    n = len(df)
    for c in cols:
        if c in df.columns:
            blank = df[c].isna() | (df[c].astype(str).str.strip() == "")
            out.append({"column": c, "blank_count": int(blank.sum()), "blank_pct": float(blank.mean())})
    return pd.DataFrame(out)

blank_cols = [c for c in ["agency","parent_agency","legal_deadline_list"] if c in ck.columns]
blank_df = blank_share(ck, blank_cols)
blank_out = os.path.join(VERIFY_DIR, "ua_verify_blank_rates.csv")
blank_df.to_csv(blank_out, index=False)

# ==========================
# 8) Human summary
# ==========================
lines = []
lines.append("=== UA Verification Summary ===")
if os.path.isfile(xml_vs_csv_out):
    df = pd.read_csv(xml_vs_csv_out, low_memory=False)
    n_total = len(df)
    n_mismatch = (df["status"] == "mismatch").sum()
    n_missing_cols = (df["status"] == "missing_in_csv_columns").sum()
    n_notfound = (df["status"].str.contains("not found", na=False)).sum()
    lines.append(f"XML vs CSV detail rows: {n_total:,}")
    lines.append(f"  mismatches: {n_mismatch:,}; missing columns: {n_missing_cols:,}; not-found rows/files: {n_notfound:,}")
    lines.append(f"  -> Detail: {xml_vs_csv_out}")
if os.path.isfile(missing_cols_out):
    lines.append(f"XML fields missing as CSV columns (unique rows): {len(missing_cols_df):,}")
    lines.append(f"  -> Detail: {missing_cols_out}")

if os.path.isfile(ck_vs_rows_out):
    mism = pd.read_csv(ck_vs_rows_out)
    lines.append(f"CK vs Rows (last pub_ym mismatches): {len(mism):,}")
    lines.append(f"  -> Detail: {ck_vs_rows_out}")

if os.path.isfile(tt_mismatch_out):
    tm = pd.read_csv(tt_mismatch_out)
    lines.append(f"Timetable latest mismatches (last issue only): {len(tm):,}")
    lines.append(f"  -> Detail: {tt_mismatch_out}")

if os.path.isfile(blank_out):
    lines.append("Blank-rate snapshot (CK-last):")
    for _, r in blank_df.iterrows():
        lines.append(f"  {r['column']}: blanks={int(r['blank_count']):,} ({r['blank_pct']:.1%})")
    lines.append(f"  -> Detail: {blank_out}")

summary_txt = os.path.join(VERIFY_DIR, "ua_verify_summary.txt")
with open(summary_txt, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print("\n".join(lines))
print(f"\nVerification artifacts written under: {VERIFY_DIR}")
