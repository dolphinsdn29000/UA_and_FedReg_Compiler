# UA_COMPILER_8_22_25.py
# -------------------------------------------------------------
# Unified Agenda compiler (CK-like), with dynamic column union.
# - Brings ALL fields encountered across all XMLs (1995..)
# - Preserves list blocks as JSON strings in CSV
# - Computes CK "last" strictly from the RIN's last publication
# - Latest timetable fields come ONLY from the last issue
#
# Outputs under <OUT_DIR>:
#   ua_rows.csv                (per issue)
#   ua_timetables.csv          (long-form timetable)
#   ua_ck_last.csv             (one row per RIN; backfill + last-issue timetable)
#   ua_ck_counts.csv           (counts by year/season)
#   ua_yearly_counts.png       (chart)
#
# Requires: pandas, lxml, python-dateutil, matplotlib
# -------------------------------------------------------------

import os
import re
import json
import math
from collections import defaultdict, Counter
from datetime import datetime
from dateutil import parser as dateparser

import pandas as pd
from lxml import etree

import matplotlib
matplotlib.use("Agg")  # headlesså
import matplotlib.pyplot as plt


# =======================
# 1) PATHS (hard-coded)
# =======================
UA_DIR  = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data"
OUT_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA"
os.makedirs(OUT_DIR, exist_ok=True)

CK_START = "199510"   # CK window start (inclusive)
CK_END   = "201912"   # CK window end   (inclusive)

# ------------- Helpers -------------
def localname(tag):
    if tag is None:
        return ""
    # '{ns}NAME' -> 'NAME'
    return tag.split("}")[-1]

def t(x):  # safe text
    return (x or "").strip()

def to_pub_ym_from_file(fname):
    m = re.search(r"REGINFO_RIN_DATA_(\d{6})\.xml$", os.path.basename(fname))
    return m.group(1) if m else None

def parse_tt_date(raw):
    """
    Parse UA 'TTBL_DATE', which can be:
      - 'MM/DD/YYYY', 'MM/00/YYYY', 'MM/YYYY', 'To Be Determined', '', etc.
    Return tuple: (raw_str, iso_date_or_blank)
    We keep raw intact; for comparisons we coerce:
      - 'MM/00/YYYY' -> YYYY-MM-01
      - 'MM/YYYY'    -> YYYY-MM-01
      - unparseable  -> ''
    """
    s = t(raw)
    if s == "" or s.lower().startswith("to be"):
        return s, ""
    # Standardize single slashes; accept e.g., 10/00/1995
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if mm < 1 or mm > 12:
            return s, ""
        if dd == 0:
            dd = 1
        return s, f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    m2 = re.match(r"^(\d{1,2})/(\d{4})$", s)
    if m2:
        mm, yyyy = int(m2.group(1)), int(m2.group(2))
        if mm < 1 or mm > 12:
            return s, ""
        return s, f"{yyyy:04d}-{mm:02d}-01"
    # Fallback parser; may or may not succeed
    try:
        dt = dateparser.parse(s, fuzzy=True, default=datetime(1900,1,1))
        return s, dt.strftime("%Y-%m-%d")
    except Exception:
        return s, ""

def pub_season(ym):
    if not ym or len(ym) != 6 or not ym.isdigit():
        return ""
    return "Spring" if ym[4:] == "04" else ("Fall" if ym[4:] == "10" else "")

def as_json_str(obj):
    # Ensure timestamps, dates, numpy types become strings
    def default(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return str(o)
        return str(o)
    try:
        return json.dumps(obj, ensure_ascii=False, default=default)
    except TypeError:
        # Last resort: stringify
        return json.dumps(str(obj), ensure_ascii=False)

# ---- Extraction for structured subtrees ----
def extract_publication(elem):
    out = {}
    pub = elem.find(".//*[local-name()='PUBLICATION']")
    if pub is not None:
        pid   = pub.find(".//*[local-name()='PUBLICATION_ID']")
        pttl  = pub.find(".//*[local-name()='PUBLICATION_TITLE']")
        out["publication_id"]    = t(pid.text) if pid is not None else ""
        out["publication_title"] = t(pttl.text) if pttl is not None else ""
    return out

def extract_agency(elem, tag_name):
    out = {}
    tag = elem.find(f".//*[local-name()='{tag_name}']")
    if tag is not None:
        code = tag.find(".//*[local-name()='CODE']")
        name = tag.find(".//*[local-name()='NAME']")
        acr  = tag.find(".//*[local-name()='ACRONYM']")
        out[f"{tag_name.lower()}_code"]    = t(code.text) if code is not None else ""
        out[f"{tag_name.lower()}_name"]    = t(name.text) if name is not None else ""
        out[f"{tag_name.lower()}_acronym"] = t(acr.text) if acr is not None else ""
    else:
        out[f"{tag_name.lower()}_code"]    = ""
        out[f"{tag_name.lower()}_name"]    = ""
        out[f"{tag_name.lower()}_acronym"] = ""
    return out

def list_texts(elem, list_tag, item_tag):
    arr = []
    lst = elem.find(f".//*[local-name()='{list_tag}']")
    if lst is None:
        return arr
    for it in lst.findall(f".//*[local-name()='{item_tag}']"):
        arr.append(t(it.text))
    return arr

def extract_related_rins(elem):
    out = []
    lst = elem.find(".//*[local-name()='RELATED_RIN_LIST']")
    if lst is None:
        return out
    for rr in lst.findall(".//*[local-name()='RELATED_RIN']"):
        r = rr.find(".//*[local-name()='RIN']")
        rel = rr.find(".//*[local-name()='RIN_RELATION']")
        out.append({"rin": t(r.text) if r is not None else "",
                    "relation": t(rel.text) if rel is not None else ""})
    return out

def extract_contacts(elem):
    out = []
    lst = elem.find(".//*[local-name()='AGENCY_CONTACT_LIST']")
    if lst is None:
        return out
    for c in lst.findall(".//*[local-name()='CONTACT']"):
        item = {}
        for tag in ["PREFIX","FIRST_NAME","MIDDLE_NAME","LAST_NAME","TITLE","PHONE","FAX","EMAIL"]:
            node = c.find(f".//*[local-name()='{tag}']")
            if node is not None:
                item[tag.lower()] = t(node.text)
        # nested agency in contact
        cag = c.find(".//*[local-name()='AGENCY']")
        if cag is not None:
            for tag in ["CODE","NAME","ACRONYM"]:
                node = cag.find(f".//*[local-name()='{tag}']")
                item[f"contact_agency_{tag.lower()}"] = t(node.text) if node is not None else ""
        # address
        addr = c.find(".//*[local-name()='MAILING_ADDRESS']")
        if addr is not None:
            for tag in ["STREET_ADDRESS","CITY","STATE","ZIP"]:
                node = addr.find(f".//*[local-name()='{tag}']")
                item[f"address_{tag.lower()}"] = t(node.text) if node is not None else ""
        out.append(item)
    return out

def extract_unfunded(elem):
    # Some vintages wrap in UNFUNDED_MANDATE_LIST/UNFUNDED_MANDATE
    arr = []
    lst = elem.find(".//*[local-name()='UNFUNDED_MANDATE_LIST']")
    if lst is None:
        return arr
    for u in lst.findall(".//*[local-name()='UNFUNDED_MANDATE']"):
        arr.append(t(u.text))
    return arr

def extract_legal_deadlines(elem):
    out = []
    lst = elem.find(".//*[local-name()='LEGAL_DLINE_LIST']")
    if lst is None:
        return out
    for info in lst.findall(".//*[local-name()='LEGAL_DLINE_INFO']"):
        d = {}
        for tag in ["DLINE_TYPE","DLINE_ACTION_STAGE","DLINE_DATE","DLINE_DESC"]:
            node = info.find(f".//*[local-name()='{tag}']")
            d[tag.lower()] = t(node.text) if node is not None else ""
        out.append(d)
    return out

def extract_timetable(elem):
    out = []
    lst = elem.find(".//*[local-name()='TIMETABLE_LIST']")
    if lst is None:
        return out
    for tt in lst.findall(".//*[local-name()='TIMETABLE']"):
        act = tt.find(".//*[local-name()='TTBL_ACTION']")
        dte = tt.find(".//*[local-name()='TTBL_DATE']")
        fr  = tt.find(".//*[local-name()='FR_CITATION']")
        raw, iso = parse_tt_date(t(dte.text) if dte is not None else "")
        out.append({"action": t(act.text) if act is not None else "",
                    "date_raw": raw,
                    "date_iso": iso,
                    "fr_citation": t(fr.text) if fr is not None else ""})
    return out

# gather additional simple leaf fields (outside lists/contacts/agency blocks)
_SIMPLE_EXCLUDE_ANCESTORS = {
    "PUBLICATION","AGENCY","PARENT_AGENCY","AGENCY_CONTACT_LIST",
    "TIMETABLE_LIST","LEGAL_DLINE_LIST","CFR_LIST","LEGAL_AUTHORITY_LIST",
    "SMALL_ENTITY_LIST","GOVT_LEVEL_LIST","RELATED_RIN_LIST","UNFUNDED_MANDATE_LIST"
}

def collect_simple_leaves(rin_info_elem):
    """
    Capture leaf texts that are not in list/contacts/agency/publication blocks.
    Keyed by local-name (upper snake). If duplicates, last one wins.
    """
    out = {}
    # Walk descendants
    for el in rin_info_elem.iter():
        ln = localname(el.tag)
        if not ln or len(el) > 0:
            continue  # not a leaf
        # Ascend to check ancestor exclusions
        p = el.getparent()
        excluded = False
        while p is not None and localname(p.tag) != "RIN_INFO":
            if localname(p.tag) in _SIMPLE_EXCLUDE_ANCESTORS:
                excluded = True
                break
            p = p.getparent()
        if excluded:
            continue
        # store text if non-empty
        val = t(el.text)
        if val == "":
            continue
        # don't duplicate fields we handle structurally
        if ln in {"RIN"}:
            continue
        key = ln.lower()
        out[key] = val
    return out

def iter_rin_records(xml_path):
    pub_ym_file = to_pub_ym_from_file(xml_path) or ""
    ctx = etree.iterparse(xml_path, events=("end",), tag=None, recover=True, huge_tree=True)
    for ev, el in ctx:
        if localname(el.tag) == "RIN_INFO":
            rec = {}
            # base ids
            rin_node = el.find(".//*[local-name()='RIN']")
            rin = t(rin_node.text) if rin_node is not None else ""
            if not rin:
                el.clear()
                continue

            rec["rin"] = rin
            # publication (prefer tag, else filename)
            pub = extract_publication(el)
            rec.update(pub)
            if not rec.get("publication_id"):
                rec["publication_id"] = pub_ym_file
            rec["pub_season"] = pub_season(rec.get("publication_id",""))
            rec["source_xml"] = os.path.basename(xml_path)

            # Agency + Parent agency
            rec.update(extract_agency(el, "AGENCY"))
            rec.update(extract_agency(el, "PARENT_AGENCY"))

            # Lists
            rec["cfr_list"]               = as_json_str(list_texts(el, "CFR_LIST", "CFR"))
            rec["legal_authority_list"]   = as_json_str(list_texts(el, "LEGAL_AUTHORITY_LIST", "LEGAL_AUTHORITY"))
            rec["small_entity_list"]      = as_json_str(list_texts(el, "SMALL_ENTITY_LIST", "SMALL_ENTITY"))
            rec["govt_level_list"]        = as_json_str(list_texts(el, "GOVT_LEVEL_LIST", "GOVT_LEVEL"))
            rec["unfunded_mandate_list"]  = as_json_str(extract_unfunded(el))
            rec["related_rins"]           = as_json_str(extract_related_rins(el))
            rec["contacts"]               = as_json_str(extract_contacts(el))
            legal_deadlines               = extract_legal_deadlines(el)
            rec["legal_deadline_list"]    = as_json_str(legal_deadlines)
            # has_statutory_deadline flag
            has_stat = 0
            for d in legal_deadlines:
                if d.get("dline_type","").strip().lower().startswith("statutory"):
                    has_stat = 1
                    break
            rec["has_statutory_deadline"] = str(has_stat)

            # Timetable
            tt = extract_timetable(el)
            rec["timetable_json"] = as_json_str(tt)

            # Simple leaves (all scalar extras)
            simple = collect_simple_leaves(el)
            # Normalize a few common names for consistency
            # Map older variants -> modern-ish names where helpful
            # (we keep both if present)
            if "rule_title" not in simple:
                # 1995 uses RULE_TITLE; we've already normalized key to 'rule_title'
                pass
            rec.update(simple)

            yield rec
            el.clear()
            # free memory up the chain
            parent = el.getparent()
            while parent is not None and len(parent) > 0 and parent[0] is not None and parent[0].getprevious() is not None:
                try:
                    del parent[0]
                except Exception:
                    break

def load_all_xml(ua_dir):
    files = sorted([os.path.join(ua_dir, f) for f in os.listdir(ua_dir)
                    if re.match(r"REGINFO_RIN_DATA_\d{6}\.xml$", f)])
    all_rows = []
    tt_long  = []
    # dynamic union of columns
    for i, path in enumerate(files, 1):
        try:
            for rec in iter_rin_records(path):
                all_rows.append(rec)
                # explode timetable for long form
                pub = rec.get("publication_id","")
                rin = rec.get("rin","")
                src = rec.get("source_xml","")
                try:
                    tt = json.loads(rec.get("timetable_json","[]"))
                except Exception:
                    tt = []
                for item in tt:
                    tt_long.append({
                        "rin": rin,
                        "publication_id": pub,
                        "source_xml": src,
                        "ttbl_action": item.get("action",""),
                        "ttbl_date_raw": item.get("date_raw",""),
                        "ttbl_date_iso": item.get("date_iso",""),
                        "fr_citation": item.get("fr_citation","")
                    })
        except Exception as e:
            print(f"[WARN] Skipped {os.path.basename(path)}: {e}")

    if not all_rows:
        raise RuntimeError("Parsed 0 rows from UA XMLs.")
    df_rows = pd.DataFrame(all_rows)
    df_tt   = pd.DataFrame(tt_long) if tt_long else pd.DataFrame(
        columns=["rin","publication_id","source_xml","ttbl_action","ttbl_date_raw","ttbl_date_iso","fr_citation"]
    )

    # coerce publication_id as 6-digit string
    if "publication_id" in df_rows.columns:
        df_rows["publication_id"] = df_rows["publication_id"].astype(str).str.replace(r"\D","",regex=True)
    df_tt["publication_id"] = df_tt["publication_id"].astype(str).str.replace(r"\D","",regex=True)

    # ensure core cols exist
    for c in ["rin","publication_id","source_xml","pub_season",
              "agency_code","agency_name","agency_acronym",
              "parent_agency_code","parent_agency_name","parent_agency_acronym"]:
        if c not in df_rows.columns:
            df_rows[c] = ""

    return df_rows, df_tt

def to_int_ym(ym):
    try:
        s = str(ym)
        if len(s) == 6 and s.isdigit():
            return int(s)
    except Exception:
        pass
    return -1

def last_issue_ck(df_rows, df_tt):
    """
    Build CK-like 'last' table:
      - For each RIN, pick the row with the largest publication_id (YYYYMM)
      - Backfill selected descriptive fields (only if blank) from earlier issues
      - Compute latest timetable *within that last issue only*
    """
    # sort rows by publication_id ascending, then stable
    rows = df_rows.copy()
    rows["pub_int"] = rows["publication_id"].apply(to_int_ym)
    rows = rows.sort_values(["rin","pub_int"]).reset_index(drop=True)

    # identify last idx per rin
    last_idx = rows.groupby("rin")["pub_int"].idxmax()
    keep = rows.loc[last_idx].copy()
    keep = keep.sort_values(["pub_int","rin"]).reset_index(drop=True)

    # Backfill fields if last issue blank
    # Choose a set of backfillable fields (can be broad)
    cannot_bf = {"rin","publication_id","source_xml","pub_season","timetable_json"}
    backfill_cols = [c for c in rows.columns if c not in cannot_bf]

    def backfill_group(g):
        # g sorted ascending by pub_int
        bf = g[backfill_cols].ffill().iloc[-1]  # forward-fill then take last
        last = g.iloc[-1].copy()
        for c in backfill_cols:
            if t(str(last.get(c,""))) == "":
                last[c] = bf.get(c,"")
        return last

    # Build BF table per rin
    bf_parts = []
    for rin, g in rows.groupby("rin", sort=False):
        g2 = g.sort_values("pub_int")
        bf_parts.append(backfill_group(g2))
    bf_df = pd.DataFrame(bf_parts).reset_index(drop=True)

    # Ensure we align the BF result to kept last-rows by rin
    ck = keep.drop(columns=[c for c in keep.columns if c in backfill_cols], errors="ignore")\
            .merge(bf_df[["rin"] + backfill_cols], on="rin", how="left")

    # Latest timetable (only from the last issue)
    # Build quick index for last-issue timetables
    df_tt2 = df_tt.copy()
    df_tt2["pub_int"] = df_tt2["publication_id"].apply(to_int_ym)
    # find each rin's last pub_int
    last_pub = rows.groupby("rin")["pub_int"].max().reset_index().rename(columns={"pub_int":"last_pub_int"})
    tt_last = df_tt2.merge(last_pub, on="rin", how="inner")
    tt_last = tt_last[tt_last["pub_int"] == tt_last["last_pub_int"]].copy()

    # normalize date for max
    tt_last["_dt_rank"] = tt_last["ttbl_date_iso"].apply(lambda s: s if s else "")
    # pick the maximum iso date (string) per rin
    # (YYYY-MM-DD string compares lexicographically)
    agg = tt_last.sort_values(["_dt_rank","ttbl_action"]).groupby("rin").tail(1)
    latest_map_date = dict(zip(agg["rin"], agg["ttbl_date_iso"]))
    latest_map_act  = dict(zip(agg["rin"], agg["ttbl_action"]))

    # attach to ck
    ck["latest_date_in_last_issue"]   = ck["rin"].map(latest_map_date).fillna("")
    ck["latest_action_in_last_issue"] = ck["rin"].map(latest_map_act).fillna("")

    # keep also the last-issue timetable_json (not union across issues)
    last_tt_json = {}
    for _, r in keep.iterrows():
        last_tt_json[r["rin"]] = r.get("timetable_json","")
    ck["last_issue_timetable_json"] = ck["rin"].map(last_tt_json).fillna("")

    # annotate last pub fields cleanly
    ck["last_pub_ym"]   = ck["publication_id"]
    ck["last_pub_file"] = ck["source_xml"]

    # Final tidy
    ck = ck.drop(columns=["pub_int"], errors="ignore")
    return ck

def write_counts_and_plot(df_rows):
    # counts by year & season (distinct RINs per year-season)
    yr = df_rows.copy()
    yr["year"] = yr["publication_id"].str.slice(0,4)
    counts = (yr.groupby(["year","pub_season"])["rin"].nunique()
                .reset_index()
                .rename(columns={"rin":"n_rins"}))
    counts.to_csv(os.path.join(OUT_DIR,"ua_ck_counts.csv"), index=False)

    # yearly (distinct RINs by year)
    by_year = (yr.groupby("year")["rin"].nunique().reset_index())
    plt.figure(figsize=(12,5))
    plt.plot(by_year["year"], by_year["rin"], marker="o")
    plt.xticks(rotation=90)
    plt.xlabel("Year")
    plt.ylabel("Distinct RINs")
    plt.title("Unified Agenda – Distinct RINs by Publication Year")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR,"ua_yearly_counts.png"))
    plt.close()

def main():
    print(f"[INFO] UA folder: {UA_DIR}")
    print(f"[INFO] Output folder: {OUT_DIR}")
    print(f"[INFO] CK window: {CK_START} - {CK_END}")

    df_rows, df_tt = load_all_xml(UA_DIR)

    # Save per-issue rows
    path_rows = os.path.join(OUT_DIR,"ua_rows.csv")
    df_rows.to_csv(path_rows, index=False)
    print(f"[INFO] Wrote per-issue rows: {path_rows}")

    # Save timetable long
    path_tt = os.path.join(OUT_DIR,"ua_timetables.csv")
    df_tt.to_csv(path_tt, index=False)
    print(f"[INFO] Wrote timetables:    {path_tt}")

    # Build CK last
    df_ck = last_issue_ck(df_rows, df_tt)
    path_ck = os.path.join(OUT_DIR,"ua_ck_last.csv")
    df_ck.to_csv(path_ck, index=False)
    print(f"[INFO] Wrote CK-last rows:  {path_ck}")

    # Summary & chart
    print(f"[INFO] Issue-level rows parsed: {len(df_rows):,}")
    print(f"[INFO] Distinct RINs (any issue): {df_rows['rin'].nunique():,}")
    # CK window (for reference)
    rows_in_win = df_rows[(df_rows["publication_id"] >= CK_START) & (df_rows["publication_id"] <= CK_END)]
    print(f"[INFO] Distinct RINs in CK window: {rows_in_win['rin'].nunique():,}")

    write_counts_and_plot(df_rows)
    print(f"[INFO] Wrote counts & chart -> {os.path.join(OUT_DIR, 'ua_ck_counts.csv')} / ua_yearly_counts.png")

if __name__ == "__main__":
    main()
