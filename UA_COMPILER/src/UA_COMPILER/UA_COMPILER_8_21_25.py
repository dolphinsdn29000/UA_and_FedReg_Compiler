# UA_COMPILER.py
# -------------------------------------------------------------
# Unified Agenda compiler (CK-like), with dynamic column union.
# - Brings EVERY scalar leaf encountered across ALL XMLs
#   (1995..latest) into ua_rows.csv and into CK-last outputs.
# - Preserves list blocks as JSON strings in CSVs.
# - Computes per-issue timetable + "latest" within that issue.
# - Builds CK-last for the CK window (199510–201904) and a
#   "last over full range" file for all available years.
#
# Outputs under <OUT_DIR>:
#   ua_rows.csv
#   ua_timetables.csv
#   ua_ck_last.csv       (CK window 199510–201904)
#   ua_last_full.csv     (all available years, through 2024+)
#   ua_ck_counts.csv     (counts per publication_id YYYYMM)
#   ua_yearly_counts.png (Spring/Fall series)
#
# Requires: pandas, lxml, python-dateutil, matplotlib
# -------------------------------------------------------------

import os
import re
import json
from datetime import datetime
from collections import defaultdict

import pandas as pd
from lxml import etree
from dateutil import parser as dateparser

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# =======================
# 1) PATHS (Tony’s env)
# =======================
UA_DIR  = "/Users/tonymolino/Dropbox/Mac/Desktop/NEW_ML_REGULATIONS_PAPER 2/Unified_Agenda_Download/ua_main_data"
OUT_DIR = os.path.join(UA_DIR, "_ck_out")
os.makedirs(OUT_DIR, exist_ok=True)

# CK window (inclusive)
CK_START = "199510"
CK_END   = "201904"

# ---------------- Helpers ----------------
def t(x):  # safe text
    return (x or "").strip()

def localname(tag: str) -> str:
    if not tag:
        return ""
    return tag.split("}", 1)[-1]

def snake(s: str) -> str:
    """lower-snake-case for tag/field names"""
    if s is None:
        return ""
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()

# Canonical aliases so legacy names collapse to a consistent column
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

def to_pub_ym_from_file(fname):
    m = re.search(r"REGINFO_RIN_DATA_(\d{6})\.xml$", os.path.basename(fname))
    return m.group(1) if m else ""

def pub_season(ym):
    if not ym or len(ym) != 6 or not ym.isdigit():
        return ""
    return "Spring" if ym[4:] == "04" else ("Fall" if ym[4:] == "10" else "")

def parse_tt_date(raw):
    """
    UA TIMETABLE date can be 'MM/DD/YYYY', 'MM/00/YYYY', 'MM/YYYY', 'TBD', etc.
    Return (raw, iso 'YYYY-MM-DD' or '').
    """
    s = t(raw)
    if s == "" or s.lower().startswith(("to be", "tbd")):
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
        dt = dateparser.parse(s, fuzzy=True, default=datetime(1900, 1, 1))
        return s, dt.strftime("%Y-%m-%d")
    except Exception:
        return s, ""

def as_json_str(obj):
    def _default(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return str(o)
        return str(o)
    try:
        return json.dumps(obj, ensure_ascii=False, default=_default)
    except TypeError:
        return json.dumps(str(obj), ensure_ascii=False)

# ---------- Namespace-agnostic finders ----------
def iter_desc(elem):
    """Yield all descendants including self (namespace-agnostic)."""
    yield elem
    for ch in elem.iterdescendants():
        yield ch

def has_ancestor_named(el, names_lower_set):
    p = el.getparent()
    while p is not None:
        if localname(p.tag).lower() in names_lower_set:
            return True
        p = p.getparent()
    return False

def first_desc_by_name(elem, name_lower, exclude_under=None):
    """
    Return first descendant by local-name (case-insensitive).
    If exclude_under is a set of container names, skip nodes under those.
    """
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

# ---------- Structured extractors (walker-based) ----------
_LIST_CONTAINERS = {
    "agency_contact_list",
    "timetable_list",
    "legal_dline_list",
    "cfr_list",
    "legal_authority_list",
    "small_entity_list",
    "govt_level_list",
    "related_rin_list",
    "unfunded_mandate_list",
}

def extract_publication(rin_info):
    out = {}
    pub = first_desc_by_name(rin_info, "publication", exclude_under=_LIST_CONTAINERS)
    if pub is not None:
        pid = first_desc_by_name(pub, "publication_id")
        pttl = first_desc_by_name(pub, "publication_title")
        out["publication_id"] = t(pid.text) if pid is not None else ""
        out["publication_title"] = t(pttl.text) if pttl is not None else ""
    return out

def extract_agency_block(rin_info, block_name):
    """
    Extract AGENCY / PARENT_AGENCY (not those nested under CONTACTs)
    """
    out = {f"{block_name}_name": "", f"{block_name}_code": "", f"{block_name}_acronym": ""}
    blk = first_desc_by_name(rin_info, block_name, exclude_under={"agency_contact_list"})
    if blk is None:
        return out
    code = first_desc_by_name(blk, "code")
    name = first_desc_by_name(blk, "name")
    acr  = first_desc_by_name(blk, "acronym")
    out[f"{block_name}_code"]    = t(code.text) if code is not None else ""
    out[f"{block_name}_name"]    = t(name.text) if name is not None else ""
    out[f"{block_name}_acronym"] = t(acr.text)  if acr  is not None else ""
    # duplicate 'name' into shorthand 'agency' / 'parent_agency' for convenience
    if block_name == "agency":
        out["agency"] = out["agency_name"]
    if block_name == "parent_agency":
        out["parent_agency"] = out["parent_agency_name"]
    return out

def list_texts(rin_info, list_tag, item_tag):
    arr = []
    for lst in all_desc_by_name(rin_info, list_tag):
        # keep only items inside this list container
        for d in lst.iter():
            if localname(d.tag).lower() == item_tag:
                if has_ancestor_named(d, {list_tag}) and not has_ancestor_named(d, {"agency_contact_list"}):
                    arr.append(t(d.text))
    return arr

def extract_related_rins(rin_info):
    out = []
    for lst in all_desc_by_name(rin_info, "related_rin_list"):
        for rr in lst.iter():
            if localname(rr.tag).lower() == "related_rin":
                rnode = first_desc_by_name(rr, "rin")
                rel   = first_desc_by_name(rr, "rin_relation")
                out.append({
                    "rin": t(rnode.text) if rnode is not None else "",
                    "relation": t(rel.text) if rel is not None else ""
                })
    return out

def extract_contacts(rin_info):
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

def extract_unfunded(rin_info):
    arr = []
    for lst in all_desc_by_name(rin_info, "unfunded_mandate_list"):
        for u in lst.iter():
            if localname(u.tag).lower() == "unfunded_mandate":
                arr.append(t(u.text))
    return arr

def extract_legal_deadlines(rin_info):
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

def extract_timetable(rin_info):
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

# ---------- Simple leaves (all remaining scalar fields) ----------
_EXCLUDE_ANCESTOR = _LIST_CONTAINERS | {"agency", "parent_agency", "publication"}

def collect_scalar_leaves(rin_info):
    """
    Collect scalar leaf texts outside known list/agency/publication containers.
    Keys are canonicalized to lower-snake-case and alias-mapped.
    If duplicates, last one wins.
    """
    out = {}
    for el in rin_info.iter():
        if len(el) > 0:
            continue
        ln = localname(el.tag)
        if not ln:
            continue
        # skip RIN (we handle separately)
        if ln.lower() == "rin":
            continue
        if has_ancestor_named(el, _EXCLUDE_ANCESTOR):
            continue
        val = t(el.text)
        if val == "":
            continue
        key = canon(ln)
        out[key] = val
    # map commonly seen older variants if they slipped through
    if "rule_title" in out and "title" not in out:
        out["title"] = out["rule_title"]
    if "rule_stage" in out and "stage" not in out:
        out["stage"] = out["rule_stage"]
    return out

# ---------- Core record iterator ----------
def iter_rin_records(xml_path):
    pub_ym_file = to_pub_ym_from_file(xml_path)
    ctx = etree.iterparse(xml_path, events=("end",), recover=True, huge_tree=True)
    for ev, el in ctx:
        if localname(el.tag).lower() != "rin_info":
            continue

        # RIN
        rin_node = first_desc_by_name(el, "rin")
        rin = t(rin_node.text) if rin_node is not None else ""
        if not rin:
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]
            continue

        rec = {"rin": rin}
        # Publication id/title (prefer tag, else filename)
        pub = extract_publication(el)
        rec.update(pub)
        if not rec.get("publication_id"):
            rec["publication_id"] = pub_ym_file
        rec["pub_season"] = pub_season(rec["publication_id"])
        rec["source_xml"] = os.path.basename(xml_path)

        # Agency & Parent agency
        rec.update(extract_agency_block(el, "agency"))
        rec.update(extract_agency_block(el, "parent_agency"))

        # Lists
        rec["cfr_list"]              = as_json_str(list_texts(el, "cfr_list", "cfr"))
        rec["legal_authority_list"]  = as_json_str(list_texts(el, "legal_authority_list", "legal_authority"))
        rec["small_entity_list"]     = as_json_str(list_texts(el, "small_entity_list", "small_entity"))
        rec["govt_level_list"]       = as_json_str(list_texts(el, "govt_level_list", "govt_level"))
        rec["unfunded_mandate_list"] = as_json_str(extract_unfunded(el))
        rec["related_rins"]          = as_json_str(extract_related_rins(el))
        rec["contacts"]              = as_json_str(extract_contacts(el))

        # Legal deadlines (+ flag)
        legal_deadlines = extract_legal_deadlines(el)
        rec["legal_deadline_list"] = as_json_str(legal_deadlines)
        has_stat = 0
        for d in legal_deadlines:
            if t(d.get("dline_type","")).lower().startswith("statutory"):
                has_stat = 1
                break
        rec["has_statutory_deadline"] = str(has_stat)

        # Timetable (issue-only) + per-issue latest
        tts = extract_timetable(el)
        rec["timetable_all"] = as_json_str(tts)
        # latest within the same issue
        latest_iso, latest_action = "", ""
        if tts:
            # pick max date_iso; break ties by action alpha to stabilize
            tts_sorted = sorted(tts, key=lambda d: (d.get("date_iso",""), d.get("action","")))
            latest_iso    = tts_sorted[-1].get("date_iso","") or ""
            latest_action = tts_sorted[-1].get("action","")    or ""
        rec["latest_action_date_in_issue"] = latest_iso
        rec["latest_action_in_issue"]      = latest_action

        # Scalars (everything else)
        rec.update(collect_scalar_leaves(el))

        yield rec

        # Free memory for streaming
        el.clear()
        parent = el.getparent()
        while parent is not None and parent.getprevious() is not None:
            try:
                del parent.getparent()[0]
            except Exception:
                break

# ---------- Load all XMLs ----------
def load_all_xml(ua_dir):
    files = sorted([
        os.path.join(ua_dir, f)
        for f in os.listdir(ua_dir)
        if re.match(r"REGINFO_RIN_DATA_\d{6}\.xml$", f)
    ])
    all_rows = []
    tt_long  = []
    for i, path in enumerate(files, 1):
        try:
            for rec in iter_rin_records(path):
                all_rows.append(rec)
                # explode timetable for long table
                pub = rec.get("publication_id","")
                rin = rec.get("rin","")
                src = rec.get("source_xml","")
                try:
                    tt = json.loads(rec.get("timetable_all","[]"))
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
                        "fr_citation": item.get("fr_citation",""),
                    })
        except Exception as e:
            print(f"[WARN] Skipped {os.path.basename(path)}: {e}")

    if not all_rows:
        raise RuntimeError("Parsed 0 rows from UA XMLs.")

    df_rows = pd.DataFrame(all_rows)
    df_tt   = pd.DataFrame(tt_long) if tt_long else pd.DataFrame(
        columns=["rin","publication_id","source_xml","ttbl_action","ttbl_date_raw","ttbl_date_iso","fr_citation"]
    )

    # Normalize id columns
    df_rows["publication_id"] = df_rows["publication_id"].astype(str).str.replace(r"\D","",regex=True)
    if "rin" in df_rows.columns:
        df_rows["rin"] = df_rows["rin"].astype(str).str.strip()
    if not df_tt.empty:
        df_tt["publication_id"] = df_tt["publication_id"].astype(str).str.replace(r"\D","",regex=True)
        df_tt["rin"] = df_tt["rin"].astype(str).str.strip()

    # Ensure core columns exist no matter what (dynamic union preserved)
    for c in ["agency","parent_agency","pub_season","source_xml",
              "agency_name","agency_code","agency_acronym",
              "parent_agency_name","parent_agency_code","parent_agency_acronym",
              "latest_action_in_issue","latest_action_date_in_issue","timetable_all"]:
        if c not in df_rows.columns:
            df_rows[c] = ""

    return df_rows, df_tt

def to_int_ym(ym):
    s = str(ym)
    return int(s) if len(s) == 6 and s.isdigit() else -1

# ---------- Build CK-style "last" ----------
def build_last(df_rows, df_tt, start_ym=None, end_ym=None):
    """
    If start_ym & end_ym provided, restrict to that window; else use all rows.
    Backfill **only scalar** fields; keep last issue's timetable & latest.
    Return df with:
      - last_pub_ym, source_xml_of_last
      - timetable_all_last_issue
      - latest_action_last_issue, latest_action_date_last_issue
      - PLUS the full union of scalar columns (dynamic)
    """
    if start_ym and end_ym:
        rows = df_rows[(df_rows["publication_id"] >= start_ym) &
                       (df_rows["publication_id"] <= end_ym)].copy()
    else:
        rows = df_rows.copy()

    if rows.empty:
        return rows.head(0)

    rows["pub_int"] = rows["publication_id"].apply(to_int_ym)
    rows = rows.sort_values(["rin","pub_int","publication_id"]).reset_index(drop=True)

    # Identify last row per RIN in (window or full)
    last_idx = rows.groupby("rin")["pub_int"].idxmax()
    last_rows = rows.loc[last_idx].copy()

    # Backfill only scalar columns:
    # Non-backfillable identifiers / list-like fields
    non_bf = {"rin","publication_id","pub_season","source_xml",
              "timetable_all","latest_action_in_issue","latest_action_date_in_issue"}
    # heuristics: any column ending with '_list' or named 'contacts' or 'related_rins' are list-like
    for c in list(rows.columns):
        if c.endswith("_list") or c in {"contacts","related_rins"}:
            non_bf.add(c)

    backfill_cols = [c for c in rows.columns if c not in non_bf | {"pub_int"}]

    # Build per-RIN backfilled scalars
    parts = []
    for rin, g in rows.groupby("rin", sort=False):
        g2 = g.sort_values("pub_int")
        # forward-fill across issues, take last
        bf = g2[backfill_cols].ffill().iloc[-1]
        last = g2.iloc[-1].copy()
        for c in backfill_cols:
            if t(str(last.get(c,""))) == "":
                last[c] = bf.get(c,"")
        parts.append(last)

    bf_df = pd.DataFrame(parts).reset_index(drop=True)

    # Compose CK/full-last by overwriting last_rows' scalar cols with backfilled values
    result = last_rows.drop(columns=[c for c in last_rows.columns if c in backfill_cols], errors="ignore") \
                      .merge(bf_df[["rin"] + backfill_cols], on="rin", how="left")

    # Attach last-issue timetable & latest
    last_tt_json = dict(zip(last_rows["rin"], last_rows["timetable_all"]))
    result["timetable_all_last_issue"] = result["rin"].map(last_tt_json).fillna("")

    result["latest_action_last_issue"]       = result["rin"].map(dict(zip(last_rows["rin"], last_rows["latest_action_in_issue"]))).fillna("")
    result["latest_action_date_last_issue"]  = result["rin"].map(dict(zip(last_rows["rin"], last_rows["latest_action_date_in_issue"]))).fillna("")

    # Rename and tidy ids
    result["last_pub_ym"]        = result["publication_id"]
    result["source_xml_of_last"] = result["source_xml"]
    result = result.drop(columns=["pub_int"], errors="ignore")

    # Reindex to ensure we keep the **full** union of columns (rows.columns union + new last* fields)
    union_cols = list(sorted(set(rows.columns) | set(result.columns) |
                             {"last_pub_ym","source_xml_of_last",
                              "timetable_all_last_issue",
                              "latest_action_last_issue","latest_action_date_last_issue"}))
    result = result.reindex(columns=union_cols)
    return result

# ---------- Counts & plot ----------
def write_counts_and_plot(df_rows):
    # per publication_id
    counts = df_rows.groupby("publication_id")["rin"].nunique().reset_index(name="n_rins")
    counts = counts.sort_values("publication_id")
    counts.to_csv(os.path.join(OUT_DIR, "ua_ck_counts.csv"), index=False)

    # yearly Spring/Fall series
    yr = df_rows.copy()
    yr["year"] = yr["publication_id"].str.slice(0,4)
    sf = yr.groupby(["year","pub_season"])["rin"].nunique().reset_index(name="n_rins")
    pivot = sf.pivot(index="year", columns="pub_season", values="n_rins").fillna(0)
    plt.figure(figsize=(12,5))
    if "Spring" in pivot.columns:
        plt.plot(pivot.index, pivot["Spring"], marker="o", label="Spring")
    if "Fall" in pivot.columns:
        plt.plot(pivot.index, pivot["Fall"], marker="o", label="Fall")
    plt.xticks(rotation=90)
    plt.xlabel("Year")
    plt.ylabel("Distinct RINs")
    plt.title("Unified Agenda – Distinct RINs by Publication Year (Spring/Fall)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, "ua_yearly_counts.png"))
    plt.close()

def main():
    print(f"[INFO] UA folder:   {UA_DIR}")
    print(f"[INFO] Output dir:  {OUT_DIR}")
    print(f"[INFO] CK window:   {CK_START}–{CK_END}")

    df_rows, df_tt = load_all_xml(UA_DIR)

    # Save per-issue (dynamic union preserved automatically)
    path_rows = os.path.join(OUT_DIR, "ua_rows.csv")
    df_rows.to_csv(path_rows, index=False)
    print(f"[INFO] Wrote ua_rows.csv: {len(df_rows):,} rows -> {path_rows}")

    # Save timetable long
    path_tt = os.path.join(OUT_DIR, "ua_timetables.csv")
    df_tt.to_csv(path_tt, index=False)
    print(f"[INFO] Wrote ua_timetables.csv: {len(df_tt):,} rows -> {path_tt}")

    # CK-last (window 199510–201904)
    df_ck = build_last(df_rows, df_tt, CK_START, CK_END)
    path_ck = os.path.join(OUT_DIR, "ua_ck_last.csv")
    df_ck.to_csv(path_ck, index=False)
    print(f"[INFO] Wrote CK-last (window): {len(df_ck):,} rows -> {path_ck}")

    # Full-range last (for 2020–2024+ assessment)
    df_full = build_last(df_rows, df_tt, None, None)
    path_full = os.path.join(OUT_DIR, "ua_last_full.csv")
    df_full.to_csv(path_full, index=False)
    print(f"[INFO] Wrote last-over-full-range: {len(df_full):,} rows -> {path_full}")

    # Quick stats
    print(f"[INFO] Distinct RINs overall: {df_rows['rin'].nunique():,}")
    in_ck = df_rows[(df_rows["publication_id"] >= CK_START) & (df_rows["publication_id"] <= CK_END)]
    print(f"[INFO] Distinct RINs in CK window: {in_ck['rin'].nunique():,}")

    write_counts_and_plot(df_rows)
    print(f"[INFO] Counts & plot -> {os.path.join(OUT_DIR,'ua_ck_counts.csv')} / ua_yearly_counts.png")

if __name__ == "__main__":
    main()
