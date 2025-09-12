# UA_Parser_For_Single_xml_9_12_25.py
# -------------------------------------------------------------
# Parse a single Unified Agenda XML (1995..2018+ variants) and
# write a flat CSV.
#
# - Handles both the 1995 tree and 2018-era additions automatically.
# - Uses lxml.etree(iterparse, recover=True) when available
#   (robust to slightly malformed XML); otherwise falls back to
#   xml.etree.ElementTree (strict).
# - Preserves list families as JSON strings.
# - Adds Latest_Action and latest_action_date from the latest timetable date.
# - ALWAYS emits a superset schema: 1995 baseline + 2018 adds.
#   Any extra unknown top-level scalar children are also captured as columns.
# -------------------------------------------------------------
"""
Unified Agenda (UA) Single‑XML Parser — WHY this exists and WHY it’s built this way
===================================================================================

Why parse UA XMLs at all?
-------------------------
• Research and replication: UA schemas drift across years (1995 → 2018+). If we rely on
  hand‑picked fields or a single vintage, we silently drop information and our derived
  tables (counts, stage transitions, timetables) become irreproducible.
• CK‑style analytics need a *complete and consistent* issue‑level substrate before
  computing last‑appearance logic per RIN.

Why lxml.iterparse(recover=True) with a namespace‑agnostic walker?
------------------------------------------------------------------
• Old UA files have quirks (namespaces, stray CDATA/HTML, occasional tag mismatches).
  `lxml.etree.iterparse(..., recover=True)` streams memory‑safely and *keeps going*
  through minor defects. The traversal uses local‑name comparisons (not XPath predicates)
  so we don’t crash on vintage‑specific namespaces or tag prefixes.
• If lxml isn’t available, we fall back to stdlib `xml.etree.ElementTree` (strict), so
  the module still runs—just with less resilience.

Why keep a superset schema (1995 baseline + 2018 additions) every time?
-----------------------------------------------------------------------
• UA columns appear/disappear across vintages. Our downstream consumers need a stable
  CSV shape. We always emit the superset (e.g., MAJOR / EO_13771_DESIGNATION / FEDERALISM
  are present for 1995 files but blank), so joins and validation never change shape run to run.

Why “dynamic union” of any extra top‑level scalars?
---------------------------------------------------
• Even with a curated superset, some files carry extra simple fields (top‑level scalars).
  We auto‑detect unknown top‑level leaves and promote them to columns so we never lose data.

Why lists are JSON strings (not exploded columns)?
--------------------------------------------------
• Families like CFR_LIST, LEGAL_AUTHORITY_LIST, RELATED_RIN_LIST, CONTACTS, and the
  TIMETABLE_LIST are inherently 1‑to‑many. Flattening into wide columns or pivoting
  breaks when cardinality varies across files. We serialize each list as a JSON string:
  - preserves full fidelity for audits,
  - keeps a single row per (RIN, issue),
  - remains easy to explode later when needed.

Why Latest_Action and latest_action_date the way we compute them?
-----------------------------------------------------------------
• We derive “latest” *within the same issue* by parsing TTBL_DATE to ISO and
  choosing the max ISO date. This fixes a common bug where “latest” was computed
  across all issues for a RIN (leaking future entries into past issues).
• If dates are imprecise (“MM/00/YYYY”, “MM/YYYY”, “To Be Determined”), we map to a
  safe month anchor or leave blank—never guess specific days.

Why avoid XPath with local‑name() predicates?
---------------------------------------------
• Python’s ElementPath is limited; complex local‑name predicates often throw
  “Invalid predicate” on older trees. A namespace‑agnostic *walker* (compare local names)
  is robust across all vintages.

What guarantees does this file make?
------------------------------------
1) Every row = one RIN entry for one publication issue (publication_id = YYYYMM).
2) Columns = stable superset + any top‑level extras found in the file.
3) Lists are JSON strings; scalars are plain text; timetable dates normalized when possible.
4) Parser streams and frees memory—safe on very large files.

When should I change this file?
-------------------------------
• Only to add new known groups/fields that should be extracted as explicit scalars,
  or to extend date parsing rules. Schema *shape* should remain stable.

"""

import os
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Set

import pandas as pd

# Prefer lxml (robust); fallback to stdlib ET
try:
    from lxml import etree as _ET  # type: ignore
    _HAVE_LXML = True
except Exception:
    import xml.etree.ElementTree as _ET  # type: ignore
    _HAVE_LXML = False


# ----------- Helpers -----------
def _t(x: str) -> str:
    return (x or "").strip()

def _lname(tag: str) -> str:
    if tag is None:
        return ""
    return tag.split("}")[-1]  # strip namespace if present

def _child(elem, name: str):
    for ch in elem:
        if _lname(ch.tag) == name:
            return ch
    return None

def _children(elem, name: str) -> List:
    return [ch for ch in elem if _lname(ch.tag) == name]

def _text_child(elem, name: str) -> str:
    c = _child(elem, name)
    return _t(c.text) if c is not None else ""

def _parse_tt_date(raw: str) -> str:
    """Return ISO YYYY-MM-DD when possible; else ''."""
    s = _t(raw)
    if not s or s.lower().startswith("to be"):
        return ""
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12:
            if dd == 0:
                dd = 1
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        return ""
    m2 = re.fullmatch(r"(\d{1,2})/00/(\d{4})", s)
    if m2:
        mm, yyyy = int(m2.group(1)), int(m2.group(2))
        if 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-01"
        return ""
    m3 = re.fullmatch(r"(\d{1,2})/(\d{4})", s)
    if m3:
        mm, yyyy = int(m3.group(1)), int(m3.group(2))
        if 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-01"
        return ""
    try:
        dt = datetime.strptime(s, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


# Known top-level tags we handle explicitly (1995 + 2018 adds)
_KNOWN_GROUPS_L1: Set[str] = {
    # identity / agencies
    "RIN", "PUBLICATION", "AGENCY", "PARENT_AGENCY",
    # descriptors
    "RULE_TITLE", "ABSTRACT", "PRIORITY_CATEGORY", "RIN_STATUS", "RULE_STAGE",
    # 2018-era scalars
    "MAJOR", "EO_13771_DESIGNATION", "FEDERALISM",
    # lists and groups
    "UNFUNDED_MANDATE_LIST", "CFR_LIST", "LEGAL_AUTHORITY_LIST", "LEGAL_DLINE_LIST",
    "RPLAN_ENTRY", "RPLAN_INFO", "TIMETABLE_LIST",
    "RFA_REQUIRED", "SMALL_ENTITY_LIST", "GOVT_LEVEL_LIST",
    "PRINT_PAPER", "INTERNATIONAL_INTEREST",
    "RELATED_RIN_LIST", "CHILD_RIN_LIST", "AGENCY_CONTACT_LIST",
    "REINVENT_GOVT", "ADDITIONAL_INFO", "PROCUREMENT", "SIC_DESC", "PARENT_RIN",
    "COMPLIANCE_COST",
}

# Stable superset order: 1995 baseline + 2018 adds + derived
_PREFERRED_ORDER = [
    "RIN",
    "PUBLICATION_ID", "PUBLICATION_TITLE",
    "AGENCY_CODE", "AGENCY_NAME", "AGENCY_ACRONYM",
    "PARENT_AGENCY_CODE", "PARENT_AGENCY_NAME", "PARENT_AGENCY_ACRONYM",
    "RULE_TITLE", "ABSTRACT", "PRIORITY_CATEGORY", "RIN_STATUS", "RULE_STAGE",
    # 2018-era scalars (remain blank in 1995)
    "MAJOR", "EO_13771_DESIGNATION", "FEDERALISM",
    # lists / groups
    "UNFUNDED_MANDATE_LIST",
    "CFR_LIST", "LEGAL_AUTHORITY_LIST", "LEGAL_DLINE_LIST",
    "RPLAN_ENTRY",
    "RPLAN_INFO_STMT_OF_NEED", "RPLAN_INFO_LEGAL_BASIS",
    "RPLAN_INFO_ALTERNATIVES", "RPLAN_INFO_COSTS_AND_BENEFITS", "RPLAN_INFO_RISKS",
    "TIMETABLE_LIST",
    "Latest_Action", "latest_action_date",
    "RFA_REQUIRED",
    "SMALL_ENTITY_LIST", "GOVT_LEVEL_LIST",
    "PRINT_PAPER", "INTERNATIONAL_INTEREST",
    "RELATED_RIN_LIST", "CHILD_RIN_LIST",
    "AGENCY_CONTACT_LIST",
    "REINVENT_GOVT", "ADDITIONAL_INFO", "PROCUREMENT", "SIC_DESC", "PARENT_RIN",
    "COMPLIANCE_COST_BASE_YEAR", "COMPLIANCE_COST_INITIAL_PUBLIC_COST", "COMPLIANCE_COST_RECURRING_PUBLIC_COST",
]


def _parse_rin_info(ri) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # Identity
    out["RIN"] = _text_child(ri, "RIN")

    pub = _child(ri, "PUBLICATION")
    out["PUBLICATION_ID"] = _text_child(pub, "PUBLICATION_ID") if pub is not None else ""
    out["PUBLICATION_TITLE"] = _text_child(pub, "PUBLICATION_TITLE") if pub is not None else ""

    ag = _child(ri, "AGENCY")
    out["AGENCY_CODE"] = _text_child(ag, "CODE") if ag is not None else ""
    out["AGENCY_NAME"] = _text_child(ag, "NAME") if ag is not None else ""
    out["AGENCY_ACRONYM"] = _text_child(ag, "ACRONYM") if ag is not None else ""

    pag = _child(ri, "PARENT_AGENCY")
    out["PARENT_AGENCY_CODE"] = _text_child(pag, "CODE") if pag is not None else ""
    out["PARENT_AGENCY_NAME"] = _text_child(pag, "NAME") if pag is not None else ""
    out["PARENT_AGENCY_ACRONYM"] = _text_child(pag, "ACRONYM") if pag is not None else ""

    # Descriptors
    out["RULE_TITLE"] = _text_child(ri, "RULE_TITLE")
    out["ABSTRACT"] = _text_child(ri, "ABSTRACT")
    out["PRIORITY_CATEGORY"] = _text_child(ri, "PRIORITY_CATEGORY")
    out["RIN_STATUS"] = _text_child(ri, "RIN_STATUS")
    out["RULE_STAGE"] = _text_child(ri, "RULE_STAGE")

    # 2018-era scalar additions
    out["MAJOR"] = _text_child(ri, "MAJOR")
    out["EO_13771_DESIGNATION"] = _text_child(ri, "EO_13771_DESIGNATION")
    out["FEDERALISM"] = _text_child(ri, "FEDERALISM")

    # Lists
    um_list = []
    um_parent = _child(ri, "UNFUNDED_MANDATE_LIST")
    if um_parent is not None:
        for it in _children(um_parent, "UNFUNDED_MANDATE"):
            um_list.append(_t(it.text))
    out["UNFUNDED_MANDATE_LIST"] = _json(um_list)

    cfr_list = []
    cfr_parent = _child(ri, "CFR_LIST")
    if cfr_parent is not None:
        for it in _children(cfr_parent, "CFR"):
            cfr_list.append(_t(it.text))
    out["CFR_LIST"] = _json(cfr_list)

    la_list = []
    la_parent = _child(ri, "LEGAL_AUTHORITY_LIST")
    if la_parent is not None:
        for it in _children(la_parent, "LEGAL_AUTHORITY"):
            la_list.append(_t(it.text))
    out["LEGAL_AUTHORITY_LIST"] = _json(la_list)

    dline_list = []
    dline_parent = _child(ri, "LEGAL_DLINE_LIST")
    if dline_parent is not None:
        for li in _children(dline_parent, "LEGAL_DLINE_INFO"):
            dline_list.append({
                "DLINE_TYPE": _text_child(li, "DLINE_TYPE"),
                "DLINE_ACTION_STAGE": _text_child(li, "DLINE_ACTION_STAGE"),
                "DLINE_DATE": _text_child(li, "DLINE_DATE"),
                "DLINE_DESC": _text_child(li, "DLINE_DESC"),
            })
    out["LEGAL_DLINE_LIST"] = _json(dline_list)

    out["RPLAN_ENTRY"] = _text_child(ri, "RPLAN_ENTRY")

    rplan = _child(ri, "RPLAN_INFO")
    out["RPLAN_INFO_STMT_OF_NEED"]       = _text_child(rplan, "STMT_OF_NEED") if rplan is not None else ""
    out["RPLAN_INFO_LEGAL_BASIS"]        = _text_child(rplan, "LEGAL_BASIS")  if rplan is not None else ""
    out["RPLAN_INFO_ALTERNATIVES"]       = _text_child(rplan, "ALTERNATIVES") if rplan is not None else ""
    out["RPLAN_INFO_COSTS_AND_BENEFITS"] = _text_child(rplan, "COSTS_AND_BENEFITS") if rplan is not None else ""
    out["RPLAN_INFO_RISKS"]              = _text_child(rplan, "RISKS") if rplan is not None else ""

    # Timetable + latest
    tt_list = []
    tt_parent = _child(ri, "TIMETABLE_LIST")
    if tt_parent is not None:
        for tt in _children(tt_parent, "TIMETABLE"):
            act = _text_child(tt, "TTBL_ACTION")
            dt_raw = _text_child(tt, "TTBL_DATE")
            fr = _text_child(tt, "FR_CITATION")
            tt_list.append({
                "TTBL_ACTION": act,
                "TTBL_DATE": dt_raw,
                "TTBL_DATE_ISO": _parse_tt_date(dt_raw),
                "FR_CITATION": fr
            })
    out["TIMETABLE_LIST"] = _json(tt_list)

    latest_iso, latest_act = "", ""
    if tt_list:
        with_iso = [it for it in tt_list if it.get("TTBL_DATE_ISO")]
        if with_iso:
            with_iso.sort(key=lambda x: x["TTBL_DATE_ISO"])
            latest = with_iso[-1]
            latest_iso = latest["TTBL_DATE_ISO"]
            latest_act = latest["TTBL_ACTION"]
    out["Latest_Action"] = latest_act
    out["latest_action_date"] = latest_iso

    # Misc scalars
    out["RFA_REQUIRED"] = _text_child(ri, "RFA_REQUIRED")

    se_list = []
    se_parent = _child(ri, "SMALL_ENTITY_LIST")
    if se_parent is not None:
        for it in _children(se_parent, "SMALL_ENTITY"):
            se_list.append(_t(it.text))
    out["SMALL_ENTITY_LIST"] = _json(se_list)

    gv_list = []
    gv_parent = _child(ri, "GOVT_LEVEL_LIST")
    if gv_parent is not None:
        for it in _children(gv_parent, "GOVT_LEVEL"):
            gv_list.append(_t(it.text))
    out["GOVT_LEVEL_LIST"] = _json(gv_list)

    out["PRINT_PAPER"]            = _text_child(ri, "PRINT_PAPER")
    out["INTERNATIONAL_INTEREST"] = _text_child(ri, "INTERNATIONAL_INTEREST")

    rel_list = []
    rel_parent = _child(ri, "RELATED_RIN_LIST")
    if rel_parent is not None:
        for it in _children(rel_parent, "RELATED_RIN"):
            rel_list.append({
                "RIN": _text_child(it, "RIN"),
                "RIN_RELATION": _text_child(it, "RIN_RELATION")
            })
    out["RELATED_RIN_LIST"] = _json(rel_list)

    child_list = []
    child_parent = _child(ri, "CHILD_RIN_LIST")
    if child_parent is not None:
        for it in _children(child_parent, "CHILD_RIN"):
            child_list.append({
                "RIN": _text_child(it, "RIN"),
                "RULE_TITLE": _text_child(it, "RULE_TITLE")
            })
    out["CHILD_RIN_LIST"] = _json(child_list)

    contact_list = []
    contact_parent = _child(ri, "AGENCY_CONTACT_LIST")
    if contact_parent is not None:
        for ct in _children(contact_parent, "CONTACT"):
            ct_d = {
                "PREFIX": _text_child(ct, "PREFIX"),
                "FIRST_NAME": _text_child(ct, "FIRST_NAME"),
                "MIDDLE_NAME": _text_child(ct, "MIDDLE_NAME"),
                "LAST_NAME": _text_child(ct, "LAST_NAME"),
                "SUFFIX": _text_child(ct, "SUFFIX"),
                "TITLE": _text_child(ct, "TITLE"),
                "PHONE": _text_child(ct, "PHONE"),
                "PHONE_EXT": _text_child(ct, "PHONE_EXT"),
                "TDD_PHONE": _text_child(ct, "TDD_PHONE"),
                "FAX": _text_child(ct, "FAX"),
                "EMAIL": _text_child(ct, "EMAIL"),
            }
            ctag = _child(ct, "AGENCY")
            if ctag is not None:
                ct_d["AGENCY"] = {
                    "CODE": _text_child(ctag, "CODE"),
                    "NAME": _text_child(ctag, "NAME"),
                    "ACRONYM": _text_child(ctag, "ACRONYM"),
                }
            addr = _child(ct, "MAILING_ADDRESS")
            if addr is not None:
                ct_d["MAILING_ADDRESS"] = {
                    "STREET_ADDRESS": _text_child(addr, "STREET_ADDRESS"),
                    "CITY": _text_child(addr, "CITY"),
                    "STATE": _text_child(addr, "STATE"),
                    "ZIP": _text_child(addr, "ZIP"),
                }
            contact_list.append(ct_d)
    out["AGENCY_CONTACT_LIST"] = _json(contact_list)

    out["REINVENT_GOVT"]  = _text_child(ri, "REINVENT_GOVT")
    out["ADDITIONAL_INFO"] = _text_child(ri, "ADDITIONAL_INFO")
    out["PROCUREMENT"]    = _text_child(ri, "PROCUREMENT")
    out["SIC_DESC"]       = _text_child(ri, "SIC_DESC")
    out["PARENT_RIN"]     = _text_child(ri, "PARENT_RIN")

    cc = _child(ri, "COMPLIANCE_COST")
    out["COMPLIANCE_COST_BASE_YEAR"]          = _text_child(cc, "BASE_YEAR") if cc is not None else ""
    out["COMPLIANCE_COST_INITIAL_PUBLIC_COST"] = _text_child(cc, "INITIAL_PUBLIC_COST") if cc is not None else ""
    out["COMPLIANCE_COST_RECURRING_PUBLIC_COST"] = _text_child(cc, "RECURRING_PUBLIC_COST") if cc is not None else ""

    # Auto-capture any extra simple scalar top-level children
    known_l1 = set(_KNOWN_GROUPS_L1)
    for ch in ri:
        tag = _lname(ch.tag)
        if tag in known_l1:
            continue
        if len(ch) == 0:  # not a container
            key = tag.upper()
            if key not in out:
                out[key] = _t(ch.text)

    return out


def _iter_rin_infos(xml_path: str):
    """Yield each RIN_INFO element (robust when lxml is available)."""
    if _HAVE_LXML:
        ctx = _ET.iterparse(xml_path, events=("end",), recover=True, huge_tree=True)
        for ev, el in ctx:
            if _lname(el.tag) == "RIN_INFO":
                yield el
                # memory hygiene (lxml API)
                el.clear()
                parent = el.getparent()
                while parent is not None and parent.getprevious() is not None:
                    try:
                        del parent.getparent()[0]
                    except Exception:
                        break
    else:
        # stdlib ET is strict; if the file is malformed this will raise ParseError
        ctx = _ET.iterparse(xml_path, events=("end",))
        for ev, el in ctx:
            if _lname(el.tag) == "RIN_INFO":
                yield el
                el.clear()


# ----------- Public function -----------
def build_ua_csv_from_xml(xml_path: str, out_dir: str):
    """
    Parse a single UA XML and write a CSV with:
      - 1995 schema + 2018 additions,
      - Latest_Action and latest_action_date,
      - Any extra scalar top-level fields in the file (appended at end).
    Returns (csv_path, pandas.DataFrame).
    """
    if not os.path.isfile(xml_path):
        raise FileNotFoundError(f"XML not found: {xml_path}")
    os.makedirs(out_dir, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    try:
        for el in _iter_rin_infos(xml_path):
            rows.append(_parse_rin_info(el))
    except _ET.ParseError as e:
        raise RuntimeError(
            f"XML parse failed at {xml_path}: {e}. "
            "Install lxml (pip install lxml) for robust recovery parsing."
        )

    # Build column set = full preferred order (always) + any extras observed
    all_keys: Set[str] = set()
    for r in rows:
        all_keys.update(r.keys())

    columns = list(_PREFERRED_ORDER)  # always include superset
    extras = sorted(k for k in all_keys if k not in set(columns))
    columns += extras

    df = pd.DataFrame(rows)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    df = df[columns]

    base = os.path.basename(xml_path).replace(".xml", "_flat.csv")
    out_csv = os.path.join(out_dir, base)
    df.to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv, df
