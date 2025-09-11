
# ua_single_xml_to_csv.py
# -------------------------------------------------------------
# Parse a single Unified Agenda XML (e.g., REGINFO_RIN_DATA_199510.xml)
# and write a flat CSV with ALL columns listed in the Fall 1995 tree.
#
# Lists are preserved as JSON strings. Special columns:
#   - Latest_Action
#   - latest_action_date (normalized YYYY-MM-DD when possible)
# -------------------------------------------------------------

import os
import json
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from typing import List, Dict, Any

import pandas as pd


# ----------- Helpers -----------
def _t(x: str) -> str:
    return (x or "").strip()

def _lname(tag: str) -> str:
    if tag is None:
        return ""
    return tag.split("}")[-1]

def _child(elem: ET.Element, name: str) -> ET.Element:
    for ch in elem:
        if _lname(ch.tag) == name:
            return ch
    return None

def _children(elem: ET.Element, name: str) -> List[ET.Element]:
    return [ch for ch in elem if _lname(ch.tag) == name]

def _text_child(elem: ET.Element, name: str) -> str:
    c = _child(elem, name)
    return _t(c.text) if c is not None else ""

def _parse_tt_date(raw: str) -> str:
    """Return ISO YYYY-MM-DD when possible; else ''.
       Rules:
         - MM/DD/YYYY -> YYYY-MM-DD
         - MM/00/YYYY -> YYYY-MM-01
         - MM/YYYY    -> YYYY-MM-01
         - 'To Be Determined' or empty -> ''
    """
    s = _t(raw)
    if not s or s.lower().startswith("to be"):
        return ""
    # MM/DD/YYYY
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12:
            if dd == 0:
                dd = 1
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        return ""
    # MM/00/YYYY or MM/YYYY
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
    # Try loose parse (rare)
    try:
        dt = datetime.strptime(s, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


# ----------- Core parse for one RIN_INFO -----------
def _parse_rin_info(ri: ET.Element) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # Scalars
    out["RIN"]                 = _text_child(ri, "RIN")

    # PUBLICATION
    pub = _child(ri, "PUBLICATION")
    out["PUBLICATION_ID"]      = _text_child(pub, "PUBLICATION_ID") if pub is not None else ""
    out["PUBLICATION_TITLE"]   = _text_child(pub, "PUBLICATION_TITLE") if pub is not None else ""

    # AGENCY
    ag = _child(ri, "AGENCY")
    out["AGENCY_CODE"]         = _text_child(ag, "CODE") if ag is not None else ""
    out["AGENCY_NAME"]         = _text_child(ag, "NAME") if ag is not None else ""
    out["AGENCY_ACRONYM"]      = _text_child(ag, "ACRONYM") if ag is not None else ""

    # PARENT_AGENCY (optional)
    pag = _child(ri, "PARENT_AGENCY")
    out["PARENT_AGENCY_CODE"]    = _text_child(pag, "CODE") if pag is not None else ""
    out["PARENT_AGENCY_NAME"]    = _text_child(pag, "NAME") if pag is not None else ""
    out["PARENT_AGENCY_ACRONYM"] = _text_child(pag, "ACRONYM") if pag is not None else ""

    # Rule descriptors
    out["RULE_TITLE"]          = _text_child(ri, "RULE_TITLE")
    out["ABSTRACT"]            = _text_child(ri, "ABSTRACT")
    out["PRIORITY_CATEGORY"]   = _text_child(ri, "PRIORITY_CATEGORY")
    out["RIN_STATUS"]          = _text_child(ri, "RIN_STATUS")
    out["RULE_STAGE"]          = _text_child(ri, "RULE_STAGE")

    # CFR_LIST (list of CFR strings)
    cfr_list = []
    cfr_parent = _child(ri, "CFR_LIST")
    if cfr_parent is not None:
        for it in _children(cfr_parent, "CFR"):
            cfr_list.append(_t(it.text))
    out["CFR_LIST"] = _json(cfr_list)

    # LEGAL_AUTHORITY_LIST (list of strings)
    la_list = []
    la_parent = _child(ri, "LEGAL_AUTHORITY_LIST")
    if la_parent is not None:
        for it in _children(la_parent, "LEGAL_AUTHORITY"):
            la_list.append(_t(it.text))
    out["LEGAL_AUTHORITY_LIST"] = _json(la_list)

    # LEGAL_DLINE_LIST (list of objects)
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

    # RPLAN_ENTRY (scalar)
    out["RPLAN_ENTRY"] = _text_child(ri, "RPLAN_ENTRY")

    # RPLAN_INFO (optional scalar subfields)
    rplan = _child(ri, "RPLAN_INFO")
    out["RPLAN_INFO_STMT_OF_NEED"]        = _text_child(rplan, "STMT_OF_NEED") if rplan is not None else ""
    out["RPLAN_INFO_LEGAL_BASIS"]         = _text_child(rplan, "LEGAL_BASIS")  if rplan is not None else ""
    out["RPLAN_INFO_ALTERNATIVES"]        = _text_child(rplan, "ALTERNATIVES") if rplan is not None else ""
    out["RPLAN_INFO_COSTS_AND_BENEFITS"]  = _text_child(rplan, "COSTS_AND_BENEFITS") if rplan is not None else ""
    out["RPLAN_INFO_RISKS"]               = _text_child(rplan, "RISKS") if rplan is not None else ""

    # TIMETABLE_LIST (list of objects) + Latest_Action / latest_action_date
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

    # derive Latest_Action and latest_action_date
    latest_iso = ""
    latest_act = ""
    if tt_list:
        # pick max ISO date ('' sorts low); if all blank ISO, keep blank
        with_iso = [it for it in tt_list if it.get("TTBL_DATE_ISO")]
        if with_iso:
            with_iso.sort(key=lambda x: x["TTBL_DATE_ISO"])
            latest = with_iso[-1]
            latest_iso = latest["TTBL_DATE_ISO"]
            latest_act = latest["TTBL_ACTION"]
    out["Latest_Action"] = latest_act
    out["latest_action_date"] = latest_iso

    # Other scalars
    out["RFA_REQUIRED"]          = _text_child(ri, "RFA_REQUIRED")

    # SMALL_ENTITY_LIST (list of strings)
    se_list = []
    se_parent = _child(ri, "SMALL_ENTITY_LIST")
    if se_parent is not None:
        for it in _children(se_parent, "SMALL_ENTITY"):
            se_list.append(_t(it.text))
    out["SMALL_ENTITY_LIST"] = _json(se_list)

    # GOVT_LEVEL_LIST (list of strings)
    gv_list = []
    gv_parent = _child(ri, "GOVT_LEVEL_LIST")
    if gv_parent is not None:
        for it in _children(gv_parent, "GOVT_LEVEL"):
            gv_list.append(_t(it.text))
    out["GOVT_LEVEL_LIST"] = _json(gv_list)

    out["PRINT_PAPER"]            = _text_child(ri, "PRINT_PAPER")
    out["INTERNATIONAL_INTEREST"] = _text_child(ri, "INTERNATIONAL_INTEREST")

    # RELATED_RIN_LIST (list of objects)
    rel_list = []
    rel_parent = _child(ri, "RELATED_RIN_LIST")
    if rel_parent is not None:
        for it in _children(rel_parent, "RELATED_RIN"):
            rel_list.append({
                "RIN": _text_child(it, "RIN"),
                "RIN_RELATION": _text_child(it, "RIN_RELATION")
            })
    out["RELATED_RIN_LIST"] = _json(rel_list)

    # CHILD_RIN_LIST (list of objects)
    child_list = []
    child_parent = _child(ri, "CHILD_RIN_LIST")
    if child_parent is not None:
        for it in _children(child_parent, "CHILD_RIN"):
            child_list.append({
                "RIN": _text_child(it, "RIN"),
                "RULE_TITLE": _text_child(it, "RULE_TITLE")
            })
    out["CHILD_RIN_LIST"] = _json(child_list)

    # AGENCY_CONTACT_LIST (list of objects with nested AGENCY and MAILING_ADDRESS)
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

    # Trailing scalars
    out["REINVENT_GOVT"] = _text_child(ri, "REINVENT_GOVT")
    out["ADDITIONAL_INFO"] = _text_child(ri, "ADDITIONAL_INFO")
    out["PROCUREMENT"] = _text_child(ri, "PROCUREMENT")
    out["SIC_DESC"] = _text_child(ri, "SIC_DESC")
    out["PARENT_RIN"] = _text_child(ri, "PARENT_RIN")

    # COMPLIANCE_COST (optional group)
    cc = _child(ri, "COMPLIANCE_COST")
    out["COMPLIANCE_COST_BASE_YEAR"] = _text_child(cc, "BASE_YEAR") if cc is not None else ""
    out["COMPLIANCE_COST_INITIAL_PUBLIC_COST"] = _text_child(cc, "INITIAL_PUBLIC_COST") if cc is not None else ""
    out["COMPLIANCE_COST_RECURRING_PUBLIC_COST"] = _text_child(cc, "RECURRING_PUBLIC_COST") if cc is not None else ""

    return out


# ----------- Public function -----------
def build_ua_csv_from_xml(xml_path: str, out_dir: str) -> str:
    """
    Parse a single UA XML and write a CSV containing *every* column from the Fall 1995 tree,
    plus Latest_Action and latest_action_date.
    Returns the path to the written CSV.
    """
    if not os.path.isfile(xml_path):
        raise FileNotFoundError(f"XML not found: {xml_path}")
    os.makedirs(out_dir, exist_ok=True)

    # Stream-parse: iterate RIN_INFO
    # Use iterparse with 'end' events for memory safety
    ctx = ET.iterparse(xml_path, events=("end",))
    rows = []
    for ev, el in ctx:
        if el.tag.split("}")[-1] == "RIN_INFO":
            rows.append(_parse_rin_info(el))
            # clear to save memory
            el.clear()

    # Order and write
    # Define the exact column order from the tree + two derived columns
    columns = [
        "RIN",
        "PUBLICATION_ID", "PUBLICATION_TITLE",
        "AGENCY_CODE", "AGENCY_NAME", "AGENCY_ACRONYM",
        "PARENT_AGENCY_CODE", "PARENT_AGENCY_NAME", "PARENT_AGENCY_ACRONYM",
        "RULE_TITLE", "ABSTRACT", "PRIORITY_CATEGORY", "RIN_STATUS", "RULE_STAGE",
        "CFR_LIST", "LEGAL_AUTHORITY_LIST", "LEGAL_DLINE_LIST",
        "RPLAN_ENTRY",
        "RPLAN_INFO_STMT_OF_NEED", "RPLAN_INFO_LEGAL_BASIS",
        "RPLAN_INFO_ALTERNATIVES", "RPLAN_INFO_COSTS_AND_BENEFITS", "RPLAN_INFO_RISKS",
        "TIMETABLE_LIST",
        "Latest_Action", "latest_action_date",
        "RFA_REQUIRED", "SMALL_ENTITY_LIST", "GOVT_LEVEL_LIST",
        "PRINT_PAPER", "INTERNATIONAL_INTEREST",
        "RELATED_RIN_LIST", "CHILD_RIN_LIST",
        "AGENCY_CONTACT_LIST",
        "REINVENT_GOVT", "ADDITIONAL_INFO", "PROCUREMENT", "SIC_DESC", "PARENT_RIN",
        "COMPLIANCE_COST_BASE_YEAR", "COMPLIANCE_COST_INITIAL_PUBLIC_COST", "COMPLIANCE_COST_RECURRING_PUBLIC_COST",
    ]

    df = pd.DataFrame(rows)
    # Ensure every requested column exists
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    df = df[columns]

    # Derive output filename from XML basename
    base = os.path.basename(xml_path).replace(".xml", "_flat.csv")
    out_csv = os.path.join(out_dir, base)
    df.to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv, df
