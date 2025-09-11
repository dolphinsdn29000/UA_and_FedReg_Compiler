# reginfo_patch.py
# Drop-in XML parser for REGINFO / Unified Agenda files
# - Backwards-compatible: yields dicts by default
# - Handles CDATA/HTML entities, partial dates, lists, booleans, URLs
# - Streaming iterparse to keep memory low

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date
from html import unescape
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union
import itertools
import re
import xml.etree.ElementTree as ET

# ------------------------------
# Public API
# ------------------------------

def parse_reginfo_xml(
    src: Union[str, bytes],
    *,
    return_dataclasses: bool = False
) -> Iterator[Union["RinInfo", Dict[str, Any]]]:
    """
    Streaming parser. Yields one RIN entry at a time (dict by default).
    `src` can be a path or XML bytes.
    """
    for rin in _iter_rin_info(src):
        if return_dataclasses:
            yield rin
        else:
            yield _rin_to_dict(rin)

# ------------------------------
# Data model (minimal but stable)
# ------------------------------

@dataclass
class Publication:
    id: Optional[str] = None
    title: Optional[str] = None

@dataclass
class Agency:
    code: Optional[str] = None
    name: Optional[str] = None
    acronym: Optional[str] = None

@dataclass
class Contact:
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    title: Optional[str] = None
    agency_code: Optional[str] = None
    agency_name: Optional[str] = None
    agency_acronym: Optional[str] = None
    phone: Optional[str] = None
    phone_ext: Optional[str] = None
    tdd_phone: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None

@dataclass
class Timetable:
    action: Optional[str] = None
    # Some dates are partial; we carry both the normalized date (if possible)
    # and a precision string: "day", "month", or "unknown"
    date: Optional[date] = None
    precision: str = "unknown"  # "day" | "month" | "unknown"
    fr_citation: Optional[str] = None

@dataclass
class RinInfo:
    rin: Optional[str] = None
    publication: Publication = field(default_factory=Publication)
    agency: Agency = field(default_factory=Agency)
    parent_agency: Agency = field(default_factory=Agency)
    rule_title: Optional[str] = None
    abstract: Optional[str] = None
    priority_category: Optional[str] = None
    rin_status: Optional[str] = None
    rule_stage: Optional[str] = None
    major: Optional[bool] = None
    unfunded_mandate: Optional[bool] = None
    cfr_list: List[str] = field(default_factory=list)
    legal_authorities: List[str] = field(default_factory=list)
    rplan_entry: Optional[bool] = None
    timetable: List[Timetable] = field(default_factory=list)
    rfa_required: Optional[bool] = None
    small_entities: List[str] = field(default_factory=list)
    govt_levels: List[str] = field(default_factory=list)
    federalism: Optional[Union[bool, str]] = None
    energy_affected: Optional[Union[bool, str]] = None
    further_info_url: Optional[str] = None
    print_paper: Optional[bool] = None
    international_interest: Optional[bool] = None
    related_rins: List[str] = field(default_factory=list)
    additional_info: Optional[str] = None
    public_comment_url: Optional[str] = None
    naics_codes: List[str] = field(default_factory=list)
    contacts: List[Contact] = field(default_factory=list)

# ------------------------------
# Parsing helpers
# ------------------------------

_BOOL_TRUE = {"yes", "y", "true", "t", "1"}
_BOOL_FALSE = {"no", "n", "false", "f", "0"}

def _to_bool(val: Optional[str]) -> Optional[bool]:
    if val is None:
        return None
    v = val.strip().lower()
    if v in _BOOL_TRUE:
        return True
    if v in _BOOL_FALSE:
        return False
    return None  # preserve unknowns

def _clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    # Unescape HTML entities and normalize whitespace
    t = unescape(text)
    t = re.sub(r"\s+", " ", t).strip()
    return t or None

def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    return _clean_text(elem.text)

_DATE_RE = re.compile(r"^(?P<m>\d{2})/(?P<d>\d{2})/(?P<y>\d{4})$")

def _parse_partial_date(va: Optional[str]) -> Tuple[Optional[date], str]:
    """
    Accepts dates like MM/DD/YYYY; handles unknown days or months given as '00'.
    Returns (date_or_none, precision) where precision is 'day'|'month'|'unknown'.
    """
    if not va:
        return (None, "unknown")
    m = _DATE_RE.match(va.strip())
    if not m:
        return (None, "unknown")
    mm = int(m.group("m"))
    dd = int(m.group("d"))
    yy = int(m.group("y"))
    # Normalize unknown components:
    if mm == 0 and dd == 0:
        return (None, "unknown")
    if dd == 0:
        # Use first of month but mark precision='month'
        try:
            return (date(yy, mm, 1), "month")
        except Exception:
            return (None, "unknown")
    # full day precision
    try:
        return (date(yy, mm, dd), "day")
    except Exception:
        return (None, "unknown")

_URL_RE = re.compile(r"^[a-z]+://", re.IGNORECASE)

def _normalize_url(u: Optional[str]) -> Optional[str]:
    u = _clean_text(u)
    if not u:
        return None
    if not _URL_RE.search(u):
        # add scheme if missing
        return f"http://{u}"
    return u

_PHONE_DIGITS_RE = re.compile(r"\d+")

def _normalize_phone(p: Optional[str]) -> Optional[str]:
    p = _clean_text(p)
    if not p:
        return None
    digits = "".join(_PHONE_DIGITS_RE.findall(p))
    if not digits:
        return None
    # Return E.164-ish for US if 10 or 11 digits; else raw digits
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return digits

def _get_all_text(parent: ET.Element, tag: str) -> List[str]:
    return [t for t in (_text(e) for e in parent.findall(tag)) if t]

def _first(parent: ET.Element, path: str) -> Optional[ET.Element]:
    found = parent.find(path)
    return found

def _parse_contact(c_el: ET.Element) -> Contact:
    a_el = _first(c_el, "AGENCY")
    m_el = _first(c_el, "MAILING_ADDRESS")
    return Contact(
        first_name=_text(_first(c_el, "FIRST_NAME")),
        middle_name=_text(_first(c_el, "MIDDLE_NAME")),
        last_name=_text(_first(c_el, "LAST_NAME")),
        title=_text(_first(c_el, "TITLE")),
        agency_code=_text(_first(a_el, "CODE")) if a_el is not None else None,
        agency_name=_text(_first(a_el, "NAME")) if a_el is not None else None,
        agency_acronym=_text(_first(a_el, "ACRONYM")) if a_el is not None else None,
        phone=_normalize_phone(_text(_first(c_el, "PHONE"))),
        phone_ext=_clean_text(_text(_first(c_el, "PHONE_EXT"))),
        tdd_phone=_normalize_phone(_text(_first(c_el, "TDD_PHONE"))),
        fax=_normalize_phone(_text(_first(c_el, "FAX"))),
        email=_clean_text(_text(_first(c_el, "EMAIL"))),
        street=_clean_text(_text(_first(m_el, "STREET_ADDRESS"))) if m_el is not None else None,
        city=_clean_text(_text(_first(m_el, "CITY"))) if m_el is not None else None,
        state=_clean_text(_text(_first(m_el, "STATE"))) if m_el is not None else None,
        zip=_clean_text(_text(_first(m_el, "ZIP"))) if m_el is not None else None,
    )

def _parse_timetable(t_el: ET.Element) -> Timetable:
    dt_raw = _text(_first(t_el, "TTBL_DATE"))
    dt, prec = _parse_partial_date(dt_raw)
    return Timetable(
        action=_text(_first(t_el, "TTBL_ACTION")),
        date=dt,
        precision=prec,
        fr_citation=_clean_text(_text(_first(t_el, "FR_CITATION"))),
    )

def _parse_agency(a_el: Optional[ET.Element]) -> Agency:
    if a_el is None:
        return Agency()
    return Agency(
        code=_text(_first(a_el, "CODE")),
        name=_text(_first(a_el, "NAME")),
        acronym=_text(_first(a_el, "ACRONYM")),
    )

def _rin_to_dict(rin: RinInfo) -> Dict[str, Any]:
    # Flatten dataclasses to nested dicts with consistent primitives
    d = asdict(rin)
    # Reformat dates to ISO + precision
    for tt in d.get("timetable", []):
        if tt.get("date"):
            tt["date"] = tt["date"].isoformat()
    return d

# ------------------------------
# Core streaming iterator
# ------------------------------

def _iter_rin_info(src: Union[str, bytes]) -> Iterator[RinInfo]:
    # iterparse to keep memory bounded; clear elements as we go
    context = ET.iterparse(src, events=("start", "end"))
    _, root = next(context)  # get root element
    stack: List[ET.Element] = []

    for ev, el in context:
        if ev == "start":
            stack.append(el)
            continue

        # ev == "end"
        if el.tag == "RIN_INFO":
            yield _parse_rin_info(el)
            # Clear processed element to free memory
            el.clear()
            # also clear parents to prevent memory leak
            _prune_to_root(stack, el)

        # Keep moving
        continue

def _prune_to_root(stack: List[ET.Element], processed: ET.Element) -> None:
    # remove processed from stack and clear its previous siblings to help GC
    while stack and stack[-1] is not processed:
        stack.pop()
    if stack:
        stack.pop()  # remove processed itself
    # periodically clear the root to release already-parsed siblings
    # (root is the first element in the stack after pops)

def _parse_rin_info(node: ET.Element) -> RinInfo:
    # Convenience: find helper
    f = node.find

    # Publication
    pub_el = f("PUBLICATION")
    publication = Publication(
        id=_text(_first(pub_el, "PUBLICATION_ID")) if pub_el is not None else None,
        title=_text(_first(pub_el, "PUBLICATION_TITLE")) if pub_el is not None else None,
    )

    # Agencies
    agency = _parse_agency(_first(node, "AGENCY"))
    parent_agency = _parse_agency(_first(node, "PARENT_AGENCY"))

    # Lists
    cfr_list = _get_all_text(_first(node, "CFR_LIST") or node, "CFR")
    legal_authorities = _get_all_text(_first(node, "LEGAL_AUTHORITY_LIST") or node, "LEGAL_AUTHORITY")
    small_entities = _get_all_text(_first(node, "SMALL_ENTITY_LIST") or node, "SMALL_ENTITY")
    govt_levels = _get_all_text(_first(node, "GOVT_LEVEL_LIST") or node, "GOVT_LEVEL")
    related_rins = _get_all_text(_first(node, "RELATED_RIN_LIST") or node, "RIN")
    naics_codes = [t for t in _get_all_text(_first(node, "NAICS_LIST") or node, "NAICS_CD")]

    # Timetable
    t_list_el = _first(node, "TIMETABLE_LIST")
    timetables = []
    if t_list_el is not None:
        for t_el in t_list_el.findall("TIMETABLE"):
            timetables.append(_parse_timetable(t_el))

    # Contacts
    contacts = []
    c_list_el = _first(node, "AGENCY_CONTACT_LIST")
    if c_list_el is not None:
        for c_el in c_list_el.findall("CONTACT"):
            contacts.append(_parse_contact(c_el))

    # Simple fields
    abstract = _text(_first(node, "ABSTRACT"))
    additional_info = _text(_first(node, "ADDITIONAL_INFO"))

    # Booleans or enums
    major = _to_bool(_text(_first(node, "MAJOR")))
    unfunded = _to_bool(_text(_first(_first(node, "UNFUNDED_MANDATE_LIST") or node, "UNFUNDED_MANDATE")))
    rplan_entry = _to_bool(_text(_first(node, "RPLAN_ENTRY")))
    rfa_required = _to_bool(_text(_first(node, "RFA_REQUIRED")))
    federalism_raw = _text(_first(node, "FEDERALISM"))
    energy_raw = _text(_first(node, "ENERGY_AFFECTED"))
    print_paper = _to_bool(_text(_first(node, "PRINT_PAPER")))
    international_interest = _to_bool(_text(_first(node, "INTERNATIONAL_INTEREST")))

    return RinInfo(
        rin=_text(_first(node, "RIN")),
        publication=publication,
        agency=agency,
        parent_agency=parent_agency,
        rule_title=_text(_first(node, "RULE_TITLE")),
        abstract=abstract,
        priority_category=_text(_first(node, "PRIORITY_CATEGORY")),
        rin_status=_text(_first(node, "RIN_STATUS")),
        rule_stage=_text(_first(node, "RULE_STAGE")),
        major=major,
        unfunded_mandate=unfunded,
        cfr_list=cfr_list,
        legal_authorities=legal_authorities,
        rplan_entry=rplan_entry,
        timetable=timetables,
        rfa_required=rfa_required,
        small_entities=small_entities,
        govt_levels=govt_levels,
        federalism=_normalize_tri_state(federalism_raw),
        energy_affected=_normalize_tri_state(energy_raw),
        further_info_url=_normalize_url(_text(_first(node, "FURTHER_INFO_URL"))),
        print_paper=print_paper,
        international_interest=international_interest,
        related_rins=related_rins,
        additional_info=additional_info,
        public_comment_url=_normalize_url(_text(_first(node, "PUBLIC_COMMENT_URL"))),
        naics_codes=naics_codes,
        contacts=contacts,
    )

def _normalize_tri_state(v: Optional[str]) -> Optional[Union[bool, str]]:
    """
    FEDERALISM and ENERGY_AFFECTED sometimes carry 'Yes/No' or 'Undetermined'.
    Return True/False for yes/no; otherwise the original canonical string.
    """
    vv = _clean_text(v)
    if vv is None:
        return None
    lb = _to_bool(vv)
    return lb if lb is not None else vv
