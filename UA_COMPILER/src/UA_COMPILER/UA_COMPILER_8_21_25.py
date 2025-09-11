#!/usr/bin/env python3
"""
reginfo_to_csv.py

Parse a REGINFO "Unified Agenda of Federal Regulatory and Deregulatory Actions"
XML file (REGINFO_RIN_DATA) and export normalized CSV files.

Outputs (written to --outdir):
  - rin_summary.csv
  - rin_timetable.csv
  - rin_contacts.csv
  - rin_cfr.csv
  - rin_legal_authority.csv
  - rin_related_rins.csv
  - rin_naics.csv
  - rin_small_entity.csv
  - rin_govt_levels.csv

Example:
  python reginfo_to_csv.py REGINFO_RIN_DATA.xml --outdir ./reginfo_out
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Tuple


def ns_strip(tag: str) -> str:
    """Strip any XML namespace and return the local tag name."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag


def text_clean(s: str | None) -> str:
    """Collapse whitespace; return empty string if None."""
    if not s:
        return ""
    return " ".join(s.split())


def child_text(parent: ET.Element, child_tag: str) -> str:
    """Get cleaned text of first child with tag (no namespace required)."""
    node = parent.find(child_tag)
    return text_clean(node.text) if node is not None else ""


def findall(parent: ET.Element, path: str) -> List[ET.Element]:
    """Helper to find all elements by path (no namespaces expected)."""
    return list(parent.findall(path))


def ensure_outdir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)


def write_csv(out_path: Path, rows: List[Dict[str, str]], header: List[str]) -> None:
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            # Guarantee all fields exist
            for k in header:
                r.setdefault(k, "")
            w.writerow(r)


def parse_rin_info(elem: ET.Element) -> Tuple[
    Dict[str, str],
    List[Dict[str, str]],  # timetables
    List[Dict[str, str]],  # contacts
    List[Dict[str, str]],  # cfrs
    List[Dict[str, str]],  # legal_auth
    List[Dict[str, str]],  # related_rins
    List[Dict[str, str]],  # naics
    List[Dict[str, str]],  # small_entity
    List[Dict[str, str]],  # govt_levels
]:
    """Parse one <RIN_INFO> block into normalized pieces."""
    # Core summary fields
    rin = child_text(elem, "RIN")
    pub_id = child_text(elem, "PUBLICATION/PUBLICATION_ID")
    pub_title = child_text(elem, "PUBLICATION/PUBLICATION_TITLE")

    agency_code = child_text(elem, "AGENCY/CODE")
    agency_name = child_text(elem, "AGENCY/NAME")
    agency_acronym = child_text(elem, "AGENCY/ACRONYM")

    parent_agency_code = child_text(elem, "PARENT_AGENCY/CODE")
    parent_agency_name = child_text(elem, "PARENT_AGENCY/NAME")
    parent_agency_acronym = child_text(elem, "PARENT_AGENCY/ACRONYM")

    rule_title = child_text(elem, "RULE_TITLE")
    abstract = child_text(elem, "ABSTRACT")
    priority_category = child_text(elem, "PRIORITY_CATEGORY")
    rin_status = child_text(elem, "RIN_STATUS")
    rule_stage = child_text(elem, "RULE_STAGE")
    major = child_text(elem, "MAJOR")

    rfa_required = child_text(elem, "RFA_REQUIRED")
    federalism = child_text(elem, "FEDERALISM")
    energy_affected = child_text(elem, "ENERGY_AFFECTED")
    print_paper = child_text(elem, "PRINT_PAPER")
    international_interest = child_text(elem, "INTERNATIONAL_INTEREST")
    rplan_entry = child_text(elem, "RPLAN_ENTRY")  # Sometimes "No"/"Yes"
    additional_info = child_text(elem, "ADDITIONAL_INFO")
    further_info_url = child_text(elem, "FURTHER_INFO_URL")
    public_comment_url = child_text(elem, "PUBLIC_COMMENT_URL")

    # Lists that we'll also reference in summary (joined) but provide normalized CSVs
    cfrs_elems = findall(elem, "CFR_LIST/CFR")
    cfrs_list = [text_clean(x.text) for x in cfrs_elems if text_clean(x.text)]

    leg_elems = findall(elem, "LEGAL_AUTHORITY_LIST/LEGAL_AUTHORITY")
    legal_authority_list = [text_clean(x.text) for x in leg_elems if text_clean(x.text)]

    small_elems = findall(elem, "SMALL_ENTITY_LIST/SMALL_ENTITY")
    small_entity_list = [text_clean(x.text) for x in small_elems if text_clean(x.text)]

    gl_elems = findall(elem, "GOVT_LEVEL_LIST/GOVT_LEVEL")
    govt_level_list = [text_clean(x.text) for x in gl_elems if text_clean(x.text)]

    # Related RINs
    rr_elems = findall(elem, "RELATED_RIN_LIST/RELATED_RIN")
    related_rins = []
    for rr in rr_elems:
        rr_rin = child_text(rr, "RIN")
        rr_rel = child_text(rr, "RIN_RELATION")
        if rr_rin or rr_rel:
            related_rins.append({"rin": rin, "related_rin": rr_rin, "relation": rr_rel})

    # NAICS (optional)
    naics_elems = findall(elem, "NAICS_LIST/NAICS")
    naics_rows = []
    for n in naics_elems:
        code = child_text(n, "NAICS_CD")
        desc = child_text(n, "NAICS_DESC")
        if code or desc:
            naics_rows.append({"rin": rin, "naics_code": code, "naics_desc": desc})

    # Timetables
    tt_elems = findall(elem, "TIMETABLE_LIST/TIMETABLE")
    timetables = []
    for tt in tt_elems:
        action = child_text(tt, "TTBL_ACTION")
        date = child_text(tt, "TTBL_DATE")
        fr_cite = child_text(tt, "FR_CITATION")
        timetables.append({
            "rin": rin,
            "action": action,
            "date": date,          # left as-is (can be MM/DD/YYYY or MM/00/YYYY etc.)
            "fr_citation": fr_cite
        })

    # Contacts
    c_elems = findall(elem, "AGENCY_CONTACT_LIST/CONTACT")
    contacts = []
    for c in c_elems:
        first = child_text(c, "FIRST_NAME")
        middle = child_text(c, "MIDDLE_NAME")
        last = child_text(c, "LAST_NAME")
        title = child_text(c, "TITLE")
        phone = child_text(c, "PHONE")
        phone_ext = child_text(c, "PHONE_EXT")
        tdd_phone = child_text(c, "TDD_PHONE")
        fax = child_text(c, "FAX")
        email = child_text(c, "EMAIL")

        # Nested agency info inside contact
        ca_code = child_text(c, "AGENCY/CODE")
        ca_name = child_text(c, "AGENCY/NAME")
        ca_acr = child_text(c, "AGENCY/ACRONYM")

        # Mailing address
        ma_street = child_text(c, "MAILING_ADDRESS/STREET_ADDRESS")
        ma_city = child_text(c, "MAILING_ADDRESS/CITY")
        ma_state = child_text(c, "MAILING_ADDRESS/STATE")
        ma_zip = child_text(c, "MAILING_ADDRESS/ZIP")

        contacts.append({
            "rin": rin,
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "title": title,
            "phone": phone,
            "phone_ext": phone_ext,
            "tdd_phone": tdd_phone,
            "fax": fax,
            "email": email,
            "contact_agency_code": ca_code,
            "contact_agency_name": ca_name,
            "contact_agency_acronym": ca_acr,
            "addr_street": ma_street,
            "addr_city": ma_city,
            "addr_state": ma_state,
            "addr_zip": ma_zip,
        })

    # Build the summary row (join multi-value fields with "; ")
    summary = {
        "rin": rin,
        "publication_id": pub_id,
        "publication_title": pub_title,

        "agency_code": agency_code,
        "agency_name": agency_name,
        "agency_acronym": agency_acronym,

        "parent_agency_code": parent_agency_code,
        "parent_agency_name": parent_agency_name,
        "parent_agency_acronym": parent_agency_acronym,

        "rule_title": rule_title,
        "abstract": abstract,

        "priority_category": priority_category,
        "rin_status": rin_status,
        "rule_stage": rule_stage,
        "major": major,

        "rfa_required": rfa_required,
        "federalism": federalism,
        "energy_affected": energy_affected,
        "print_paper": print_paper,
        "international_interest": international_interest,
        "rplan_entry": rplan_entry,
        "additional_info": additional_info,
        "further_info_url": further_info_url,
        "public_comment_url": public_comment_url,

        # Collapsed list fields for quick human scanning
        "cfr_list": "; ".join(cfrs_list),
        "legal_authority_list": "; ".join(legal_authority_list),
        "small_entity_list": "; ".join(small_entity_list),
        "govt_level_list": "; ".join(govt_level_list),

        # Counts for awareness
        "timetable_count": str(len(timetables)),
        "contact_count": str(len(contacts)),
        "related_rins_count": str(len(related_rins)),
        "naics_count": str(len(naics_rows)),
    }

    # Normalized list rows
    cfr_rows = [{"rin": rin, "cfr": cfr} for cfr in cfrs_list]
    legal_rows = [{"rin": rin, "legal_authority": la} for la in legal_authority_list]
    small_rows = [{"rin": rin, "small_entity": se} for se in small_entity_list]
    govt_rows = [{"rin": rin, "government_level": gl} for gl in govt_level_list]

    return (
        summary, timetables, contacts, cfr_rows, legal_rows,
        related_rins, naics_rows, small_rows, govt_rows
    )


def parse_reginfo_xml(xml_path: Path) -> Dict[str, List[Dict[str, str]]]:
    """
    Stream-parse the XML and collect normalized tables.
    Returns a dict of table_name -> list of row dicts.
    """
    tables = {
        "summary": [],
        "timetable": [],
        "contacts": [],
        "cfr": [],
        "legal_authority": [],
        "related_rins": [],
        "naics": [],
        "small_entity": [],
        "govt_levels": [],
    }

    # iterparse for memory efficiency
    # We trigger on end events so the element is fully built for parsing.
    context = ET.iterparse(str(xml_path), events=("end",))
    for event, elem in context:
        if ns_strip(elem.tag) == "RIN_INFO":
            (
                summary, timetables, contacts, cfr_rows, legal_rows,
                related_rins, naics_rows, small_rows, govt_rows
            ) = parse_rin_info(elem)

            tables["summary"].append(summary)
            tables["timetable"].extend(timetables)
            tables["contacts"].extend(contacts)
            tables["cfr"].extend(cfr_rows)
            tables["legal_authority"].extend(legal_rows)
            tables["related_rins"].extend(related_rins)
            tables["naics"].extend(naics_rows)
            tables["small_entity"].extend(small_rows)
            tables["govt_levels"].extend(govt_rows)

            # Free memory: clear processed element
            elem.clear()

    return tables


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export normalized CSVs from a REGINFO Unified Agenda XML."
    )
    ap.add_argument("xml_path", help="Path to REGINFO_RIN_DATA XML file.")
    ap.add_argument(
        "--outdir", default="reginfo_out",
        help="Output directory for CSV files (default: reginfo_out)"
    )
    args = ap.parse_args()

    xml_path = Path(args.xml_path).expanduser().resolve()
    if not xml_path.exists():
        print(f"ERROR: XML file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)

    outdir = Path(args.outdir).expanduser().resolve()
    ensure_outdir(outdir)

    print(f"Parsing: {xml_path}")
    tables = parse_reginfo_xml(xml_path)

    # Write CSVs
    write_csv(
        outdir / "rin_summary.csv",
        tables["summary"],
        header=[
            "rin",
            "publication_id", "publication_title",
            "agency_code", "agency_name", "agency_acronym",
            "parent_agency_code", "parent_agency_name", "parent_agency_acronym",
            "rule_title", "abstract",
            "priority_category", "rin_status", "rule_stage", "major",
            "rfa_required", "federalism", "energy_affected", "print_paper", "international_interest",
            "rplan_entry", "additional_info", "further_info_url", "public_comment_url",
            "cfr_list", "legal_authority_list", "small_entity_list", "govt_level_list",
            "timetable_count", "contact_count", "related_rins_count", "naics_count",
        ],
    )

    write_csv(
        outdir / "rin_timetable.csv",
        tables["timetable"],
        header=["rin", "action", "date", "fr_citation"],
    )

    write_csv(
        outdir / "rin_contacts.csv",
        tables["contacts"],
        header=[
            "rin",
            "first_name", "middle_name", "last_name", "title",
            "phone", "phone_ext", "tdd_phone", "fax", "email",
            "contact_agency_code", "contact_agency_name", "contact_agency_acronym",
            "addr_street", "addr_city", "addr_state", "addr_zip",
        ],
    )

    write_csv(
        outdir / "rin_cfr.csv",
        tables["cfr"],
        header=["rin", "cfr"],
    )

    write_csv(
        outdir / "rin_legal_authority.csv",
        tables["legal_authority"],
        header=["rin", "legal_authority"],
    )

    write_csv(
        outdir / "rin_related_rins.csv",
        tables["related_rins"],
        header=["rin", "related_rin", "relation"],
    )

    write_csv(
        outdir / "rin_naics.csv",
        tables["naics"],
        header=["rin", "naics_code", "naics_desc"],
    )

    write_csv(
        outdir / "rin_small_entity.csv",
        tables["small_entity"],
        header=["rin", "small_entity"],
    )

    write_csv(
        outdir / "rin_govt_levels.csv",
        tables["govt_levels"],
        header=["rin", "government_level"],
    )

    print(f"CSV files written to: {outdir}")


if __name__ == "__main__":
    main()
