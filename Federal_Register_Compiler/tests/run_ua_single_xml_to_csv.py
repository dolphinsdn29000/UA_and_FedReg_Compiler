# run_ua_single_xml_to_csv.py
# -------------------------------------------------------------
# Calls build_ua_csv_from_xml twice with your paths and compares columns.
# -------------------------------------------------------------

from ua_single_xml_to_csv import build_ua_csv_from_xml

XML_1995 = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data/REGINFO_RIN_DATA_199510.xml"
XML_2018 = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data/REGINFO_RIN_DATA_201810.xml"
OUT_DIR  = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA"

if __name__ == "__main__":
    csv_1995, df95 = build_ua_csv_from_xml(XML_1995, OUT_DIR)
    print(f"[1995] Wrote: {csv_1995}  rows={len(df95):,}  cols={len(df95.columns):,}")

    csv_2018, df18 = build_ua_csv_from_xml(XML_2018, OUT_DIR)
    print(f"[2018] Wrote: {csv_2018}  rows={len(df18):,}  cols={len(df18.columns):,}")

    s95, s18 = set(df95.columns), set(df18.columns)
    only_in_95 = sorted(s95 - s18)
    only_in_18 = sorted(s18 - s95)
    in_both    = sorted(s95 & s18)

    print("\n=== Column comparison ===")
    print(f"In both ({len(in_both)}): {in_both[:20]}{' ...' if len(in_both)>20 else ''}")
    print(f"Only in 1995 ({len(only_in_95)}): {only_in_95}")
    print(f"Only in 2018 ({len(only_in_18)}): {only_in_18}")

    # Quick sanity previews
    print("\n[1995] First columns preview:", list(df95.columns)[:12])
    print(df95.head(3).to_string(index=False))
    print("\n[2018] First columns preview:", list(df18.columns)[:12])
    print(df18.head(3).to_string(index=False))
