
# run_ua_single_xml_to_csv.py
# -------------------------------------------------------------
# Calls build_ua_csv_from_xml with the user-specified paths.
# -------------------------------------------------------------

import os
from ua_single_xml_to_csv import build_ua_csv_from_xml

XML_PATH = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data/REGINFO_RIN_DATA_199510.xml"
OUT_DIR  = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA"

if __name__ == "__main__":
    csv_path, df = build_ua_csv_from_xml(XML_PATH, OUT_DIR)
    print(f"Wrote CSV to: {csv_path}")
    print(f"Rows: {len(df):,}, Columns: {len(df.columns):,}")
    # Print first few columns and first row as a sanity check
    print("Columns:", list(df.columns))
    print(df.head(3).to_string(index=False))
