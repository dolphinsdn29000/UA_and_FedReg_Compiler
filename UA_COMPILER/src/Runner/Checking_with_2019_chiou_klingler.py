"""

**** The point of this is to check with prior literature if our method of getting
the unfied agenda into a single rule rows worked the same. CHiou and klinger get about
39k form 1995-2019. For some reason the hand did an aritcle whcih we dont use in early 1995,
however, so we expect a little less because we dont have this data, but this file shows
restricting to their data (minus the one half year we dont hav edata for) we get
38k rules, very closely matching their result"


UA Throwaway Test Runner (≤ 2019) — WHY this exists
===================================================

Why a separate test runner?
---------------------------
• Fast, no‑side‑effects validation that our “last‑per‑RIN” counts match legacy work
  (e.g., CK window). It avoids writing *any* outputs to disk and prints only the count.

Why stop at 201912?
-------------------
• To reproduce the historical window cleanly and compare apples to apples with prior
  baselines and publications.

Why import the parser by file path again?
-----------------------------------------
• Keeps the test hermetic: it always uses the parser you intend, regardless of IDE
  settings or working directory. No dependency on PYTHONPATH.

What this runner deliberately avoids
------------------------------------
• No CSV writes (uses TemporaryDirectory for any per‑file artifacts).
• No schema juggling—only the minimal columns needed for last‑per‑RIN (RIN,
  PUBLICATION_ID, source_xml). This keeps it fast and eliminates unrelated failures.

Expected behavior
-----------------
• Prints a single integer: the number of unique RINs in the last‑per‑RIN subset for
  files with YYYYMM ≤ 201912. If parsing fails entirely, prints 0.

"""


import os
import re
import sys
import tempfile
import importlib.util
import pandas as pd

# ---- ABSOLUTE PATH to your parser ----
MODULE_PATH = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/src/UA_COMPILER/UA_Parser_For_Single_xml_9_12_25.py"
spec = importlib.util.spec_from_file_location("ua_parser_single", MODULE_PATH)
ua = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ua)  # ua.build_ua_csv_from_xml is now available

# ---- Your UA directory ----
SRC_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data"

UA_NAME_RE = re.compile(r"REGINFO_RIN_DATA_(\d{6})\.xml$", re.IGNORECASE)

def list_xmls_through_2019(src_dir: str):
    keep = []
    for f in os.listdir(src_dir):
        full = os.path.join(src_dir, f)
        if not os.path.isfile(full):
            continue
        m = UA_NAME_RE.match(f)
        if not m:
            continue
        ym = m.group(1)
        try:
            if int(ym) <= 201912:
                keep.append(full)
        except Exception:
            pass
    return sorted(keep)

def extract_pub_ym_from_filename(fname: str) -> str:
    m = UA_NAME_RE.match(os.path.basename(fname))
    return m.group(1) if m else ""

def to_pub_int(ym: str) -> int:
    s = str(ym or "").strip()
    s = re.sub(r"\D", "", s)
    if len(s) == 6:
        try:
            return int(s)
        except Exception:
            return -1
    return -1

def main():
    xml_paths = list_xmls_through_2019(SRC_DIR)

    # No output files: use a temp dir for per-file CSVs the parser writes
    with tempfile.TemporaryDirectory(prefix="ua_test2019_") as tmp_out:
        dfs = []
        for xml_path in xml_paths:
            try:
                _, df = ua.build_ua_csv_from_xml(xml_path, tmp_out)
                df = df.copy()
                df["source_xml"] = os.path.basename(xml_path)  # needed for fallback YYYYMM
                dfs.append(df)
            except Exception:
                # silent skip to keep output clean
                pass

        if not dfs:
            print(0)  # nothing parsed; print 0 and exit
            return

        # Combine in memory (only the columns we need)
        need_cols = {"RIN", "PUBLICATION_ID", "source_xml"}
        col_union = set().union(*[set(d.columns) for d in dfs])
        cols = list(need_cols & col_union)
        aligned = [d.reindex(columns=cols, fill_value="") for d in dfs]
        df_all = pd.concat(aligned, ignore_index=True)

        # Drop blank RINs
        df_all = df_all[df_all["RIN"].astype(str).str.strip() != ""].copy()
        if df_all.empty:
            print(0)
            return

        # Build last-per-RIN
        df_all["_pub_ym_csv"]  = df_all["PUBLICATION_ID"].astype(str).str.replace(r"\D", "", regex=True)
        df_all["_pub_ym_file"] = df_all["source_xml"].apply(extract_pub_ym_from_filename)
        df_all["_pub_ym"]      = df_all["_pub_ym_csv"].where(df_all["_pub_ym_csv"].str.len() == 6,
                                                             df_all["_pub_ym_file"])
        df_all["_pub_int"]     = df_all["_pub_ym"].apply(to_pub_int)
        df_all["_row_ordinal"] = range(len(df_all))

        df_all = df_all.sort_values(["RIN", "_pub_int", "_row_ordinal"])
        idx_last = df_all.groupby("RIN")["_pub_int"].idxmax()
        df_last_only = df_all.loc[idx_last]

        # Print ONLY the count
        print(len(df_last_only))

if __name__ == "__main__":
    main()
