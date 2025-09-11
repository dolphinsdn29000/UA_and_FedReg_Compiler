# run_ua_all_xmls.py
# -------------------------------------------------------------
# Parse ALL UA XML files in a directory and write:
#  1) ONE combined CSV across all files (with 'source_xml'),
#  2) ONE "last per RIN" CSV (keep only the last appearance of each RIN),
#     with EO_13771_DESIGNATION backfilled from the latest prior non-blank.
#
# Uses the existing parser in ua_single_xml_to_csv.py (lxml-based).
# -------------------------------------------------------------

import os
import re
import pandas as pd
from ua_single_xml_to_csv import build_ua_csv_from_xml, _PREFERRED_ORDER

# --- Paths (your exact paths) ---
SRC_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/Unified_Agenda_xml_Data"
OUT_DIR = "/Users/tonymolino/Dropbox/Mac/Desktop/PyProjects/UA_and_FEG_REG_COMPILER/UA_COMPILER/UA_COMPILER_OUTPUT_DATA"

AGG_CSV = os.path.join(OUT_DIR, "ua_all_flat.csv")
COUNTS_BY_FILE_CSV = os.path.join(OUT_DIR, "ua_all_counts_by_file.csv")
LAST_PER_RIN_CSV = os.path.join(OUT_DIR, "ua_all_last_per_rin.csv")
LAST_PER_RIN_EO_LOG = os.path.join(OUT_DIR, "ua_all_last_per_rin_eo13771_backfill_log.csv")

# Canonical UA filename pattern and helper to extract YYYYMM from the filename
UA_NAME_RE = re.compile(r"REGINFO_RIN_DATA_(\d{6})\.xml$", re.IGNORECASE)

def list_xmls(src_dir: str):
    files = sorted(
        f for f in os.listdir(src_dir)
        if os.path.isfile(os.path.join(src_dir, f))
    )
    ua_files = [os.path.join(src_dir, f) for f in files if UA_NAME_RE.match(f)]
    if ua_files:
        return ua_files
    # Fallback: any .xml
    return [os.path.join(src_dir, f) for f in files if f.lower().endswith(".xml")]

def extract_pub_ym_from_filename(fname: str) -> str:
    m = UA_NAME_RE.match(os.path.basename(fname))
    return m.group(1) if m else ""

def to_pub_int(ym: str) -> int:
    """Convert 'YYYYMM' to int YYYYMM for sorting (invalid -> -1)."""
    s = str(ym or "").strip()
    s_digits = re.sub(r"\D", "", s)
    if len(s_digits) == 6:
        try:
            return int(s_digits)
        except Exception:
            return -1
    return -1

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    xml_paths = list_xmls(SRC_DIR)
    if not xml_paths:
        raise FileNotFoundError(f"No XML files found in {SRC_DIR}")

    print(f"[INFO] Found {len(xml_paths)} XML files in {SRC_DIR}")

    dfs = []
    for i, xml_path in enumerate(xml_paths, 1):
        base = os.path.basename(xml_path)
        try:
            csv_path, df = build_ua_csv_from_xml(xml_path, OUT_DIR)
            df = df.copy()
            df["source_xml"] = base
            dfs.append(df)
            print(f"[{i}/{len(xml_paths)}] Parsed {base}: rows={len(df):,}, cols={len(df.columns):,}")
        except Exception as e:
            print(f"[WARN] Skipping {base}: {e}")

    if not dfs:
        raise RuntimeError("Parsed 0 rows from UA XMLs — all files failed or none matched.")

    # --- Build unified column order ---
    preferred = list(_PREFERRED_ORDER)
    # Insert source_xml after PUBLICATION_TITLE (keeps identity fields together)
    if "PUBLICATION_TITLE" in preferred:
        idx = preferred.index("PUBLICATION_TITLE") + 1
    else:
        idx = 1  # right after RIN if PUBLICATION_TITLE not found
    order = preferred[:idx] + ["source_xml"] + preferred[idx:]

    # Union of all columns seen across files
    col_union = set()
    for d in dfs:
        col_union.update(d.columns)

    extras = sorted(c for c in col_union if c not in set(order))
    final_cols = order + extras

    # --- Concatenate with aligned columns (missing -> empty string) ---
    aligned = [d.reindex(columns=final_cols, fill_value="") for d in dfs]
    df_all = pd.concat(aligned, ignore_index=True)

    # --- Write combined CSV ---
    df_all.to_csv(AGG_CSV, index=False, encoding="utf-8")
    print(f"[INFO] Wrote combined CSV: {AGG_CSV}  rows={len(df_all):,}  cols={len(df_all.columns):,}")

    # --- Counts by file (sanity) ---
    counts = df_all.groupby("source_xml")["RIN"].count().reset_index(name="rows")
    counts = counts.sort_values("source_xml")
    counts.to_csv(COUNTS_BY_FILE_CSV, index=False, encoding="utf-8")
    print(f"[INFO] Wrote counts by file: {COUNTS_BY_FILE_CSV}")

    # ============================
    # LAST-PER-RIN with backfill
    # ============================
    df_last_calc = df_all.copy()

    # Normalize PUBLICATION_ID and compute publication int rank
    if "PUBLICATION_ID" not in df_last_calc.columns:
        df_last_calc["PUBLICATION_ID"] = ""

    df_last_calc["_pub_ym_csv"]  = df_last_calc["PUBLICATION_ID"].astype(str).str.replace(r"\D", "", regex=True)
    df_last_calc["_pub_ym_file"] = df_last_calc["source_xml"].apply(extract_pub_ym_from_filename)
    df_last_calc["_pub_ym"]      = df_last_calc["_pub_ym_csv"].where(df_last_calc["_pub_ym_csv"].str.len() == 6,
                                                                     df_last_calc["_pub_ym_file"])
    df_last_calc["_pub_int"]     = df_last_calc["_pub_ym"].apply(to_pub_int)
    df_last_calc["_row_ordinal"] = range(len(df_last_calc))

    # Determine the last row per RIN (max _pub_int, tie-breaker on row order)
    df_last_calc = df_last_calc.sort_values(["RIN", "_pub_int", "_row_ordinal"])
    idx_last = df_last_calc.groupby("RIN")["_pub_int"].idxmax()
    df_last_only = df_last_calc.loc[idx_last].copy()

    # ----- Backfill EO_13771_DESIGNATION if blank in the last row -----
    eo_col = "EO_13771_DESIGNATION"
    if eo_col in df_last_only.columns:

        # Build map of last _pub_int per RIN
        last_pub_map = df_last_only.set_index("RIN")["_pub_int"]

        # Candidate prior rows with non-blank EO (strictly earlier issues)
        df_prior = df_last_calc.merge(
            last_pub_map.rename("last_pub_int"), left_on="RIN", right_index=True, how="inner"
        )
        df_prior["_eo_norm"] = df_prior[eo_col].astype(str).str.strip()
        df_prior = df_prior[(df_prior["_eo_norm"] != "") & (df_prior["_pub_int"] < df_prior["last_pub_int"])]

        # For each RIN, select the latest prior row (_pub_int max)
        df_prior = df_prior.sort_values(["RIN", "_pub_int", "_row_ordinal"])
        df_prior_latest = df_prior.groupby("RIN").tail(1)

        # Maps for fill value and provenance
        fill_val_map      = dict(zip(df_prior_latest["RIN"], df_prior_latest[eo_col]))
        fill_pubym_map    = dict(zip(df_prior_latest["RIN"], df_prior_latest["_pub_ym"]))
        fill_srcxml_map   = dict(zip(df_prior_latest["RIN"], df_prior_latest["source_xml"]))

        # Determine which last rows actually need a backfill
        need_mask = df_last_only[eo_col].astype(str).str.strip().eq("") & df_last_only["RIN"].isin(fill_val_map.keys())
        n_need = int(need_mask.sum())

        if n_need > 0:
            # Apply backfill
            df_last_only.loc[need_mask, eo_col] = df_last_only.loc[need_mask, "RIN"].map(fill_val_map)

            # Optional: write a small backfill log for audit
            bf_log = df_last_only.loc[need_mask, ["RIN", "PUBLICATION_ID", "source_xml"]].copy()
            bf_log = bf_log.rename(columns={"PUBLICATION_ID": "last_PUBLICATION_ID", "source_xml": "last_source_xml"})
            bf_log["filled_EO_13771_DESIGNATION"] = bf_log["RIN"].map(fill_val_map)
            bf_log["filled_from_pub_ym"]          = bf_log["RIN"].map(fill_pubym_map)
            bf_log["filled_from_source_xml"]      = bf_log["RIN"].map(fill_srcxml_map)
            bf_log.to_csv(LAST_PER_RIN_EO_LOG, index=False, encoding="utf-8")
            print(f"[INFO] EO_13771_DESIGNATION backfilled for {n_need} RINs. Log -> {LAST_PER_RIN_EO_LOG}")
        else:
            print("[INFO] No EO_13771_DESIGNATION backfills were needed.")

    else:
        print(f"[INFO] Column '{eo_col}' not present; skipping EO backfill.")

    # Clean helper columns before saving
    df_last_only = df_last_only.drop(columns=["_pub_ym_csv", "_pub_ym_file", "_pub_ym", "_pub_int", "_row_ordinal"],
                                     errors="ignore")

    # Reorder to the final columns layout (add any extras discovered)
    # Build union and final order (same as combined)
    col_union_last = set(df_last_only.columns)
    extras_last = sorted(c for c in col_union_last if c not in set(final_cols))
    final_cols_last = final_cols + extras_last

    df_last_only = df_last_only.reindex(columns=final_cols_last, fill_value="")

    # --- Write last-per-RIN CSV ---
    df_last_only.to_csv(LAST_PER_RIN_CSV, index=False, encoding="utf-8")
    print(f"[INFO] Wrote last-per-RIN CSV: {LAST_PER_RIN_CSV}  rows={len(df_last_only):,}  (unique RINs)")

    # Optional previews
    print("\n[Preview] Combined (first 3):")
    print(df_all.head(3).to_string(index=False))
    print("\n[Preview] Last-per-RIN (first 3):")
    print(df_last_only.head(3).to_string(index=False))


if __name__ == "__main__":
    main()
