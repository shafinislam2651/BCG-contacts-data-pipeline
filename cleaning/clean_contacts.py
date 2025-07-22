import pandas as pd
import re
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)

# Fields to be set explicitly to NULL (dropped from use)
UNUSED_FIELDS = [
    "SALUTATION", "DIRECTPHONE", "DIRECTFAX", "HOMEPHONE",
    "MSN_ID", "YAHOO_ID", "SKYPE_ID", "SYNC_CONTACTS", "LINKEDIN",
    "TWITTER", "FACEBOOK", "CAMPAIGN_WAVE_SEQNO", "LATITUDE", "LONGITUDE",
    "GEOCODE_STATUS", "X_STORE", "X_EMAIL2", "X_EMAIL3",
    "X_PHONE1", "X_PHONE2", "X_PHONE3", "X_PHONE4", "X_PHONE5", "X_TT_EXTENSION"
] + [f"SUB{i}" for i in range(1, 27)] + ["X_REGION"]

def clean_fields(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("🧹 Cleaning all fields...")

    # Set unused fields to NULL if they exist
    for field in UNUSED_FIELDS:
        if field in df.columns:
            df[field] = pd.NA

    # Clean email fields
    for col in df.columns:
        if "EMAIL" in col.upper():
            df[col] = df[col].astype(str).str.strip().str.lower()
            df[col] = df[col].apply(lambda x: x if re.match(r"[^@]+@[^@]+\.[^@]+", x) else pd.NA)

    # Clean phone fields
    for col in df.columns:
        if "PHONE" in col.upper():
            df[col] = df[col].astype(str).apply(lambda x: re.sub(r"\D", "", x.strip()) if pd.notna(x) else pd.NA)

    # Clean name/title fields
    for col in df.columns:
        if col.upper() in ["FIRSTNAME", "LASTNAME", "FULLNAME", "TITLE"]:
            df[col] = df[col].astype(str).str.title().str.strip()

    # Clean address/postcode
    for col in df.columns:
        if "ADDRESS" in col.upper() or "POST_CODE" in col.upper():
            df[col] = df[col].astype(str).str.strip()

    # Handle boolean fields
    for col in ["ISACTIVE", "OPTOUT_EMARKETING"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().map({
                "YES": True, "NO": False, "TRUE": True, "FALSE": False
            }).fillna(pd.NA)

    # Parse dates
    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")

    return df


def deduplicate_contacts(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("🧹 Deduplicating contacts (keep most recent, merge missing fields)...")

    df["DEDUP_KEY"] = ""

    if "EMAIL" in df.columns:
        df["DEDUP_KEY"] = df["EMAIL"].fillna("").str.lower()

    if "FULLNAME" in df.columns or "MOBILE" in df.columns:
        mask = df["DEDUP_KEY"] == ""
        df.loc[mask, "DEDUP_KEY"] = (
            df.loc[mask, "FULLNAME"].fillna("").str.lower() + "-" +
            df.loc[mask, "MOBILE"].fillna("")
        )

    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")
        df = df.sort_values(by="LAST_UPDATED", ascending=False)

    merged_rows = []
    for key, group in df.groupby("DEDUP_KEY"):
        merged = group.iloc[0].copy()
        for col in df.columns:
            if col == "DEDUP_KEY":
                continue
            if pd.isna(merged[col]) or merged[col] in ["", "nan", "None", "NaN"]:
                for _, row in group.iterrows():
                    val = row[col]
                    if pd.notna(val) and val not in ["", "nan", "None", "NaN"]:
                        merged[col] = val
                        break
        merged_rows.append(merged)

    return pd.DataFrame(merged_rows).drop(columns=["DEDUP_KEY"])


    return df


def find_latest_csv_file(directory="data_sources"):
    files = glob.glob(os.path.join(directory, "*.csv"))
    if not files:
        raise FileNotFoundError("No CSV files found in the data_sources directory.")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


# === Main pipeline ===
try:
    input_path = find_latest_csv_file()
    logging.info(f"📥 Loading file: {input_path}")
    df = pd.read_csv(input_path)
except Exception as e:
    logging.error(f"❌ Failed to load input file: {e}")
    raise

cleaned_df = clean_fields(df)
deduped_df = deduplicate_contacts(cleaned_df)

# Export as CSV
output_path = "output/cleaned_contacts.csv"
os.makedirs("output", exist_ok=True)
try:
    deduped_df.to_csv(output_path, index=False)
    logging.info(f"✅ Cleaned + deduplicated data saved to: {output_path}")
except Exception as e:
    logging.error(f"❌ Failed to save output file: {e}")
    raise
