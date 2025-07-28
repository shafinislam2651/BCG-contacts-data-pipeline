import pandas as pd
import re
import logging
import os
import glob
from typing import Optional

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Fields to be set explicitly to NULL (dropped from use)
UNUSED_FIELDS = [
    "SALUTATION", "DIRECTPHONE", "DIRECTFAX", "HOMEPHONE",
    "MSN_ID", "YAHOO_ID", "SKYPE_ID", "SYNC_CONTACTS", "LINKEDIN",
    "TWITTER", "FACEBOOK", "CAMPAIGN_WAVE_SEQNO", "LATITUDE", "LONGITUDE",
    "GEOCODE_STATUS", "X_STORE", "X_EMAIL2", "X_EMAIL3",
    "X_PHONE1", "X_PHONE2", "X_PHONE3", "X_PHONE4", "X_PHONE5", "X_TT_EXTENSION"
] + [f"SUB{i}" for i in range(1, 27)] + ["X_REGION"]

def clean_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize all fields in the dataframe."""
    logging.info(f"🧹 Cleaning {len(df)} records with {len(df.columns)} columns...")

    # Set unused fields to NULL if they exist
    nullified_count = 0
    for field in UNUSED_FIELDS:
        if field in df.columns:
            df[field] = pd.NA
            nullified_count += 1
    
    if nullified_count > 0:
        logging.info(f"✅ Nullified {nullified_count} unused fields")

    # Clean email fields
    email_cols = [col for col in df.columns if "EMAIL" in col.upper()]
    for col in email_cols:
        original_count = df[col].notna().sum()
        df[col] = df[col].astype(str).str.strip().str.lower()
        df[col] = df[col].apply(lambda x: x if re.match(r"[^@]+@[^@]+\.[^@]+", x) else pd.NA)
        valid_count = df[col].notna().sum()
        logging.info(f"📧 Cleaned {col}: {original_count} → {valid_count} valid emails")

    # Clean phone fields
    phone_cols = [col for col in df.columns if "PHONE" in col.upper()]
    for col in phone_cols:
        original_count = df[col].notna().sum()
        df[col] = df[col].astype(str).apply(
            lambda x: (
                digits if (digits := re.sub(r"\D", "", x)) and len(digits) >= 8 else pd.NA
            ) if pd.notna(x) and re.sub(r"\D", "", x) else pd.NA
        )
        valid_count = df[col].notna().sum()
        logging.info(f"📞 Cleaned {col}: {original_count} → {valid_count} valid phones")

    # Clean name/title fields
    name_cols = [col for col in df.columns if col.upper() in ["FIRSTNAME", "LASTNAME", "FULLNAME", "TITLE"]]
    for col in name_cols:
        df[col] = df[col].astype(str).str.title().str.strip()
        logging.info(f"👤 Cleaned {col}")

    # Clean address/postcode
    address_cols = [col for col in df.columns if "ADDRESS" in col.upper() or "POST_CODE" in col.upper()]
    for col in address_cols:
        df[col] = df[col].astype(str).str.strip()
        logging.info(f"📍 Cleaned {col}")

    # Handle boolean fields
    for col in ["ISACTIVE", "OPTOUT_EMARKETING"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().map({
                "YES": True, "NO": False, "TRUE": True, "FALSE": False
            }).fillna(pd.NA)
            logging.info(f"✅ Standardized boolean field {col}")

    # Parse dates
    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")
        valid_dates = df["LAST_UPDATED"].notna().sum()
        logging.info(f"📅 Parsed {valid_dates} valid dates in LAST_UPDATED")

    return df


def deduplicate_contacts(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate contacts based on email and name+mobile combinations."""
    original_count = len(df)
    logging.info(f"🧹 Deduplicating {original_count} contacts...")

    df = df.copy()
    df["DEDUP_KEY"] = ""

    # Create deduplication keys
    if "EMAIL" in df.columns:
        df["DEDUP_KEY"] = df["EMAIL"].fillna("").str.lower()
        email_keys = (df["DEDUP_KEY"] != "").sum()
        logging.info(f"📧 Created {email_keys} deduplication keys based on email")

    if "FULLNAME" in df.columns or "MOBILE" in df.columns:
        mask = df["DEDUP_KEY"] == ""
        df.loc[mask, "DEDUP_KEY"] = (
            df.loc[mask, "FULLNAME"].fillna("").str.lower() + "-" +
            df.loc[mask, "MOBILE"].fillna("")
        )
        name_mobile_keys = (df["DEDUP_KEY"] != "").sum() - email_keys
        logging.info(f"👤 Created {name_mobile_keys} deduplication keys based on name+mobile")

    # Sort by last updated to keep most recent
    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")
        df = df.sort_values(by="LAST_UPDATED", ascending=False)

    # Merge duplicate records
    merged_rows = []
    total_groups = df.groupby("DEDUP_KEY").ngroups
    
    for i, (key, group) in enumerate(df.groupby("DEDUP_KEY"), 1):
        if i % 1000 == 0:
            logging.info(f"🔄 Processing group {i}/{total_groups}")
            
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

    result_df = pd.DataFrame(merged_rows).drop(columns=["DEDUP_KEY"])
    final_count = len(result_df)
    duplicates_removed = original_count - final_count
    
    logging.info(f"✅ Deduplication complete: {original_count} → {final_count} records ({duplicates_removed} duplicates removed)")
    
    return result_df


def find_latest_tsv_file(directory: str = "data_sources") -> str:
    """Find the most recent TSV file in the specified directory."""
    files = glob.glob(os.path.join(directory, "*.tsv"))
    if not files:
        raise FileNotFoundError(f"No TSV files found in the {directory} directory.")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def main():
    """Main execution function with comprehensive error handling."""
    try:
        # Find and load input file
        input_path = find_latest_tsv_file()
        logging.info(f"📥 Loading file: {input_path}")
        
        # Check file size
        file_size = os.path.getsize(input_path) / (1024 * 1024)  # MB
        logging.info(f"📊 File size: {file_size:.2f} MB")
        
        df = pd.read_csv(input_path, sep="\t")
        logging.info(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")
        
        # Clean the data
        cleaned_df = clean_fields(df)
        
        # Deduplicate the data
        deduped_df = deduplicate_contacts(cleaned_df)
        
        # Export as TSV
        output_path = "output/cleaned_contacts.tsv"
        os.makedirs("output", exist_ok=True)
        
        deduped_df.to_csv(output_path, index=False, sep="\t")
        
        # Report final statistics
        output_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        logging.info(f"✅ Successfully saved {len(deduped_df)} records to: {output_path}")
        logging.info(f"📊 Output file size: {output_size:.2f} MB")
        
        # Show sample of cleaned data
        logging.info(f"📋 Sample of cleaned data:")
        logging.info(f"   Columns: {list(deduped_df.columns)}")
        logging.info(f"   First few records:")
        for i, row in deduped_df.head(3).iterrows():
            logging.info(f"   Record {i+1}: {dict(row.head(5))}")
            
    except FileNotFoundError as e:
        logging.error(f"❌ File not found: {e}")
        raise
    except pd.errors.EmptyDataError:
        logging.error("❌ The input file is empty or contains no valid data")
        raise
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
