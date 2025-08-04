import pandas as pd
import re
import logging
import os
import glob
from typing import Optional

# Logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Fields to be set explicitly to NULL 
UNUSED_FIELDS = [
    "SALUTATION", "DIRECTPHONE", "DIRECTFAX", "HOMEPHONE",
    "MSN_ID", "YAHOO_ID", "SKYPE_ID", "SYNC_CONTACTS", "LINKEDIN",
    "TWITTER", "FACEBOOK", "CAMPAIGN_WAVE_SEQNO", "LATITUDE", "LONGITUDE",
    "GEOCODE_STATUS", "X_STORE", "X_EMAIL2", "X_EMAIL3",
    "X_PHONE1", "X_PHONE2", "X_PHONE3", "X_PHONE4", "X_PHONE5", "X_TT_EXTENSION",
    "X_REGION"
]

def preserve_integer_values(df: pd.DataFrame) -> pd.DataFrame:
    """Preserve integer values and prevent float conversion for numeric fields, except SUB fields which are nulled."""
    logging.info("🔢 Preserving integer values...")
    
    # Identify numeric columns that should remain as integers
    numeric_cols = []
    for col in df.columns:
        if col in ["SEQNO", "SALESNO", "COMPANY_ACCNO"]:
            numeric_cols.append(col)
        elif col.startswith("SUB"):
            continue  
        elif df[col].dtype in ['int64', 'float64']:
            # Check if the column contains only whole numbers
            if df[col].notna().any():
                if df[col].dtype == 'float64':
                    # Check if all non-null values are whole numbers
                    non_null_vals = df[col].dropna()
                    if len(non_null_vals) > 0 and (non_null_vals % 1 == 0).all():
                        numeric_cols.append(col)
    
    # Convert to integers where appropriate
    for col in numeric_cols:
        if col in df.columns:
            try:
                # Convert to integer, handling NaN values
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                logging.info(f"✅ Preserved integer format for {col}")
            except Exception as e:
                logging.warning(f"⚠️ Could not convert {col} to integer: {e}")
    
    return df

def reset_seq_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Reset SEQ numbers to be in proper descending order (1, 2, 3, 4...)."""
    logging.info("🔄 Resetting SEQ numbers to proper order...")
    
    # Reset the main SEQNO column
    if "SEQNO" in df.columns:
        df["SEQNO"] = range(1, len(df) + 1)
        logging.info(f"✅ Reset SEQNO to range 1-{len(df)}")
    
    # Reset SUB columns if they exist and contain sequence numbers
    sub_cols = [f"SUB{i}" for i in range(1, 27)]
    for col in sub_cols:
        if col in df.columns:
            # Check if this SUB column contains sequence-like data
            if df[col].notna().any():
                try:
                    # Convert to numeric and check if it looks like sequence data
                    numeric_vals = pd.to_numeric(df[col], errors='coerce')
                    if numeric_vals.notna().any():
                        # Only reset if the values look like they could be sequence numbers
                        # (i.e., they are mostly sequential or have gaps that suggest sequence data)
                        unique_vals = numeric_vals.dropna().unique()
                        if len(unique_vals) > 1 and max(unique_vals) <= len(df) * 2:
                            # Reset to sequential numbers starting from 1
                            df[col] = range(1, len(df) + 1)
                            logging.info(f"✅ Reset {col} to sequential order")
                except Exception as e:
                    logging.warning(f"⚠️ Could not reset {col}: {e}")
    
    return df

def clean_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardise all fields in the dataframe."""
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
    for col in df.columns:
        if "PHONE" in col.upper():
            df[col] = df[col].astype(str).apply(lambda x: re.sub(r"\D", "", x.strip()) if pd.notna(x) else pd.NA)

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
    boolean_fields = ["ISACTIVE", "OPTOUT_EMARKETING"] + [f"SUB{i}" for i in range(1, 27)]
    for col in boolean_fields:
        if col in df.columns:
            original_count = df[col].notna().sum()
            original_values = df[col].value_counts().head(5).to_dict()
            
            # Convert to string and standardize to Y/N format
            df[col] = df[col].astype(str).str.strip().str.upper()
            
            # Keep only Y or N values, set everything else to null
            df[col] = df[col].apply(lambda x: x if x in ["Y", "N"] else pd.NA)
            
            final_count = df[col].notna().sum()
            y_count = (df[col] == "Y").sum()
            n_count = (df[col] == "N").sum()
            
            logging.info(f"✅ Standardised boolean field {col}: {original_count} → {final_count} valid values")
            logging.info(f"   Original values: {original_values}")
            logging.info(f"   Final: {y_count} Y, {n_count} N, {final_count - y_count - n_count} null")
  
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
            
        # Create a comprehensive merged record by combining all available data
        merged = {}
        
        # For each column, find the best value from all duplicates
        for col in df.columns:
            if col == "DEDUP_KEY":
                continue
                
            # Collect all non-null values from all duplicates for this column
            available_values = []
            for _, row in group.iterrows():
                val = row[col]
                if pd.notna(val) and val not in ["", "nan", "None", "NaN"]:
                    available_values.append(val)
            
            # Choose the best value based on priority:
            # 1. First non-null value (from most recent record if sorted by LAST_UPDATED)
            # 2. If multiple values exist, prefer the longest/most complete one
            if available_values:
                if len(available_values) == 1:
                    merged[col] = available_values[0]
                else:
                    # If multiple values, choose the most complete one
                    best_value = max(available_values, key=lambda x: len(str(x)) if pd.notna(x) else 0)
                    merged[col] = best_value
                    logging.debug(f"📝 Merged {col}: found {len(available_values)} values, chose '{best_value}'")
            else:
                merged[col] = pd.NA
        
        merged_rows.append(merged)

    return pd.DataFrame(merged_rows).drop(columns=["DEDUP_KEY"])


    return df


def find_latest_tsv_file(directory="data_sources"):
    """Find the most recent TSV file in the specified directory."""
    files = glob.glob(os.path.join(directory, "*.tsv"))
    if not files:
        raise FileNotFoundError(f"No TSV files found in the {directory} directory.")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


# === Main pipeline ===
try:
    input_path = find_latest_tsv_file()
    logging.info(f"📥 Loading file: {input_path}")
    df = pd.read_csv(input_path, sep='\t')
except Exception as e:
    logging.error(f"❌ Failed to load input file: {e}")
    raise

cleaned_df = clean_fields(df)
deduped_df = deduplicate_contacts(cleaned_df)

# Export as TSV
output_path = "output/cleaned_contacts.tsv"
os.makedirs("output", exist_ok=True)
try:
    deduped_df.to_tsv(output_path, index=False, sep='\t')
    logging.info(f"✅ Cleaned + deduplicated data saved to: {output_path}")
except Exception as e:
    logging.error(f"❌ Failed to save output file: {e}")
    raise
