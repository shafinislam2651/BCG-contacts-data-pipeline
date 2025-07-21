import pandas as pd
import re

def clean_core_fields(df: pd.DataFrame) -> pd.DataFrame:
    print("🧹 Cleaning core fields...")

    email_fields = ["EMAIL", "X_EMAIL2", "X_EMAIL3"]
    for field in email_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.strip().str.lower()
            df[field] = df[field].apply(lambda x: x if re.match(r"[^@]+@[^@]+\.[^@]+", x) else "")

    phone_fields = [
        "MOBILE", "DIRECTPHONE", "HOMEPHONE", "X_PHONE1", "X_PHONE2", "X_PHONE3", "X_PHONE4", "X_PHONE5"
    ]
    for field in phone_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).apply(lambda x: re.sub(r"\D", "", x.strip()))

    name_fields = ["FIRSTNAME", "LASTNAME", "FULLNAME", "SALUTATION", "TITLE"]
    for field in name_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.title().str.strip()

    address_fields = [col for col in df.columns if "ADDRESS" in col.upper()] + ["POST_CODE", "X_REGION"]
    for field in address_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.strip()

    social_fields = ["LINKEDIN", "TWITTER", "FACEBOOK", "SKYPE_ID", "YAHOO_ID", "MSN_ID"]
    for field in social_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.lower().str.strip()

    bool_fields = ["ISACTIVE", "SYNC_CONTACTS", "OPTOUT_EMARKETING"]
    for field in bool_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.upper().replace({
                "YES": True, "NO": False, "TRUE": True, "FALSE": False
            })

    if "LAST_UPDATED" in df.columns:
        df["LAST_UPDATED"] = pd.to_datetime(df["LAST_UPDATED"], errors="coerce")

    return df

def deduplicate_contacts(df: pd.DataFrame) -> pd.DataFrame:
    print("🧹 Deduplicating contacts (keep most recent)...")

    # Create a unique deduplication key based on EMAIL or FULLNAME + MOBILE
    df["DEDUP_KEY"] = df["EMAIL"].fillna("").str.lower()
    df["DEDUP_KEY"] = df["DEDUP_KEY"].where(df["DEDUP_KEY"] != "", df["FULLNAME"].str.lower().fillna("") + "-" + df["MOBILE"].fillna(""))

    # Sort by LAST_UPDATED descending to keep the most recent
    df = df.sort_values(by="LAST_UPDATED", ascending=False)

    # Drop duplicates using dedup key
    df = df.drop_duplicates(subset="DEDUP_KEY", keep="first")

    # Drop helper column
    df = df.drop(columns=["DEDUP_KEY"])

    return df

# === Load, clean, deduplicate, and export ===
df = pd.read_excel("data_sources/EXO CONTACT PULL 19-3-2025.xlsx")


cleaned_df = clean_core_fields(df)
deduplicated_df = deduplicate_contacts(cleaned_df)

deduplicated_df.to_excel("data_sources/EXO_CONTACT_CLEANED_DEDUPED.xlsx", index=False)

print("✅ Cleaning + deduplication complete. File saved as 'EXO_CONTACT_CLEANED_DEDUPED.xlsx'")

