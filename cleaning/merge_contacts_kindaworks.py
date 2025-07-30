import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# File paths
mailchimp_path = "data_sources/mailchimpclean.tsv"
merged_path = "output/MergedDatabase.tsv"
output_dir = "output"
output_path = os.path.join(output_dir, "MergedDatabase.tsv")

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Load the datasets
mailchimp_df = pd.read_csv(mailchimp_path, sep='\t', low_memory=False)
merged_df = pd.read_csv(merged_path, sep='\t', low_memory=False)

# Standardize column names and values (strip, lower)
def normalize(val):
    if pd.isna(val):
        return ""
    return str(val).strip().lower()

def get_name(df):
    # Always combine first and last name columns if available
    if 'firstname' in df.columns and 'lastname' in df.columns:
        return (df['firstname'].fillna('') + ' ' + df['lastname'].fillna('')).str.strip()
    elif 'first name' in df.columns and 'last name' in df.columns:
        return (df['first name'].fillna('') + ' ' + df['last name'].fillna('')).str.strip()
    elif 'fullname' in df.columns:
        return df['fullname'].fillna('').astype(str).str.strip()
    elif 'name' in df.columns:
        return df['name'].fillna('').astype(str).str.strip()
    else:
        return pd.Series([''] * len(df))

def get_mobile(df):
    if 'mobile' in df.columns:
        return df['mobile']
    elif 'number' in df.columns:
        return df['number']
    else:
        return pd.Series([''] * len(df))

def get_email(df):
    if 'email address' in df.columns:
        return df['email address']
    elif 'email' in df.columns:
        return df['email']
    else:
        return pd.Series([''] * len(df))

mailchimp_df.columns = mailchimp_df.columns.str.strip().str.lower()
merged_df.columns = merged_df.columns.str.strip().str.lower()

mailchimp_df["name"] = get_name(mailchimp_df)
merged_df["name"] = get_name(merged_df)

mailchimp_df["mobile"] = get_mobile(mailchimp_df)
merged_df["mobile"] = get_mobile(merged_df)

mailchimp_df["email"] = get_email(mailchimp_df)
merged_df["email"] = get_email(merged_df)

mailchimp_df["name_norm"] = mailchimp_df["name"].apply(normalize)
mailchimp_df["mobile_norm"] = mailchimp_df["mobile"].apply(normalize)
mailchimp_df["email_norm"] = mailchimp_df["email"].apply(normalize)

merged_df["name_norm"] = merged_df["name"].apply(normalize)
merged_df["mobile_norm"] = merged_df["mobile"].apply(normalize)
merged_df["email_norm"] = merged_df["email"].apply(normalize)

# Fill missing contact info and log changes (case-insensitive, trimmed, only if full name exists and matches)
for idx, row in merged_df.iterrows():
    name = row.get("name_norm")
    mobile = row.get("mobile_norm")
    email = row.get("email_norm")
    if not name:
        continue  # skip rows with no full name

    # Fill missing email if full name and mobile match
    if (not row.get("email")) or pd.isna(row.get("email")) or str(row.get("email")).strip() == "":
        match = mailchimp_df[(mailchimp_df["name_norm"] == name) & (mailchimp_df["mobile_norm"] == mobile)]
        if not match.empty and pd.notna(match.iloc[0]["email"]) and match.iloc[0]["email"]:
            merged_df.at[idx, "email"] = match.iloc[0]["email"]
            logging.info(f"Filled missing email for '{row.get('name')}' (mobile: {row.get('mobile')}) with '{match.iloc[0]['email']}'")

    # Fill missing mobile if full name and email match
    if (not row.get("mobile")) or pd.isna(row.get("mobile")) or str(row.get("mobile")).strip() == "":
        match = mailchimp_df[(mailchimp_df["name_norm"] == name) & (mailchimp_df["email_norm"] == email)]
        if not match.empty and pd.notna(match.iloc[0]["mobile"]) and match.iloc[0]["mobile"]:
            merged_df.at[idx, "mobile"] = match.iloc[0]["mobile"]
            logging.info(f"Filled missing mobile for '{row.get('name')}' (email: {row.get('email')}) with '{match.iloc[0]['mobile']}'")

# Drop normalization columns before saving
merged_df = merged_df.drop(columns=["name_norm", "mobile_norm", "email_norm"])

# Save merged data
merged_df.to_csv(output_path, sep='\t', index=False)
logging.info(f"Merged file saved to {output_path}")
logging.info("Script execution completed successfully.")