import pandas as pd
import os
import json
import logging
from tqdm import tqdm  # Add tqdm for progress bar
from rapidfuzz import fuzz  # Add rapidfuzz for fuzzy matching

def normalize(val):
    if pd.isna(val):
        return ""
    return str(val).strip().lower()

def get_name(row):
    if 'fullname' in row and row['fullname']:
        return str(row['fullname']).strip()
    elif 'FIRSTNAME' in row and 'LASTNAME' in row:
        return f"{str(row.get('FIRSTNAME','')).strip()} {str(row.get('LASTNAME','')).strip()}".strip()
    elif 'name' in row and row['name']:
        return str(row['name']).strip()
    return ""

def get_number(row):
    for key in ['MOBILE', 'mobile', 'number']:
        if key in row and row[key]:
            return str(row[key]).strip()
    return ""

def get_email(row):
    for key in ['EMAIL', 'email', 'email address']:
        if key in row and row[key]:
            return str(row[key]).strip()
    return ""

def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    merged_path = "output/MergedDatabase.tsv"
    errors_path = "output/validation_errors.json"
    # Use only 1.tsv as the auxiliary data source
    aux_path = os.path.join("data_files", "1.tsv")

    # Load merged database and validation errors
    merged_df = pd.read_csv(merged_path, sep='\t', dtype=str).fillna("")
    with open(errors_path) as f:
        errors = json.load(f)

    # Build a list of incomplete row indices
    incomplete_indices = set()
    for err in errors:
        idx = err['row'] - 1  # validation is 1-based
        incomplete_indices.add(idx)

    # Load only data_files/1.tsv into a DataFrame
    if os.path.exists(aux_path):
        aux_df = pd.read_csv(aux_path, sep='\t', dtype=str).fillna("")
    else:
        aux_df = pd.DataFrame()

    # Precompute normalized fields for aux_df
    aux_df['name_norm'] = aux_df.apply(get_name, axis=1).str.lower().str.strip()
    aux_df['number_norm'] = aux_df.apply(get_number, axis=1).str.lower().str.strip()
    aux_df['email_norm'] = aux_df.apply(get_email, axis=1).str.lower().str.strip()

    updated = 0
    # Wrap the loop with tqdm for a progress bar
    for idx in tqdm(incomplete_indices, desc="Filling missing contacts"):
        row = merged_df.iloc[idx]
        name = get_name(row)
        number = get_number(row)
        email = get_email(row)
        name_norm = normalize(name)
        number_norm = normalize(number)
        email_norm = normalize(email)
        # Find strong matches: at least 2/3 fields match and the third is not empty in aux_df
        candidates = []
        for _, aux_row in aux_df.iterrows():
            match_count = 0
            # Fuzzy match for name (threshold 90)
            if name_norm and aux_row['name_norm'] and fuzz.ratio(name_norm, aux_row['name_norm']) >= 90:
                match_count += 1
            if number_norm and number_norm == aux_row['number_norm']:
                match_count += 1
            if email_norm and email_norm == aux_row['email_norm']:
                match_count += 1
            if match_count >= 2:
                candidates.append(aux_row)
        if not candidates:
            continue
        # Use the first strong match
        best = candidates[0]
        changed = False
        # Fill missing name
        if not name and best['name_norm']:
            merged_df.at[idx, 'fullname'] = best['name_norm'].title()
            changed = True
        # Fill missing number
        if not number and best['number_norm']:
            merged_df.at[idx, 'MOBILE'] = best['number_norm']
            changed = True
        # Fill missing email
        if not email and best['email_norm']:
            merged_df.at[idx, 'EMAIL'] = best['email_norm']
            changed = True
        if changed:
            updated += 1
            logging.info(f"Row {idx+1}: filled missing fields from strong match in 1.tsv.")

    merged_df.to_csv(merged_path, sep='\t', index=False)
    logging.info(f"Updated {updated} rows in {merged_path}.")

if __name__ == "__main__":
    main()
