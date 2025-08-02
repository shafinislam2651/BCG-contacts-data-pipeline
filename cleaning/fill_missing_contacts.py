import os
import pandas as pd
import json
import re
from collections import defaultdict
import numpy as np

def normalize_value(val):
    """Normalize a string value by stripping whitespace and converting to lowercase"""
    if pd.isna(val) or val == '':
        return ''
    return re.sub(r'\s+', ' ', str(val).strip().lower())

def normalize_phone(val):
    """Extract digits from phone number and normalize format"""
    if pd.isna(val) or val == '':
        return ''
    digits = re.sub(r'\D', '', str(val))
    if len(digits) >= 10:
        return digits[-10:]  # Keep last 10 digits
    return digits if digits else ''

def get_full_name(row, first_col=None, last_col=None, name_col=None):
    """Get normalized full name from row using available name columns"""
    if first_col and last_col:
        first = normalize_value(row.get(first_col, ''))
        last = normalize_value(row.get(last_col, ''))
        if first or last:
            return f"{first} {last}".strip()
    if name_col:
        return normalize_value(row.get(name_col, ''))
    return ''

def has_matching_fields(row1, row2, required_matches=2):
    """Check if two rows match on at least required_matches fields"""
    matches = 0
    fields = [('_name', '_name'), ('_phone', '_phone'), ('_email', '_email')]
    for field1, field2 in fields:
        val1 = row1.get(field1, '')
        val2 = row2.get(field2, '')
        if val1 and val2 and val1 == val2:
            matches += 1
            if matches >= required_matches:
                return True
    return False

def fill_from_source(merged_df, source_df, source_fname, merged_fields, source_fields, change_log):
    """Fill missing fields in merged_df from matching rows in source_df"""
    updates = 0
    
    # Pre-filter to only rows with missing data
    missing_mask = merged_df[merged_fields].isnull().any(axis=1) | (merged_df[merged_fields] == '').any(axis=1)
    missing_indices = merged_df.index[missing_mask]
    
    if len(missing_indices) == 0:
        return 0
        
    print(f"Processing {len(missing_indices)} rows with missing data from {source_fname}")
    
    # Create lookup dictionaries for faster matching
    source_lookups = {
        'name': defaultdict(list),
        'email': defaultdict(list), 
        'phone': defaultdict(list)
    }
    
    # Build lookups once
    for idx, row in source_df.iterrows():
        name = row.get('_name', '').strip()
        email = row.get('_email', '').strip()
        phone = row.get('_phone', '').strip()
        
        if name:
            source_lookups['name'][name].append(idx)
        if email:
            source_lookups['email'][email].append(idx)
        if phone:
            source_lookups['phone'][phone].append(idx)
    
    # Process missing rows in batches for better performance
    for idx in missing_indices:
        target = merged_df.loc[idx]
        target_name = target.get('_name', '').strip()
        target_email = target.get('_email', '').strip()
        target_phone = target.get('_phone', '').strip()
        
        # Find candidate matches efficiently
        candidates = set()
        if target_name in source_lookups['name']:
            candidates.update(source_lookups['name'][target_name])
        if target_email in source_lookups['email']:
            candidates.update(source_lookups['email'][target_email])
        if target_phone in source_lookups['phone']:
            candidates.update(source_lookups['phone'][target_phone])
        
        # Check each candidate for a valid match
        for candidate_idx in candidates:
            source_row = source_df.loc[candidate_idx]
            
            # Quick match check - need at least 2 matching fields
            match_count = 0
            matched_fields = []
            
            if target_name and source_row.get('_name', '').strip() == target_name:
                match_count += 1
                matched_fields.append(f"name: '{target_name}'")
            if target_email and source_row.get('_email', '').strip() == target_email:
                match_count += 1
                matched_fields.append(f"email: '{target_email}'")
            if target_phone and source_row.get('_phone', '').strip() == target_phone:
                match_count += 1
                matched_fields.append(f"phone: '{target_phone}'")
                
            if match_count >= 2:
                # Fill missing fields
                changed = False
                match_info = " & ".join(matched_fields)
                
                for m_field, s_field in zip(merged_fields, source_fields):
                    if pd.isna(target[m_field]) or target[m_field] == '':
                        s_val = source_row.get(s_field, '')
                        if s_val and str(s_val) != 'nan':
                            merged_df.at[idx, m_field] = s_val
                            change_log.append({
                                'row': int(idx) + 1,
                                'field': m_field,
                                'old_value': target[m_field],
                                'new_value': s_val,
                                'source_file': source_fname,
                                'matched_on': match_info
                            })
                            changed = True
                            
                if changed:
                    updates += 1
                    break  # Stop after first successful match
                
    return updates

def main():
    # Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data_files')
    output_dir = os.path.join(base_dir, 'output')
    merged_path = os.path.join(output_dir, 'MergedDatabase.tsv')
    log_path = os.path.join(output_dir, 'fill_missing_log.json')
    
    # Load merged database
    print(f"\nLoading {merged_path}...")
    merged_df = pd.read_csv(merged_path, sep='\t', dtype=str).fillna('')
    
    # Find columns in merged file - handle exact column names (uppercase)
    first_col = 'FIRSTNAME'  # Known column name in MergedDatabase.tsv
    last_col = 'LASTNAME'    # Known column name in MergedDatabase.tsv
    name_col = 'FULLNAME'    # Known column name in MergedDatabase.tsv
    email_col = 'X_EMAIL2'   # Using X_EMAIL2 as the main email column
    phone_cols = ['MOBILE', 'DIRECTPHONE', 'HOMEPHONE', 'X_PHONE1', 'X_PHONE2', 'X_PHONE3', 'X_PHONE4', 'X_PHONE5']  # All possible phone columns
    
    print("\nFound columns in merged database:")
    print(f"  First Name: {first_col} ({merged_df[first_col].notna().sum()} non-empty)")
    print(f"  Last Name: {last_col} ({merged_df[last_col].notna().sum()} non-empty)")
    print(f"  Full Name: {name_col} ({merged_df[name_col].notna().sum()} non-empty)")
    print(f"  Email: {email_col} ({merged_df[email_col].notna().sum()} non-empty)")
    
    print("\nNormalizing fields in merged database...")
    merged_df['_name'] = merged_df.apply(lambda r: get_full_name(r, first_col, last_col, name_col), axis=1)
    merged_df['_phone'] = merged_df[phone_cols[0]].apply(normalize_phone) if phone_cols else ''
    merged_df['_email'] = merged_df[email_col].apply(normalize_value) if email_col else ''
    
    # Process all TSV files in data_files directory
    change_log = []
    total_files = len([f for f in os.listdir(data_dir) if f.endswith('.tsv')])
    processed = 0
    
    for fname in os.listdir(data_dir):
        if not fname.endswith('.tsv'):
            continue
            
        processed += 1
        print(f"\nProcessing file {processed}/{total_files}: {fname}")
        src_path = os.path.join(data_dir, fname)
        
        # Read and process source file
        src_df = pd.read_csv(src_path, sep='\t', dtype=str).fillna('')
        
        # Find columns in source file
        # Check all common variations of column names
        src_first = next((col for col in src_df.columns if col in ['First Name', 'FirstName', 'firstname']), None)
        src_last = next((col for col in src_df.columns if col in ['Last Name', 'LastName', 'lastname']), None)
        src_name = next((col for col in src_df.columns if col in ['Name', 'Full Name', 'FullName', 'fullname']), None)
        src_email = next((col for col in src_df.columns if col in ['Email Address', 'Email', 'email']), None)
        src_phones = [col for col in src_df.columns if any(p in col for p in ['Phone Number', 'Mobile Number', 'Phone', 'Mobile', 'mobile', 'phone'])]
        
        # Skip if file doesn't have required columns
        if not any([src_first and src_last, src_name]) or (not src_email and not src_phones):
            print(f"Skipping {fname} - missing required columns")
            continue
            
        # Normalize source fields
        src_df['_name'] = src_df.apply(lambda r: get_full_name(r, src_first, src_last, src_name), axis=1)
        src_df['_phone'] = src_df[src_phones[0]].apply(normalize_phone) if src_phones else ''
        src_df['_email'] = src_df[src_email].apply(normalize_value) if src_email else ''
        
        # Define field mappings
        merged_fields = []
        source_fields = []
        
        if first_col and src_first:
            merged_fields.append(first_col)
            source_fields.append(src_first)
        if last_col and src_last:
            merged_fields.append(last_col)
            source_fields.append(src_last)
        if email_col and src_email:
            merged_fields.append(email_col)
            source_fields.append(src_email)
        # Only add the first phone field instead of all phone fields
        if phone_cols and src_phones:
            merged_fields.append(phone_cols[0])  # Only use the first phone column (MOBILE)
            source_fields.append(src_phones[0])
        
        # Fill missing fields from this source
        updates = fill_from_source(
            merged_df, src_df, fname,
            merged_fields, source_fields,
            change_log
        )
        print(f"Made {updates} updates from {fname}")
    
    # Drop temporary columns and save results
    merged_df.drop(columns=['_name', '_phone', '_email'], inplace=True)
    
    # Save updated database
    print(f"\nSaving updated database to {merged_path}")
    merged_df.to_csv(merged_path, sep='\t', index=False)
    
    # Save change log
    print(f"Writing change log to {log_path}")
    with open(log_path, 'w') as f:
        json.dump(change_log, f, indent=2)
        
    print(f"\nDone! Made {len(change_log)} total updates across {total_files} files.")

if __name__ == '__main__':
    main()