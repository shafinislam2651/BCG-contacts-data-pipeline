import os
import pandas as pd
import json
import re

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
        val1 = row1.get(field1, '').strip().lower()
        val2 = row2.get(field2, '').strip().lower()
        if val1 and val2 and val1 == val2:
            matches += 1
            # Print matching fields for debugging
            print(f"    Match on {field1}: '{val1}' == '{val2}'")
    if matches >= required_matches:
        print(f"    Found {matches} matches between rows!")
    return matches >= required_matches

def fill_from_source(merged_df, source_df, source_fname, merged_fields, source_fields, change_log):
    """Fill missing fields in merged_df from matching rows in source_df"""
    updates = 0
    total = len(merged_df)
    
    # Create a reference dictionary of merged_df rows that have missing data
    missing_data_rows = {}
    for idx, row in merged_df.iterrows():
        if any(pd.isna(row[field]) or row[field] == '' for field in merged_fields):
            missing_data_rows[idx] = row
    
    if not missing_data_rows:
        print("No missing data found in target fields.")
        return 0
        
    print(f"Found {len(missing_data_rows)} rows with missing data")
    processed = 0
    
    for idx, target in missing_data_rows.items():
        processed += 1
        if processed % 100 == 0:
            print(f'Checking row {processed}/{len(missing_data_rows)} for matches...')
        
        # Print the key fields we're looking to match
        print(f"\nChecking row {idx + 1}:")
        print(f"  Name: {target.get('_name', '')}")
        print(f"  Email: {target.get('_email', '')}")
        print(f"  Phone: {target.get('_phone', '')}")
            
        for _, source in source_df.iterrows():
            # Check for matches
            if has_matching_fields(target, source):
                changed = False
                # Try to fill each missing field if we have it in the source
                for m_field, s_field in zip(merged_fields, source_fields):
                    if pd.isna(target[m_field]) or target[m_field] == '':
                        s_val = source.get(s_field, '')
                        if s_val and s_val != 'nan':
                            old_val = target[m_field]
                            merged_df.at[idx, m_field] = s_val
                            print(f"  Filling {m_field}: '{old_val}' -> '{s_val}'")
                            change_log.append({
                                'row': idx + 1,
                                'field': m_field,
                                'old_value': old_val,
                                'new_value': s_val,
                                'source_file': source_fname
                            })
                            changed = True
                            
                if changed:
                    updates += 1
                    print(f"Updated row {idx + 1} with data from {source_fname}")
                
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
    
    # Find columns in merged file - handle exact column names
    first_col = 'firstname'  # Known column name in MergedDatabase.tsv
    last_col = 'lastname'    # Known column name in MergedDatabase.tsv
    name_col = 'fullname'    # Known column name in MergedDatabase.tsv
    email_col = 'x_email2'   # Using x_email2 as the main email column
    phone_cols = ['mobile', 'directphone', 'homephone', 'x_phone1', 'x_phone2', 'x_phone3', 'x_phone4', 'x_phone5']  # All possible phone columns
    
    print("\nFound columns in merged database:")
    print(f"  First Name: {first_col} ({merged_df[first_col].notna().sum()} non-empty)")
    print(f"  Last Name: {last_col} ({merged_df[last_col].notna().sum()} non-empty)")
    print(f"  Full Name: {name_col} ({merged_df[name_col].notna().sum()} non-empty)")
    print(f"  Email: {email_col} ({merged_df[email_col].notna().sum()} non-empty)")
    
    print("\nFound columns in merged database:")
    print(f"  First Name: {first_col}")
    print(f"  Last Name: {last_col}")
    print(f"  Full Name: {name_col}")
    print(f"  Email: {email_col}")
    print(f"  Phones: {phone_cols}")
    
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
        
        print(f"Found columns in {fname}:")
        print(f"  First Name: {src_first}")
        print(f"  Last Name: {src_last}")
        print(f"  Full Name: {src_name}")
        print(f"  Email: {src_email}")
        print(f"  Phones: {src_phones}")
        
        # Skip if file doesn't have required columns
        if not any([src_first and src_last, src_name]) or (not src_email and not src_phones):
            print(f"Skipping {fname} - missing required columns")
            continue
            
        print("Normalizing source fields...")
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
        for p_col in phone_cols:
            if src_phones:
                merged_fields.append(p_col)
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