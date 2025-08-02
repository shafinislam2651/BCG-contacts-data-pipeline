import os
import pandas as pd
import json
import re
from collections import defaultdict
import numpy as np
import sqlite3
from pathlib import Path

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

def create_temp_database(merged_path, temp_db_path):
    """Create a temporary SQLite database from the large TSV file"""
    print(f"Creating temporary database from {merged_path}...")
    
    # Remove existing temp database
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)
    
    conn = sqlite3.connect(temp_db_path)
    
    # Read and process the large file in chunks
    chunk_size = 10000  # Process 10k rows at a time
    chunk_num = 0
    
    for chunk in pd.read_csv(merged_path, sep='\t', dtype=str, chunksize=chunk_size):
        chunk_num += 1
        print(f"Processing chunk {chunk_num} ({len(chunk)} rows)...")
        
        # Fill NaN values
        chunk = chunk.fillna('')
        
        # Add normalized columns
        chunk['_name'] = chunk.apply(lambda r: get_full_name(r, 'FIRSTNAME', 'LASTNAME', 'FULLNAME'), axis=1)
        chunk['_phone'] = chunk['MOBILE'].apply(normalize_phone) if 'MOBILE' in chunk.columns else ''
        chunk['_email'] = chunk['X_EMAIL2'].apply(normalize_value) if 'X_EMAIL2' in chunk.columns else ''
        
        # Add row index
        chunk['orig_index'] = chunk.index + (chunk_num - 1) * chunk_size
        
        # Write to SQLite
        chunk.to_sql('merged_data', conn, if_exists='append', index=False)
    
    # Create indexes for faster lookups
    print("Creating database indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON merged_data(_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON merged_data(_email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_phone ON merged_data(_phone)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orig_index ON merged_data(orig_index)")
    
    conn.commit()
    conn.close()
    print("Temporary database created successfully")

def process_source_file_chunked(src_path, temp_db_path, source_fname, change_log):
    """Process a source file against the database using chunked approach"""
    print(f"\nProcessing source file: {source_fname}")
    
    # Read source file
    src_df = pd.read_csv(src_path, sep='\t', dtype=str).fillna('')
    
    # Find columns in source file
    src_first = next((col for col in src_df.columns if col in ['First Name', 'FirstName', 'firstname']), None)
    src_last = next((col for col in src_df.columns if col in ['Last Name', 'LastName', 'lastname']), None)
    src_name = next((col for col in src_df.columns if col in ['Name', 'Full Name', 'FullName', 'fullname']), None)
    src_email = next((col for col in src_df.columns if col in ['Email Address', 'Email', 'email']), None)
    src_phones = [col for col in src_df.columns if any(p in col for p in ['Phone Number', 'Mobile Number', 'Phone', 'Mobile', 'mobile', 'phone'])]
    
    # Skip if file doesn't have required columns
    if not any([src_first and src_last, src_name]) or (not src_email and not src_phones):
        print(f"Skipping {source_fname} - missing required columns")
        return 0
    
    # Normalize source fields
    src_df['_name'] = src_df.apply(lambda r: get_full_name(r, src_first, src_last, src_name), axis=1)
    src_df['_phone'] = src_df[src_phones[0]].apply(normalize_phone) if src_phones else ''
    src_df['_email'] = src_df[src_email].apply(normalize_value) if src_email else ''
    
    conn = sqlite3.connect(temp_db_path)
    updates = 0
    
    # Process each source record
    for _, source_row in src_df.iterrows():
        source_name = source_row.get('_name', '').strip()
        source_email = source_row.get('_email', '').strip()
        source_phone = source_row.get('_phone', '').strip()
        
        if not any([source_name, source_email, source_phone]):
            continue
        
        # Build query to find matching records with missing data
        conditions = []
        params = []
        
        if source_name:
            conditions.append("_name = ?")
            params.append(source_name)
        if source_email:
            conditions.append("_email = ?")
            params.append(source_email)
        if source_phone:
            conditions.append("_phone = ?")
            params.append(source_phone)
        
        if len(conditions) < 2:
            continue  # Need at least 2 matching fields
        
        # Find records that match on at least 2 fields and have missing data
        query = f"""
        SELECT orig_index, _name, _email, _phone, FIRSTNAME, LASTNAME, X_EMAIL2, MOBILE
        FROM merged_data 
        WHERE ({' OR '.join(conditions)})
        AND (FIRSTNAME = '' OR LASTNAME = '' OR X_EMAIL2 = '' OR MOBILE = '')
        """
        
        cursor = conn.execute(query, params)
        matches = cursor.fetchall()
        
        for match in matches:
            orig_idx, target_name, target_email, target_phone = match[:4]
            current_first, current_last, current_email, current_mobile = match[4:]
            
            # Verify we have at least 2 matching fields
            match_count = 0
            matched_fields = []
            
            if source_name and target_name == source_name:
                match_count += 1
                matched_fields.append(f"name: '{source_name}'")
            if source_email and target_email == source_email:
                match_count += 1
                matched_fields.append(f"email: '{source_email}'")
            if source_phone and target_phone == source_phone:
                match_count += 1
                matched_fields.append(f"phone: '{source_phone}'")
            
            if match_count >= 2:
                match_info = " & ".join(matched_fields)
                changed = False
                
                # Update missing fields
                update_fields = []
                update_params = []
                
                if current_first == '' and src_first and source_row.get(src_first, ''):
                    update_fields.append("FIRSTNAME = ?")
                    update_params.append(source_row[src_first])
                    change_log.append({
                        'row': int(orig_idx) + 1,
                        'field': 'FIRSTNAME',
                        'old_value': '',
                        'new_value': source_row[src_first],
                        'source_file': source_fname,
                        'matched_on': match_info
                    })
                    changed = True
                
                if current_last == '' and src_last and source_row.get(src_last, ''):
                    update_fields.append("LASTNAME = ?")
                    update_params.append(source_row[src_last])
                    change_log.append({
                        'row': int(orig_idx) + 1,
                        'field': 'LASTNAME',
                        'old_value': '',
                        'new_value': source_row[src_last],
                        'source_file': source_fname,
                        'matched_on': match_info
                    })
                    changed = True
                
                if current_email == '' and src_email and source_row.get(src_email, ''):
                    update_fields.append("X_EMAIL2 = ?")
                    update_params.append(source_row[src_email])
                    change_log.append({
                        'row': int(orig_idx) + 1,
                        'field': 'X_EMAIL2',
                        'old_value': '',
                        'new_value': source_row[src_email],
                        'source_file': source_fname,
                        'matched_on': match_info
                    })
                    changed = True
                
                if current_mobile == '' and src_phones and source_row.get(src_phones[0], ''):
                    update_fields.append("MOBILE = ?")
                    update_params.append(source_row[src_phones[0]])
                    change_log.append({
                        'row': int(orig_idx) + 1,
                        'field': 'MOBILE',
                        'old_value': '',
                        'new_value': source_row[src_phones[0]],
                        'source_file': source_fname,
                        'matched_on': match_info
                    })
                    changed = True
                
                if changed and update_fields:
                    update_query = f"UPDATE merged_data SET {', '.join(update_fields)} WHERE orig_index = ?"
                    update_params.append(orig_idx)
                    conn.execute(update_query, update_params)
                    updates += 1
    
    conn.commit()
    conn.close()
    print(f"Made {updates} updates from {source_fname}")
    return updates

def export_updated_database(temp_db_path, output_path):
    """Export the updated database back to TSV format"""
    print(f"Exporting updated database to {output_path}...")
    
    conn = sqlite3.connect(temp_db_path)
    
    # Get all data except the temporary columns
    query = """
    SELECT * FROM merged_data 
    WHERE rowid IN (
        SELECT MIN(rowid) FROM merged_data GROUP BY orig_index
    )
    ORDER BY orig_index
    """
    
    # Export in chunks to avoid memory issues
    chunk_size = 10000
    first_chunk = True
    
    for chunk_df in pd.read_sql_query(query, conn, chunksize=chunk_size):
        # Remove temporary columns
        chunk_df = chunk_df.drop(columns=['_name', '_phone', '_email', 'orig_index'], errors='ignore')
        
        # Write to file
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        chunk_df.to_csv(output_path, sep='\t', index=False, mode=mode, header=header)
        first_chunk = False
    
    conn.close()

def main():
    # Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data_files')
    output_dir = os.path.join(base_dir, 'output')
    merged_path = os.path.join(output_dir, 'MergedDatabase.tsv')
    log_path = os.path.join(output_dir, 'fill_missing_log.json')
    temp_db_path = os.path.join(output_dir, 'temp_processing.db')
    
    print("=== Large Database Processing Mode ===")
    print("This script is optimized for databases that don't fit in memory")
    
    # Create temporary SQLite database
    create_temp_database(merged_path, temp_db_path)
    
    # Process all TSV files
    change_log = []
    total_files = len([f for f in os.listdir(data_dir) if f.endswith('.tsv')])
    processed = 0
    
    for fname in os.listdir(data_dir):
        if not fname.endswith('.tsv'):
            continue
            
        processed += 1
        print(f"\nProcessing file {processed}/{total_files}: {fname}")
        src_path = os.path.join(data_dir, fname)
        
        updates = process_source_file_chunked(src_path, temp_db_path, fname, change_log)
    
    # Export updated database
    export_updated_database(temp_db_path, merged_path)
    
    # Save change log
    print(f"\nWriting change log to {log_path}")
    with open(log_path, 'w') as f:
        json.dump(change_log, f, indent=2)
    
    # Clean up temporary database
    print("Cleaning up temporary files...")
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)
        
    print(f"\nDone! Made {len(change_log)} total updates across {total_files} files.")
    print("Large database processing completed successfully.")

if __name__ == '__main__':
    main()
