import pandas as pd
import re
import sys
import os
import json

REQUIRED_FIELDS = ["FIRSTNAME", "LASTNAME", "EMAIL"]
EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"
PHONE_FIELDS = [col for col in ["MOBILE", "DIRECTPHONE", "HOMEPHONE"]]


def validate_email(email):
    if pd.isna(email) or email == "":
        return False
    return bool(re.match(EMAIL_REGEX, str(email).strip()))


def validate_phone(phone):
    if pd.isna(phone) or phone == "":
        return True  # Allow empty
    digits = re.sub(r"\D", "", str(phone))
    return 7 <= len(digits) <= 15


def main():
    """Validate the cleaned contacts TSV file"""
    # Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(base_dir, 'output', 'cleaned_contacts.tsv')
    output_path = os.path.join(base_dir, 'output', 'validation_errors.json')
    
    # Check if input file exists
    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        print("Please run clean_contacts.py first to generate cleaned_contacts.tsv")
        sys.exit(1)
    
    print(f"📄 Validating: {input_path}")
    
    # Read the TSV file
    try:
        df = pd.read_csv(input_path, sep='\t')
        print(f"📊 Loaded {len(df)} records with {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)
    
    errors = []
    # Normalize columns for case-insensitive matching
    col_map = {col.lower(): col for col in df.columns}
    email_col = None
    for possible in ["email", "e-mail", "EMAIL"]:
        if possible.lower() in col_map:
            email_col = col_map[possible.lower()]
            break
    first_col = col_map.get("firstname", None)
    last_col = col_map.get("lastname", None)
    phone_cols = [col_map.get(f.lower(), None) for f in ["MOBILE", "DIRECTPHONE", "HOMEPHONE"] if col_map.get(f.lower(), None)]

    for idx, row in df.iterrows():
        row_errors = []
        # Compose full name for reporting
        full_name = ''
        first_val = str(row.get(first_col, '')).strip() if first_col else ''
        last_val = str(row.get(last_col, '')).strip() if last_col else ''
        if first_col and last_col:
            full_name = f"{first_val} {last_val}".strip()
        elif 'fullname' in col_map:
            full_name = str(row.get(col_map['fullname'], '')).strip()
        elif 'name' in col_map:
            full_name = str(row.get(col_map['name'], '')).strip()

        email_val = str(row.get(email_col, '')).strip() if email_col else ''
        phone_vals = [str(row.get(phone_field, '')).strip() if phone_field else '' for phone_field in phone_cols]

        # Check if all fields are missing or name is 'nan nan' or equivalent
        all_missing = (
            (first_val == '' or first_val.lower() == 'nan') and
            (last_val == '' or last_val.lower() == 'nan') and
            (email_val == '' or email_val.lower() == 'nan') and
            all(pv == '' or pv.lower() == 'nan' for pv in phone_vals)
        )
        null_name = (full_name == '' or full_name.lower() == 'nan nan' or full_name.lower() == 'nan')
        if all_missing or null_name:
            continue  # skip this row

        # Check required fields
        if first_col and (pd.isna(row.get(first_col, '')) or first_val == ""):
            row_errors.append("Missing FIRSTNAME")
        if last_col and (pd.isna(row.get(last_col, '')) or last_val == ""):
            row_errors.append("Missing LASTNAME")
        if email_col and (pd.isna(row.get(email_col, '')) or email_val == ""):
            row_errors.append("Missing EMAIL")
        elif not email_col:
            row_errors.append("Missing EMAIL column")

        # Check email format
        if email_col and not pd.isna(row.get(email_col, '')) and email_val != "" and not validate_email(row.get(email_col, '')):
            row_errors.append("Invalid email format")

        # Check phone fields format (MOBILE, DIRECTPHONE, HOMEPHONE)
        phone_present = False
        for i, phone_field in enumerate(phone_cols):
            if phone_field:
                phone_val = phone_vals[i]
                if phone_val != "" and phone_val.lower() != "nan":
                    phone_present = True
                    if not validate_phone(phone_val):
                        row_errors.append(f"Invalid phone in {phone_field}")
        # If no phone number is present, report missing phone
        if not phone_present:
            row_errors.append("Missing phone number (MOBILE, DIRECTPHONE, or HOMEPHONE)")

        # Only report errors if any required field is missing or invalid
        if row_errors:
            errors.append({"row": idx+1, "name": full_name, "errors": row_errors})
    
    # Print validation results
    print(f"\n📈 VALIDATION RESULTS:")
    print(f"   Total rows: {len(df)}")
    print(f"   Rows with errors: {len(errors)}")
    print(f"   Success rate: {((len(df) - len(errors)) / len(df) * 100):.1f}%")
    
    if errors:
        print(f"\n❌ VALIDATION ERRORS:")
        for err in errors[:10]:  # Show first 10 errors
            print(f"   Row {err['row']} ({err['name']}): {', '.join(err['errors'])}")
        
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
    else:
        print("\n✅ All rows passed validation!")
    
    # Save errors to JSON file
    try:
        with open(output_path, "w") as f:
            json.dump(errors, f, indent=2)
        print(f"\n📄 Validation report saved to: {output_path}")
    except Exception as e:
        print(f"❌ Error saving validation report: {e}")
        sys.exit(1)
        
    # Determine if validation should be considered successful
    # Only fail for critical issues, not data quality issues
    critical_errors = 0
    data_quality_errors = 0
    
    for error in errors:
        for err_msg in error['errors']:
            if any(critical in err_msg.lower() for critical in ['missing email column', 'invalid email format']):
                critical_errors += 1
            else:
                data_quality_errors += 1
    
    if critical_errors > 0:
        print(f"\n❌ Found {critical_errors} critical validation errors!")
        return False
    elif data_quality_errors > 0:
        print(f"\n⚠️  Found {data_quality_errors} data quality issues (not critical)")
        return True
    else:
        return True


if __name__ == "__main__":
    print("🔍 CONTACT DATA VALIDATION")
    print("=" * 40)
    
    try:
        success = main()
        
        if success:
            print("\n🎉 Validation completed successfully!")
            sys.exit(0)
        else:
            print("\n💥 Validation failed with critical errors!")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Validation script error: {e}")
        sys.exit(1)
