import pandas as pd
import re
import sys

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


def main(csv_path, output_path=None):
    df = pd.read_csv(csv_path)
    errors = []
    for idx, row in df.iterrows():
        row_errors = []
        # Check required fields
        for field in REQUIRED_FIELDS:
            if field in df.columns and (pd.isna(row[field]) or str(row[field]).strip() == ""):
                row_errors.append(f"Missing required field: {field}")
        # Check email format
        if "EMAIL" in df.columns and not validate_email(row.get("EMAIL", "")):
            row_errors.append("Invalid email format")
        # Check phone fields format (MOBILE, DIRECTPHONE, and HOMEPHONE)
        for phone_field in ["MOBILE", "DIRECTPHONE", "HOMEPHONE"]:
            if phone_field in df.columns and not validate_phone(row.get(phone_field, "")):
                row_errors.append(f"Invalid phone in {phone_field}")
        # Check for at least one contact method: EMAIL, MOBILE, DIRECTPHONE, or HOMEPHONE
        has_email = False
        has_mobile = False
        has_direct = False
        has_home = False
        if "EMAIL" in df.columns and not pd.isna(row.get("EMAIL", "")) and str(row.get("EMAIL", "")).strip() != "":
            has_email = True
        if "MOBILE" in df.columns and not pd.isna(row.get("MOBILE", "")) and str(row.get("MOBILE", "")).strip() != "":
            has_mobile = True
        if "DIRECTPHONE" in df.columns and not pd.isna(row.get("DIRECTPHONE", "")) and str(row.get("DIRECTPHONE", "")).strip() != "":
            has_direct = True
        if "HOMEPHONE" in df.columns and not pd.isna(row.get("HOMEPHONE", "")) and str(row.get("HOMEPHONE", "")).strip() != "":
            has_home = True
        if not (has_email or has_mobile or has_direct or has_home):
            row_errors.append("No contact method: missing email and all phone numbers (mobile/direct/home)")
        if row_errors:
            errors.append({"row": idx+1, "errors": row_errors})  # +1 for header and 0-index
    print(f"Total rows: {len(df)}")
    print(f"Rows with errors: {len(errors)}")
    for err in errors:
        print(f"Row {err['row']}: {', '.join(err['errors'])}")
    if not errors:
        print("All rows passed validation.")
    if output_path:
        import json
        with open(output_path, "w") as f:
            json.dump(errors, f, indent=2)
        print(f"Errors exported to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_fields.py <csv_path> [output_path.json]")
        sys.exit(1)
    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    main(csv_path, output_path)
