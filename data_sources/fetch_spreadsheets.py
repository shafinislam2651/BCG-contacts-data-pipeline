import pandas as pd

def get_contacts(filepath="data/spreadsheet_contacts.xlsx"):
    print(f"Reading spreadsheet: {filepath}")
    df = pd.read_excel(filepath)
    contacts = []

    for _, row in df.iterrows():
        contacts.append({
            "source": "spreadsheet",
            "name": row.get("Name"),
            "email": row.get("Email"),
            "phone": row.get("Phone"),
            "company": row.get("Company")
        })

    return contacts
