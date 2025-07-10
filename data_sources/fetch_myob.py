def get_contacts(filepath="data/myob_export.csv"):
    import pandas as pd
    print(f"Reading MYOB contacts: {filepath}")
    df = pd.read_csv(filepath)

    contacts = []
    for _, row in df.iterrows():
        contacts.append({
            "source": "myob",
            "name": row.get("Name"),
            "email": row.get("Email"),
            "phone": row.get("Phone"),
            "company": row.get("Company")
        })
    return contacts
