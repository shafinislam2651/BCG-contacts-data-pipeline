import re

def clean(contacts):
    print("Cleaning contacts...")
    for c in contacts:
        if c.get("email"):
            c["email"] = c["email"].strip().lower()

        if c.get("phone"):
            c["phone"] = re.sub(r"\D", "", str(c["phone"]))

        if c.get("name"):
            c["name"] = c["name"].strip().title()

        if c.get("company"):
            c["company"] = c["company"].strip().title()

    return contacts
