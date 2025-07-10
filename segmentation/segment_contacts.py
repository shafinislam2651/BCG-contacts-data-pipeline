def apply_tags(contacts):
    print("Segmenting contacts...")

    for c in contacts:
        # Dummy rules – extend based on real fields
        if "bunnings" in (c.get("company") or "").lower():
            c["segment"] = "Supplier"
        elif c.get("source") == "myob":
            c["segment"] = "Past Customer"
        elif c.get("source") == "mailchimp":
            c["segment"] = "Prospect"
        else:
            c["segment"] = "Uncategorized"

    return contacts
