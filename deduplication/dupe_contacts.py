from collections import defaultdict

def deduplicate(contacts):
    print("Deduplicating contacts...")
    seen = {}
    unique = []

    for contact in contacts:
        key = contact.get("email") or f"{contact.get('name')}-{contact.get('phone')}"
        if key in seen:
            continue
        seen[key] = True
        unique.append(contact)

    return unique
