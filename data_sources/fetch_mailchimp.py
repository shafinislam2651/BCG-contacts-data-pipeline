import requests
import os

MAILCHIMP_API_KEY = os.getenv("MAILCHIMP_API_KEY")
LIST_ID = os.getenv("MAILCHIMP_LIST_ID")
DC = MAILCHIMP_API_KEY.split('-')[-1]  # Data center

def get_contacts():
    print("Fetching Mailchimp contacts...")
    url = f"https://{DC}.api.mailchimp.com/3.0/lists/{LIST_ID}/members"
    headers = {
        "Authorization": f"apikey {MAILCHIMP_API_KEY}"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    contacts = []

    for member in data.get("members", []):
        contacts.append({
            "source": "mailchimp",
            "email": member.get("email_address"),
            "name": member.get("full_name") or f"{member.get('merge_fields', {}).get('FNAME')} {member.get('merge_fields', {}).get('LNAME')}",
            "phone": member.get("merge_fields", {}).get("PHONE"),
            "company": None
        })

    return contacts
