from data_sources import fetch_mailchimp, fetch_outlook, fetch_spreadsheets, fetch_myob
from cleaning import clean_contacts
from deduplication import dupe_contacts
from segmentation import segment_contacts
from integration import mailchimp_uploader

def main():
    print("Fetching data from sources...")
    all_contacts = []
    all_contacts += fetch_mailchimp.get_contacts()
    all_contacts += fetch_outlook.get_contacts()
    all_contacts += fetch_spreadsheets.get_contacts()
    all_contacts += fetch_myob.get_contacts()

    print(f"Total raw contacts: {len(all_contacts)}")

    cleaned_contacts = clean_contacts.clean(all_contacts)
    unique_contacts = dupe_contacts.deduplicate(cleaned_contacts)
    segmented_contacts = segment_contacts.apply_tags(unique_contacts)

    print(f"Uploading {len(segmented_contacts)} contacts to Mailchimp...")
    mailchimp_uploader.upload(segmented_contacts)

if __name__ == "__main__":
    main()
