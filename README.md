# BCG-contacts-data-pipeline

A contact aggregation and marketing automation pipeline integrating data from Mailchimp, Outlook, MYOB EXO, and spreadsheets. Cleans, deduplicates, and segments contact data for streamlined CRM and campaign targeting.

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_ORG/crm-data-pipeline.git
cd crm-data-pipeline
```

2. Set Up a Virtual Environment
```
python3 -m venv env
source env/bin/activate  # Windows: env\Scripts\activate
```

4. Install Dependencies
```
pip install -r requirements.txt
```

🧩 Project Structure

crm-data-pipeline/

├── data_sources/         # Fetch data from Mailchimp, Outlook, Spreadsheets, MYOB


├── cleaning/             # Clean and normalize contact fields


├── deduplication/        # Remove duplicate contacts


├── segmentation/         # Tag and group contacts


├── integration/          # Push segmented contacts to Mailchimp


├── utils/                # Common validators or helpers


├── main.py               # Main pipeline execution script


├── requirements.txt      # Python dependencies



🧪 Running the Pipeline
```
python main.py**
```

