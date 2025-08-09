# BCG-contacts-data-pipeline

A contact aggregation and marketing automation pipeline that cleans, deduplicates, and standardizes contact data from multiple sources. Produces a single TSV suitable for CRM import and campaign targeting.

### 🚀 Getting Started

- **Clone the Repository**
```bash
git clone https://github.com/YOUR_USERNAME/BCG-contacts-data-pipeline.git
cd BCG-contacts-data-pipeline
```

- **Set Up a Virtual Environment**
```bash
python3 -m venv env
source env/bin/activate           # Windows: env\Scripts\activate
```

- **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 🧩 Project Structure

```
BCG-contacts-data-pipeline/
├── data_sources/           # Source TSVs (raw or staged inputs)
├── data_files/             # Optional extra datasets (if used)
├── cleaning/               # Cleaning, standardization, deduplication
│   └── clean_contacts.py
├── utils/                  # Validators and helper utilities
│   └── validators.py
├── output/                 # Pipeline outputs (MergedDatabase.tsv, cleaned_contacts.tsv)
├── env/                    # Local virtual environment (not tracked)
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── LICENSE                 # License
```

### 🧪 Running the Pipeline

- Ensure your upstream merged file exists:
  - Place `MergedDatabase.tsv` at `output/MergedDatabase.tsv`.
  - If you have an upstream merge step, run that first so `MergedDatabase.tsv` is created.

- Run the cleaner + deduplicator:
```bash
python cleaning/clean_contacts.py
```

- Output:
  - `output/cleaned_contacts.tsv` (tab-separated)

### 🔧 What the Pipeline Does

- **Field cleaning**
  - Emails normalized and validated
  - Phone numbers stripped to digits (min length enforced)
  - Names and titles normalized (title case)
  - Address fields trimmed
  - Dates parsed (e.g., `LAST_UPDATED`)

- **Boolean normalization**
  - `ISACTIVE`, `OPTOUT_EMARKETING`: only "Y" or "N"; anything else → null
  - `SUB1`–`SUB26`: preserved and standardized to "Y"/"N" (other values → null)

- **Deduplication and merging**
  - Primary key: `EMAIL`; fallback: `FULLNAME` + `MOBILE`
  - Sorts by `LAST_UPDATED` (if present)
  - For each duplicate group, merges fields to produce the most complete record:
    - Picks the first available value; if multiple, prefers the most complete (longest) value

- **Numeric handling**
  - Preserves integer-like fields (e.g., `SEQNO`, `SALESNO`, `COMPANY_ACCNO`)
  - Resets sequence columns (e.g., `SEQNO`) to a clean 1..N

### 📦 Inputs and Outputs

- **Input**: `output/MergedDatabase.tsv` (tab-separated)
- **Output**: `output/cleaned_contacts.tsv` (tab-separated)

### 📝 Notes

- If `MergedDatabase.tsv` is missing, the script will log an error with the expected path and suggest generating it first.
- Logging provides progress and field-level stats during cleaning, deduping, and export.

### 📜 License

See `LICENSE` for details.

