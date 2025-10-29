# Shopify to BigQuery Data Pipeline

This project implements an **automated ETL pipeline** that extracts key datasets (`orders`, `customers`, and optionally `products`) from a Shopify store and loads them into **Google BigQuery**.  

---

## Overview

The pipeline connects to a Shopify store via the REST Admin API, retrieves paginated data, stores it as raw CSV files locally, and then loads the cleaned data into BigQuery under a `raw_data` dataset.

### Key Features
- Extracts **orders**, **customers**, and **products**
- Handles pagination, retries, and rate limits
- Cleans column names for BigQuery compatibility
- Automatically creates BigQuery tables and dataset if missing
- Modular structure for scalability (extract, load, config, orchestration)

---

## Project Structure

```
shopify-to-bigquery-pipeline/
│
├── config.py                # Handles environment variables and configuration
├── extract_shopify_data.py  # Extracts data from Shopify REST API
├── load_to_bigquery.py      # Loads CSVs into BigQuery
├── pipeline.py              # Orchestrates the full pipeline (extract + load)
├── utils.py                 # Helper utilities (incremental loading logic)
├── data/                    # Stores extracted CSV files
├── requirements.txt         # Python dependencies
├── .env.example             # Example environment variables
└── README.md
```

---

## Setup Instructions

### 1️ Clone the Repository
```bash
git clone https://github.com/naqash98-lab/shopify-to-bigquery-pipeline.git
cd shopify-to-bigquery-pipeline
```

### 2️ Create a Virtual Environment
```bash
python -m venv venv
venv\Scripts\activate       # On Windows
# or
source venv/bin/activate    # On Mac/Linux
```

### 3 Install Dependencies
```bash
pip install -r requirements.txt
```

### 4️ Configure Environment Variables
Create a `.env` file (use `.env.example` as a template):

```bash
SHOPIFY_STORE_NAME=impression-digital-test-store
SHOPIFY_API_KEY=your_api_key
SHOPIFY_PASSWORD=your_api_password
SHOPIFY_API_VERSION=2023-10

GCP_PROJECT_ID=marketing-pipeline-demo-475611
BQ_DATASET_ID=raw_data
BQ_LOCATION=EU
GOOGLE_APPLICATION_CREDENTIALS=big_query.json
```

> Do not upload your `.env` or `big_query.json` file to GitHub.

---

## Running the Pipeline

### Run the Full Pipeline (Extract + Load)
```bash
python pipeline.py
```

### Extract Only (Shopify → CSV)
```bash
python pipeline.py --extract
```

### Load Only (CSV → BigQuery)
```bash
python pipeline.py --load
```

---

##  Output in BigQuery

After execution, your BigQuery project will contain a dataset named **`raw_data`**, with the following tables:

| Table | Description |
|--------|--------------|
| `shopify_orders` | Raw order data extracted from Shopify |
| `shopify_customers` | Customer profiles |
| `shopify_products` | Product catalog |

---

## How It Works

1. **Extraction:**  
   `extract_shopify_data.py` connects to the Shopify REST API using API credentials and retrieves paginated data for orders, customers, and products.

2. **Data Storage:**  
   Extracted data is normalized using Pandas and saved as CSVs in the `/data` directory.

3. **Loading:**  
   `load_to_bigquery.py` reads the CSVs, sanitizes column names, and uploads them into BigQuery using the official `google-cloud-bigquery` client.

4. **Orchestration:**  
   `pipeline.py` coordinates both phases and can be run end-to-end or stepwise using command-line arguments.

---

## Design Highlights
- Modular architecture (easy to extend or integrate with Airflow / Meltano)
- Automatically handles rate limits and retries
- Cleans column names to meet BigQuery schema rules
- Environment variables managed through `.env` for secure credential storage
- Ready for deployment on GCP or container-based environments

---

##  Notes 
- Future improvements could include orchestration via Airflow, and deduplication logic.
