# Shopify to BigQuery Data Pipeline

An automated pipeline that extracts data from Shopify and loads it into Google BigQuery.

## What It Does

- Extracts orders, customers, and products from your Shopify store
- Loads the data into BigQuery for analysis
- Handles pagination and rate limiting automatically
- Creates BigQuery dataset and tables if they don't exist

## Prerequisites

- Python 3.8+
- A Shopify store with API access
- Google Cloud Platform account with BigQuery enabled

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Shopify API

Create a private app in your Shopify admin:
- Go to **Apps** → **Develop apps** → **Create an app**
- Add permissions: `read_orders`, `read_customers`, `read_products`
- Note down your API Key, Password, and Store Name

### 3. Set Up Google Cloud

1. Create a GCP project
2. Enable BigQuery API
3. Create a service account with BigQuery Admin role
4. Download the JSON key file and save it as `big_query.json`

### 4. Configure Environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
SHOPIFY_STORE_NAME=your-store-name
SHOPIFY_API_KEY=your-api-key
SHOPIFY_PASSWORD=your-password

GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=raw_data
BQ_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=big_query.json
```

## Usage

Run the full pipeline:
```bash
python pipeline.py
```

Or run individual steps:
```bash
python pipeline.py --extract  # Only extract from Shopify
python pipeline.py --load     # Only load to BigQuery
```

## Project Structure

```
├── pipeline.py                 # Main script
├── extract_shopify_data.py     # Shopify data extraction
├── load_to_bigquery.py         # BigQuery loading
├── config.py                   # Configuration management
├── requirements.txt            # Dependencies
└── data/                       # Temporary CSV files
```

## BigQuery Tables

The pipeline creates these tables:

- `shopify_orders` - Order details and line items
- `shopify_customers` - Customer information
- `shopify_products` - Product catalog and variants

## Troubleshooting

**Configuration errors**: Make sure all required variables in `.env` are set

**Authentication failed**: Check your Shopify API credentials and GCP service account permissions

**Rate limiting**: The pipeline handles this automatically with retries

## Notes

- The BigQuery dataset is created automatically if it doesn't exist
- CSV files in `data/` folder are temporary and gitignored
- Never commit `.env` or `big_query.json` to version control
