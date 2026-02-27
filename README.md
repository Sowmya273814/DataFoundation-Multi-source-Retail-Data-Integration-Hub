# 🏪 DataFoundation: Multi-source Retail Data Integration Hub

> A practical data integration hub for retail operations — consolidating data from multiple sources into a properly modeled BigQuery data warehouse with ETL pipelines, SCD Type 2 dimensions, and interactive Streamlit analytics.

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Data Sources](#-data-sources)
- [Star Schema Model](#-star-schema-model)
- [Project Structure](#-project-structure)
- [Setup & Installation](#-setup--installation)
- [Running the ETL Pipeline](#-running-the-etl-pipeline)
- [Streamlit Dashboard](#-streamlit-dashboard)
- [BigQuery Queries](#-bigquery-queries)
- [SCD Type 2 Implementation](#-scd-type-2-implementation)
- [Technologies Used](#-technologies-used)

---

## 🎯 Overview

**DataFoundation** integrates data from two distinct sources:

| Source | Type | Description |
|--------|------|-------------|
| **Kaggle Retail Sales** | CSV File | 1,000+ transaction records with customer demographics |
| **Fake Store API** | REST API | 20 products across 4 categories with ratings |

The ETL pipeline extracts, cleans, transforms, and loads this data into a **star schema** data warehouse on Google BigQuery, enabling powerful cross-source analytics.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                 │
├────────────────────┬─────────────────────────────────────────────┤
│   📂 CSV File      │   🌐 Fake Store API                        │
│   (Kaggle Dataset) │   (REST API - fakestoreapi.com)             │
└────────┬───────────┴──────────────┬──────────────────────────────┘
         │                          │
         ▼                          ▼
┌──────────────────────────────────────────────────────────────────┐
│                     EXTRACT LAYER                                │
│   • CSV Reader (pandas)          • HTTP Client (requests)        │
│   • Data validation              • JSON parsing & flattening     │
│   • Source tagging               • Rate limiting                 │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                     TRANSFORM LAYER                              │
│   • Data cleaning & standardization                              │
│   • Type casting & validation                                    │
│   • Surrogate key generation                                     │
│   • Hash-based change detection                                  │
│   • Star schema dimensional modeling                             │
│   • SCD Type 2 preparation                                       │
│   • Data mart aggregations                                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
┌─────────────────────┐  ┌─────────────────────────────────────────┐
│   MySQL Staging     │  │        BigQuery Data Warehouse           │
│   (Optional)        │  │   ┌──────────────────────────────────┐  │
│   • stg_retail_sales│  │   │ Staging    │ Dimensions │ Facts  │  │
│   • stg_api_products│  │   │ stg_retail │ dim_date   │ fact_  │  │
│   • etl_run_log     │  │   │ stg_api    │ dim_cust   │ sales  │  │
└─────────────────────┘  │   │            │ dim_prod   │        │  │
                         │   │            │ dim_cat    │        │  │
                         │   ├────────────┴────────────┴────────┤  │
                         │   │         DATA MARTS               │  │
                         │   │ mart_sales_performance            │  │
                         │   │ mart_category_analysis            │  │
                         │   └──────────────────────────────────┘  │
                         └─────────────────────────────────────────┘
                                          │
                                          ▼
                         ┌─────────────────────────────────────────┐
                         │        Streamlit Dashboard               │
                         │   • Pipeline monitoring                  │
                         │   • Sales analytics                      │
                         │   • Category analysis                    │
                         │   • Customer insights                    │
                         └─────────────────────────────────────────┘
```

---

## 📊 Data Sources

### 1. Kaggle Retail Sales Dataset (CSV)
- **Records:** 1,000 transactions  
- **Fields:** Transaction ID, Date, Customer ID, Gender, Age, Product Category, Quantity, Price per Unit, Total Amount
- **Categories:** Electronics, Clothing, Beauty
- **Date Range:** 2023-01-01 to 2024-01-01

### 2. Fake Store API (REST API)
- **Endpoint:** `https://fakestoreapi.com/products`
- **Products:** 20 items across 4 categories
- **Fields:** ID, Title, Price, Description, Category, Image URL, Rating (rate + count)
- **Categories:** electronics, jewelery, men's clothing, women's clothing

---

## ⭐ Star Schema Model

```
┌─────────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
│   dim_date       │     │      fact_sales          │     │  dim_customer    │
├─────────────────┤     ├─────────────────────────┤     ├──────────────────┤
│ date_key    (PK) │◄───│ date_key          (FK)  │───►│ customer_key (PK)│
│ full_date        │     │ customer_key      (FK)  │     │ customer_id      │
│ year / quarter   │     │ category_key      (FK)  │     │ gender / age     │
│ month / day      │     │ sales_key         (PK)  │     │ age_group        │
│ is_weekend       │     │ transaction_id          │     │ customer_segment │
│ fiscal_year      │     │ quantity / price         │     │ SCD2: start/end  │
└─────────────────┘     │ total_amount            │     │ is_current       │
                        └────────────┬────────────┘     └──────────────────┘
                                     │
                        ┌────────────▼────────────┐     ┌──────────────────┐
                        │  dim_product_category    │     │   dim_product    │
                        ├─────────────────────────┤     ├──────────────────┤
                        │ category_key       (PK) │     │ product_key (PK) │
                        │ category_name            │     │ product_name     │
                        │ category_source          │     │ api_price        │
                        │ category_group           │     │ rating_rate      │
                        └─────────────────────────┘     │ SCD2: start/end  │
                                                        └──────────────────┘
```

---

## 📁 Project Structure

```
Multi-source Retail Data Integration Hub/
├── config/
│   ├── __init__.py
│   └── settings.py              # Centralized configuration
├── etl/
│   ├── __init__.py
│   ├── extract.py               # Data extraction (CSV + API)
│   ├── transform.py             # Data transformation & modeling
│   ├── load.py                  # BigQuery loading & SCD Type 2
│   ├── mysql_staging.py         # Optional MySQL staging layer
│   └── pipeline.py              # ETL orchestrator
├── sql/
│   ├── bigquery_schema.sql      # BigQuery DDL statements
│   └── analytical_queries.sql   # Pre-built analytics queries
├── streamlit_app.py             # Monitoring & analytics dashboard
├── retail_sales_dataset.csv     # Source data (Kaggle)
├── requirements.txt             # Python dependencies
├── .env                         # Environment configuration
├── .env.example                 # Environment template
├── .gitignore
└── README.md
```

---

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.9+
- Google Cloud account with BigQuery access
- GCP service account JSON key file

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/multi-source-retail-data-integration-hub.git
cd multi-source-retail-data-integration-hub
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
cp .env.example .env
# Edit .env with your GCP project ID, credentials path, etc.
```

### 5. Set up GCP credentials
Place your BigQuery service account JSON file in the project root.  
Update `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.

---

## 🔄 Running the ETL Pipeline

### Full Pipeline (Extract + Transform + Load)
```bash
python -m etl.pipeline
```

### Extract + Transform Only (no BigQuery)
```bash
python -m etl.pipeline --skip-load
```

### Extract Only
```bash
python -m etl.pipeline --extract-only
```

### Pipeline Output
The pipeline generates detailed logs:
```
╔══════════════════════════════════════════════════════════╗
║  DataFoundation: Multi-source Retail Data Integration   ║
║  ETL Pipeline - Full Execution                          ║
╚══════════════════════════════════════════════════════════╝

▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
▓ STAGE 1: EXTRACT
▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
📂 Extracting retail sales data...
✅ Extracted 1000 retail sales records
🌐 Extracting product data from API...
✅ Extracted 20 products from API

▓ STAGE 2: TRANSFORM
🧹 Cleaning retail sales data...
📅 Building Date dimension (731 days)...
👤 Building Customer dimension (SCD Type 2)...
📦 Building Product dimension (SCD Type 2)...
💰 Building Fact Sales table...
📊 Building Sales Performance data mart...
📈 Building Category Analysis data mart...

▓ STAGE 3: LOAD TO BIGQUERY
📤 Loading to BigQuery...

📋 PIPELINE EXECUTION SUMMARY
Status: ✅ SUCCESS
Total Duration: 12.34 seconds
```

---

## 📊 Streamlit Dashboard

### Launch the dashboard
```bash
streamlit run streamlit_app.py
```

### Dashboard Pages

| Page | Description |
|------|-------------|
| 🏠 **Dashboard Overview** | KPI metrics, monthly trends, category breakdown, pipeline status |
| 📂 **Data Sources** | Raw data exploration from CSV and API |
| 🔄 **ETL Pipeline** | Pipeline architecture, controls, and table schema |
| 📊 **Sales Analytics** | Interactive sales performance with filters and heatmaps |
| 🏷️ **Category Analysis** | Cross-source category performance and demographics |
| 👤 **Customer Insights** | Customer demographics, segments, and spending patterns |
| 📦 **Product Catalog** | Fake Store API product cards and price-rating analysis |
| 🗄️ **Data Warehouse** | Star schema explorer and BigQuery table browser |

---

## 🔍 BigQuery Queries

Pre-built analytical queries are available in `sql/analytical_queries.sql`:

1. **Monthly Sales Trend** — Revenue & growth by month
2. **Category Performance** — Revenue share by product category
3. **Customer Segmentation** — Demographics and spending by segment
4. **Gender Analysis** — Gender-based purchasing patterns
5. **Weekend vs Weekday** — Day-of-week sales patterns
6. **Top Customers** — Highest revenue customers
7. **Product Catalog** — API product catalog overview
8. **Quarterly YoY** — Year-over-year quarterly comparisons

---

## 🔄 SCD Type 2 Implementation

### Change Detection
- Uses **MD5 hash** of key business attributes
- Compares incoming `row_hash` with existing records

### Versioning Strategy
| Column | Purpose |
|--------|---------|
| `effective_start_date` | When this version became active |
| `effective_end_date` | When superseded (9999-12-31 = current) |
| `is_current` | Boolean flag for active version |
| `version` | Incrementing version number |
| `row_hash` | MD5 hash for change detection |

### SCD2 Tables
- **dim_customer** — Tracks changes in customer gender and age
- **dim_product** — Tracks changes in product name, price, and rating

---

## 🛠️ Technologies Used

| Technology | Purpose |
|-----------|---------|
| **Python 3.9+** | ETL pipeline, data transformation |
| **pandas** | Data manipulation and analysis |
| **Google BigQuery** | Cloud data warehouse |
| **Streamlit** | Interactive monitoring dashboard |
| **Plotly** | Data visualization charts |
| **MySQL** | Optional staging database |
| **Fake Store API** | REST API data source |
| **Kaggle Dataset** | CSV file data source |
| **Git** | Version control |

---

## 📄 License

This project is for educational and demonstration purposes.

---

<p align="center">
  <strong>🏪 DataFoundation</strong> — Multi-source Retail Data Integration Hub<br>
  Built with ❤️ using Python, BigQuery & Streamlit
</p>
