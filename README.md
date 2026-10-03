**Architecture Overview:**
  **Bronze:**	Raw data ingested as-is from GitHub into the Bronze lakehouse via Fabric Data Factory pipelines
  **Silver:**	PySpark notebook cleans, standardizes, and joins the data into a conformed OrderDetails table
  **Gold:**	PySpark notebook builds aggregated KPIs (product ratings, social sentiment, web engagement) for reporting

**Data Sources:**
**Structured (StructuredData/):** customers.csv, products.csv, Orders_Data.csv
**Unstructured (UnstructuredData/):**
  reviews.json — product reviews
  social_media.json — social platform activity
  web_logs.json — user web activity logs

**Pipelines (Pipelines/):**
Metadata-driven ingestion built with Fabric Data Factory:
  **Ingest_input_Structured_data** — copies CSVs from GitHub into Bronze folders, driven by ShoppingMart_StructuredMetaData.json
  **Ingest_input_Unstructured_data** — copies JSON files into Bronze folders, driven by ShoppingMart_UnstructuredMetaData.json
  **Master_Pipeline_shoppingMart** — orchestrates both ingestion pipelines end to end

Adding a new source is just a new entry in the metadata JSON — no pipeline changes needed.

**Silver Layer** (SilverLayer_Notebook.ipynb)
  Reads Bronze CSVs from OneLake (abfss://) with Spark
  Cleansing: drops nulls on key columns, casts OrderDate to date type
  Joins orders → customers → products into a single OrderDetails table
  Processes review JSON data into a review table

**Gold Layer** (GoldLayer_Notebook.ipynb)
  Business KPIs built on the Silver tables:
    **KPI 1** — Average product rating per product (from reviews)
    **KPI 2** — Social media sentiment trends by platform
    **KPI 3** — Web engagement per user, page, and action (from web logs)

**Tech Stack:** Microsoft Fabric · OneLake · Data Factory Pipelines · PySpark · Spark SQL · Delta Lake · GitHub (source data)

**Repository Structure:**

├── StructuredData/                  # Source CSV files
├── UnstructuredData/                # Source JSON files
├── Pipelines/                       # Fabric Data Factory pipeline definitions
│   ├── Ingest_input_Structured_data/
│   └── Ingest_input_Unstructured_data/
├── SilverLayer_Notebook.ipynb        # Cleaning & transformation
├── GoldLayer_Notebook.ipynb         # KPI aggregations
├── ShoppingMart_StructuredMetaData.json
├── ShoppingMart_UnstructuredMetaData.json
└── shoppingMarT_Medallion_Architecture.png


├── **Master Pipeline/**                       # Fabric Data Factory pipeline definitions
│   ├── Master_Pipeline_shoppingMart/
|   |  ├──Ingest_input_Structured_data/
|   |  ├──Ingest_input_Unstructured_data/
|   |  ├──SilverLayer_Notebook.ipynb
|   |  ├──GoldLayer_Notebook.ipynb


<img width="1807" height="900" alt="image" src="https://github.com/user-attachments/assets/b5849530-c9b0-48d9-9cea-586a120b5e98" />


