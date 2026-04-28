**Overview**
This project implements an end-to-end data pipeline for logistics analytics, transforming raw CSV files into structured, analysis-ready datasets and business metrics.

The pipeline follows a layered architecture:

<img width="652" height="155" alt="image" src="https://github.com/user-attachments/assets/d7e0db7d-e12d-49f3-b729-d800c7842324" />

The goal is to enable insights such as fill rate, supplier reliability, and spoilage risk while ensuring data quality and scalability.

**How to Run the Project**

   A. Tools & Setup
   
   This project is built entirely on Google Cloud Platform (GCP):
   - Google Cloud Storage (GCS)
   - BigQuery
   - Looker Studio (for visualization)
   - (Planned) Cloud Composer for orchestration
   
   Prerequisites:
   GCP account (free trial is sufficient: https://cloud.google.com/free)

   B. Steps to Reproduce
   
    1. Upload Raw Data 
    Create a GCS bucket
    Upload CSV files into a structured folder:
       gs://logistics/deliveries/2026/04/26/deliveries.csv
       gs://logistics/stores/2026/04/26/stores.csv
       gs://logistics/orders/2026/04/26/orders.csv
       gs://logistics/inventory/2026/04/26/inventory.csv
       gs://logistics/products/2026/04/26/products.csv
    Note: Date-based folders were added to support future partitioning.

    2. Create Raw Layer (External Tables):
    Create external tables in BigQuery (logistics_raw):
       `project-chainbee.logistics_raw.deliveries`
       `project-chainbee.logistics_raw.inventory`
       `project-chainbee.logistics_raw.orders`
       `project-chainbee.logistics_raw.products`
       `project-chainbee.logistics_raw.stores`
    Source: GCS CSV files
    All columns defined as STRING
    No transformations applied
    
    3. Create Refined Layer
    Create refined tables in BigQuery (logistics_refined):
       `project-chainbee.logistics_refined.deliveries`
       `project-chainbee.logistics_refined.inventory`
       `project-chainbee.logistics_refined.orders`
       `project-chainbee.logistics_refined.products`
       `project-chainbee.logistics_refined.stores`
    Transform and clean data into logistics_refined
    Apply Data type casting, Data cleaning, Data validation
    
    4. Create Reporting Layer
    Create views in BigQuery (logistics_reporting):
       `project-chainbee.logistics_reporting.store_fill_rate`
       `project-chainbee.logistics_reporting.spoilage_risk_score`
       `project-chainbee.logistics_reporting.supplier_reliability_score`
       `project-chainbee.logistics_reporting.inventory_on_hand`
       `project-chainbee.logistics_reporting.reorder_flag`
    Compute business metrics: Fill Rate, Supplier Reliability, Spoilage Risk

    5. Visualization
    Connect reporting views to Looker Studio
    Build dashboards for business insights

**Future Automation**

Pipeline will be orchestrated using Cloud Composer (Airflow)
Supports recurring ingestion and transformation workflows

**Data Quality Issues and Handling**

  A. Mixed & Ambiguous Date Formats
  
   Formats included:
   
    - YYYY-MM-DD
    - DD-Mon-YYYY
    - MM/DD/YYYY, DD/MM/YYYY (ambiguous)
    
   Handling:
   
    - Parsed only unambiguous formats
    - Ambiguous values set to NULL
    - Added is_ambiguous flag for tracking and escalation   
    
  B. Invalid Inventory Values
  
   Values such as "wala", "N/A", "null"
   
   Handling:
   
    - "wala" → 0
    - "N/A" / "null" → NULL
  
  C. Misaligned Rows (deliveries table)
  
  Some rows had shifted columns due to formatting issues
  
  Handling:
  
   - Split into valid and malformed rows
   - Realigned malformed records
   - Recombined using UNION ALL

  D. Duplicate Stores
  
  Multiple records per store_id
  
  Handling:
  
   - Used ROW_NUMBER() with QUALIFY
   - Retained latest record based on opened_date

  E. Currency Formatting Issues (unit_cost)
  
  Values contained symbols (₱) and commas
  
  Handling:
  
   - Removed non-numeric characters using regex
   - Cast to FLOAT

  F. Non-informative Columns (uom)
  
  Column contained constant value "unit"
  
  Handling / Recommendation:
  
   - Identified as redundant
   - Suggested extracting quantity from product_name

  Proposed new columns:

   - no_of_units
   - standardized uom (ml, L, g, kg, pcs)
     
**Assumptions**

  All unit_cost values are assumed to be in Philippine Peso (₱)
  Ambiguous date formats were not inferred and excluded from analysis
  Negative quantities represent reversals and are excluded from metrics
  Supplier “on-time” definition assumed (e.g., within 3 days of order date)
  Demand approximated using average order quantity
  
**Production Considerations** (Stage 5)

  A. Scheduling & Orchestration
  
   Use Cloud Composer (Airflow) to orchestrate:
   - Data ingestion
   - Transformation jobs
   - Reporting layer refresh
     
  B. Monitoring & Alerting
  
   Use monitoring tools such as:
   
    - Airflow alerts
    - Datadog or Grafana
    
   Alerts for:
   
    - Failed ingestion jobs
    - Failed transformation queries
    - Missing or incomplete data loads
    
   Critical Alert (2 AM scenario):
   
    - End-to-end pipeline failure (no data available for reporting)
    
  C. Late or Missing Data Handling
  
    Pipeline designed to:
    
    - Pick up late-arriving files in the next scheduled run
    - Process multiple days if needed

  D. Data Warehouse Design
  
  For production-scale implementation:
  
    1. Use layered architecture: Raw → Refined → Reporting
    2.Partition tables by ingestion or business date
    3. Cluster by frequently queried fields (e.g., store_id, sku)
    4. Normalize dimensions (stores, products, suppliers)
    5. Maintain fact tables (orders, deliveries, inventory)

**Use of AI Assistance**

An AI assistant (ChatGPT) was used to:
1. Refine SQL logic and transformations
2. Improve documentation and README structure
3. Validate data engineering best practices
All implementation decisions and final outputs were reviewed and validated independently.

**Incomplete Work & Next Steps**

Due to time constraints, the following were not fully completed:

  Not Finished:
  1. Final dashboard implementation
  2. Reporting layer completion for all metrics
  3. Pipeline orchestration (Airflow)
  4. Table partitioning strategy
     
  Next Steps
  1. Build full Looker Studio dashboard
  2. Implement Cloud Composer for automation
  3. Add partitioned tables in BigQuery
  4. Set up monitoring and alerting system
  5. Enhance data quality checks and reporting

**Conclusion**

This project establishes a scalable and production-ready foundation for logistics analytics.
It prioritizes data correctness, transparency, and extensibility, while clearly identifying areas for future improvement.
