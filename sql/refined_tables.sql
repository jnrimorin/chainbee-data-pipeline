/*
Name: Logistics Refined Tables
Purpose: Raw Data Transformation loaded into the Refined Layer
*/

-- NOTE: This layer transforms raw ingested data into cleaned and analysis-ready datasets. Key responsibilities are to cast string fields into proper data types, standardize formats, gandle data inconsistencies, removing duplicates, and prepare structured dataset for reporting.


-- 1. Create a dataset for the refined layer.
CREATE SCHEMA logistics_refined;

-- 2. Create transformed table for deliveries table.
CREATE OR REPLACE TABLE `project-chainbee.logistics_refined.deliveries` AS
SELECT delivery_id  
,order_id
,PARSE_DATE('%Y-%m-%d', actual_delivery_date) AS actual_delivery_date
,carrier
,delivery_status
FROM `project-chainbee.logistics_raw.deliveries`
WHERE CAST(REGEXP_EXTRACT(delivery_id, r'DEL-(\d+)') AS INT) <=200

UNION ALL
-- moved the values in their appropriate columns
SELECT order_id AS delivery_id  
,actual_delivery_date AS order_id
,PARSE_DATE('%Y-%m-%d', carrier) AS actual_delivery_date
,delivery_status AS carrier
,NULL AS delivery_status
FROM `project-chainbee.logistics_raw.deliveries`
WHERE CAST(REGEXP_EXTRACT(order_id, r'DEL-(\d+)') AS INT) >200;

-- 3. Create transformed table for inventory table.
CREATE OR REPLACE TABLE `project-chainbee.logistics_refined.inventory` AS
SELECT  inventory_id
,store_id
,sku_id
,PARSE_DATE("%Y-%m-%d", snapshot_date) as snapshot_date
-- added logic to quantity_on_hand values
,CAST((CASE 
   WHEN quantity_on_hand = "wala" THEN '0'
   WHEN quantity_on_hand IN ("null","N/A") THEN NULL
   ELSE quantity_on_hand
  END) AS INT) AS quantity_on_hand
-- update the values to be able to cast its data type as integer
,CAST(reorder_level AS INT) AS reorder_level
,PARSE_DATETIME("%Y-%m-%d %H:%M:%S", last_updated) AS last_updated
FROM `project-chainbee.logistics_raw.inventory`
ORDER BY inventory_id;

-- 4. Create transformed table for orders table.
CREATE OR REPLACE TABLE `project-chainbee.logistics_refined.orders` AS
SELECT order_id
,store_id
,sku_id
,supplier_id
,CAST(quantity_ordered AS INT) as quantity_ordered
,CAST(unit_cost AS FLOAT64) as unit_cost
,order_date AS order_date_raw

---- Parsed expected delivery date, formatted'%Y-%m-%d' and '%d-%b-%Y' to standardize the values into one format, set NULL values to dates that are ambiguous
,CASE WHEN SAFE.PARSE_DATE('%Y-%m-%d', order_date) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%Y-%m-%d', order_date)
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', order_date) IS NOT NULL AND SAFE.PARSE_DATE('%d/%m/%Y', order_date) IS NULL
      THEN SAFE.PARSE_DATE('%m/%d/%Y', order_date)
    WHEN SAFE.PARSE_DATE('%d/%m/%Y', order_date) IS NOT NULL AND SAFE.PARSE_DATE('%m/%d/%Y', order_date) IS NULL 
      THEN SAFE.PARSE_DATE('%d/%m/%Y', order_date)
    WHEN SAFE.PARSE_DATE('%d-%b-%Y', order_date) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%d-%b-%Y', order_date)
    ELSE NULL 
  END as order_date_refined

,-- Provides info on whether the order date is ambiguous or not
CASE WHEN SAFE.PARSE_DATE('%m/%d/%Y', order_date) IS NOT NULL AND SAFE.PARSE_DATE('%d/%m/%Y', order_date) IS NOT NULL
      THEN "YES"
    WHEN SAFE.PARSE_DATE('%d/%m/%Y', order_date) IS NOT NULL AND SAFE.PARSE_DATE('%m/%d/%Y', order_date) IS NOT NULL 
      THEN "YES"
    ELSE "NO"
    END AS is_ambiguous_order_date

,expected_delivery_date AS expected_delivery_date_raw

---- Parsed expected delivery date, formatted'%Y-%m-%d' and '%d-%b-%Y' to standardize the values into one format, set NULL values to dates that are ambiguous
,CASE 
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', expected_delivery_date) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%Y-%m-%d', expected_delivery_date)
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', expected_delivery_date) IS NOT NULL AND SAFE.PARSE_DATE('%d/%m/%Y', expected_delivery_date) IS NULL
      THEN SAFE.PARSE_DATE('%m/%d/%Y', expected_delivery_date)
    WHEN SAFE.PARSE_DATE('%d/%m/%Y', expected_delivery_date) IS NOT NULL AND SAFE.PARSE_DATE('%m/%d/%Y', expected_delivery_date) IS NULL 
      THEN SAFE.PARSE_DATE('%d/%m/%Y', expected_delivery_date)
    WHEN SAFE.PARSE_DATE('%d-%b-%Y', expected_delivery_date) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%d-%b-%Y', expected_delivery_date)
    ELSE NULL 
  END as expected_delivery_date_refined

-- Provides info on whether the expected delivery date is ambiguous or not
,CASE WHEN SAFE.PARSE_DATE('%m/%d/%Y', expected_delivery_date) IS NOT NULL AND SAFE.PARSE_DATE('%d/%m/%Y', expected_delivery_date) IS NOT NULL
      THEN "YES"
    WHEN SAFE.PARSE_DATE('%d/%m/%Y', expected_delivery_date) IS NOT NULL AND SAFE.PARSE_DATE('%m/%d/%Y', expected_delivery_date) IS NOT NULL 
      THEN "YES"
    ELSE "NO"
    END AS is_ambiguous_expected_delivery_date
,status
FROM `project-chainbee.logistics_raw.orders`
-- ORDER BY order_id;

-- 5. Create transformed table for products table.
CREATE OR REPLACE TABLE `project-chainbee.logistics_refined.products` AS
SELECT sku_id
,product_name
,category
,CAST(REGEXP_REPLACE(unit_cost, r'[^0-9.]', '') AS FLOAT64) AS unit_cost 
-- Keeps only numbers and dots
,CAST(shelf_life_days AS INT) AS shelf_life_days
,uom
FROM `project-chainbee.logistics_raw.products`
ORDER BY sku_id;


-- 6. Create transformed table for stores table.
CREATE OR REPLACE TABLE `project-chainbee.logistics_refined.stores` AS
SELECT store_id
,store_name
,city	
,region
,format
,PARSE_DATE("%Y-%m-%d", opened_date) as opened_date
,primary_supplier
FROM `project-chainbee.logistics_raw.stores`
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date DESC) = 1
-- Removes store_id duplication
ORDER BY store_id;


-- 7. Validation checks
-- Total row count vs. distinct count of primary key
SELECT COUNT(*) AS row_count,
COUNT(DISTINCT delivery_id) AS distinct_row_count 
FROM `project-chainbee.logistics_refined.deliveries` ;

SELECT COUNT(*) AS row_count,
COUNT(DISTINCT inventory_id) AS distinct_row_count 
FROM `project-chainbee.logistics_refined.inventory` ;

COUNT(DISTINCT order_id) AS distinct_row_count 
FROM `project-chainbee.logistics_refined.orders` ;

SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "orders";

SELECT COUNT(*) AS row_count,
COUNT(DISTINCT store_id) AS distinct_row_count 
FROM `project-chainbee.logistics_refined.products` ;


-- CHECK SCHEMA
SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "deliveries";

SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "inventory";

SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "orders";

SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "products";

SELECT column_name, data_type
FROM `project-chainbee.logistics_refined`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = stores
