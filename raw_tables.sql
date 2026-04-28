/*
Name: Logistics Raw Tables
Purpose: Ingestion of Raw CSV Files into BigQuery
Author: JN Rimorin
*/

-- NOTE: All columns in raw tables are intentionally stored as STRING to avoid load failures and preserve source-of-truth data. All type casting, business logic transformations will be handled in the refined layer via controlled SQL transformations.

-- 1. Create a dataset for the raw layer.
CREATE SCHEMA logistics_raw;

-- 2. Create external table for deliveries CSV file.
CREATE OR REPLACE EXTERNAL TABLE `project-chainbee.logistics_raw.deliveries`
( 
  delivery_id STRING,
  order_id STRING,
  actual_delivery_date STRING,
  carrier STRING,
  delivery_status STRING,
  )
OPTIONS (
  format = 'CSV',
  uris = ['gs://chainbee/logistics/deliveries/2026/04/26/deliveries.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

-- 3. Create external table for inventory CSV file.
CREATE OR REPLACE EXTERNAL TABLE `project-chainbee.logistics_raw.inventory`
( 
  inventory_id  STRING,
  store_id  STRING,
  sku_id  STRING,
  snapshot_date STRING,
  quantity_on_hand  STRING,
  reorder_level STRING,
  last_updated  STRING,
  )
OPTIONS (
  format = 'CSV', -- Can also be 'JSON', 'AVRO', 'PARQUET', or 'ORC'
  uris = ['gs://chainbee/logistics/inventory/2026/04/26/inventory.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

-- 4. Create external table for orders CSV file.
CREATE OR REPLACE EXTERNAL TABLE `project-chainbee.logistics_raw.orders`
( 
  order_id  STRING,
  store_id  STRING,
  sku_id  STRING,
  supplier_id STRING,
  quantity_ordered  STRING,
  unit_cost STRING,
  order_date  STRING,
  expected_delivery_date  STRING,
  status  STRING,
  )
OPTIONS (
  format = 'CSV', -- Can also be 'JSON', 'AVRO', 'PARQUET', or 'ORC'
  uris = ['gs://chainbee/logistics/orders/2026/04/26/orders.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


-- 5. Create external table for products CSV file.
CREATE OR REPLACE EXTERNAL TABLE `project-chainbee.logistics_raw.products`
( 
  sku_id  STRING,
  product_name  STRING,
  category  STRING,
  unit_cost STRING,
  shelf_life_days STRING,
  uom STRING,
  )
OPTIONS (
  format = 'CSV', -- Can also be 'JSON', 'AVRO', 'PARQUET', or 'ORC'
  uris = ['gs://chainbee/logistics/products/2026/04/26/products.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

-- 6. Create external table for stores CSV file.

CREATE OR REPLACE EXTERNAL TABLE `project-chainbee.logistics_raw.stores`
( 
  store_id  STRING,
  store_name  STRING,
  city  STRING,
  region  STRING,
  format  STRING,
  opened_date STRING,
  primary_supplier  STRING,
  )
OPTIONS (
  format = 'CSV', -- Can also be 'JSON', 'AVRO', 'PARQUET', or 'ORC'
  uris = ['gs://chainbee/logistics/stores/2026/04/26/stores.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

-- 7. Validation checks
-- Total row count vs. distinct count of primary key
SELECT COUNT(*) AS row_count,
COUNT(DISTINCT delivery_id) AS distinct_row_count 
FROM `project-chainbee.logistics_raw.deliveries` ;

SELECT COUNT(*) AS row_count,
COUNT(DISTINCT inventory_id) AS distinct_row_count 
FROM `project-chainbee.logistics_raw.inventory` ;

COUNT(DISTINCT order_id) AS distinct_row_count 
FROM `project-chainbee.logistics_raw.orders` ;

SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "orders";

SELECT COUNT(*) AS row_count,
COUNT(DISTINCT store_id) AS distinct_row_count 
FROM `project-chainbee.logistics_raw.products` ;


-- CHECK SCHEMA
SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "deliveries";

SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "inventory";

SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "orders";

SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "products";

SELECT column_name, data_type
FROM `project-chainbee.logistics_raw`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = stores
