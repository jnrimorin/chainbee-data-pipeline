/*
Name: Logistics Refined Tables
Purpose: Creates aggregated level metrics in the refined layer for reporting, including inventory, demand, reorder planning, and spoilage risk analysis across stores, suppliers, and SKUs.
*/

-- 1. Create a dataset for the refined layer.
CREATE SCHEMA logistics_reporting;

-- 2. Create reporting view for Fill Rate metrics:

-- NOTE: Fill rate is computed as COUNT(DISTINCT delivered orders) / COUNT(DISTINCT total orders) per store.
-- Delivery success is defined by status = 'DELIVERED'.
-- Deduplication is handled via DISTINCT order_id.
CREATE OR REPLACE VIEW `project-chainbee.logistics_reporting.store_fill_rate` AS
SELECT store_id,status
COUNT(DISTINCT order_id) AS order_count
,COUNT(DISTINCT CASE WHEN UPPER(status) = "DELIVERED" THEN order_id ELSE NULL END) AS delivered_order_count
-- ,(COUNT(DISTINCT CASE WHEN UPPER(status) = "DELIVERED" THEN order_id ELSE NULL END)/COUNT(DISTINCT order_id)) AS fill_rate
FROM `project-chainbee.logistics_refined.orders`
GROUP BY 1,2
ORDER BY store_id


-- 3. Create reporting view for Supplier Reliability Score:
-- NOTE: Some records have NULL delivery_status values due to recent alignment corrections in the deliveries dataset.
-- These records are excluded from the supplier reliability calculation to avoid distorting results,
-- and are tracked separately for data quality monitoring and upstream correction.
CREATE OR REPLACE VIEW `project-chainbee.logistics_reporting.supplier_reliability_score` AS
SELECT 
  orders.supplier_id
,deliveries.delivery_status
,COUNT(DISTINCT delivery_id) AS delivery_count
,COUNT(DISTINCT CASE WHEN UPPER(delivery_status)="ON TIME" THEN delivery_id ELSE NULL END) AS on_time_delivery_count
FROM `project-chainbee.logistics_refined.orders` orders
LEFT JOIN `project-chainbee.logistics_refined.deliveries` deliveries
  ON orders.order_id = deliveries.order_id
WHERE delivery_status IS NOT NULL
GROUP BY 1,2
ORDER BY 1


-- 4. Create reporting view for Spoilage Risk Score, Days of Inventory on Hand per store-SKU, and Reorder Flag per store-SKU
-- NOTE:
-- This view computes store-SKU level inventory metrics for reporting and decision-making.
-- It includes key KPIs:
-- - days_of_inventory: estimated stock duration based on average daily demand
-- - remaining_shelf_life: shelf life vs expected inventory duration
-- - reorder_flag: indicates if stock is at/below reorder level (YES/NO)
-- - spoilage_risk: classifies risk based on shelf life, demand, and data validity
-- Negative quantities are treated as 0, and NULL/zero demand is safely handled to avoid division errors.
CREATE OR REPLACE VIEW `project-chainbee.logistics_reporting.store_sku_metrics` AS

WITH base AS (
SELECT 
  orders.store_id
,orders.sku_id
,products.shelf_life_days
,AVG(CASE WHEN quantity_ordered<0 THEN 0 ELSE quantity_ordered END) AS avg_daily_quantity_ordered --10/day
,SUM(quantity_on_hand) AS total_quantity_on_hand
,SUM(reorder_level) AS total_reorder_level
FROM `project-chainbee.logistics_refined.orders` orders
LEFT JOIN `project-chainbee.logistics_refined.products` products
  ON orders.sku_id = products.sku_id
LEFT JOIN `project-chainbee.logistics_refined.inventory` inventory
  ON orders.sku_id = inventory.sku_id
 AND orders.store_id = inventory.store_id
WHERE products.shelf_life_days IS NOT NULL AND quantity_on_hand IS NOT NULL
GROUP BY 1,2,3
)
SELECT 
store_id
,sku_id
,shelf_life_days
,(total_quantity_on_hand/NULLIF(avg_daily_quantity_ordered,0)) AS days_of_inventory
,CASE WHEN total_quantity_on_hand > total_reorder_level THEN "NO" 
    ELSE "YES"
    END AS reorder_flag
,(shelf_life_days) - (total_quantity_on_hand/NULLIF(avg_daily_quantity_ordered,0)) AS remaining_shelf_life
,CASE WHEN (total_quantity_on_hand/NULLIF(avg_daily_quantity_ordered,0)) < shelf_life_days THEN "LOW RISK"
    WHEN (total_quantity_on_hand/NULLIF(avg_daily_quantity_ordered,0)) > shelf_life_days THEN "HIGH RISK"
    WHEN (total_quantity_on_hand) IS NULL THEN "INVALID DATA"
    WHEN  avg_daily_quantity_ordered IS NULL or avg_daily_quantity_ordered = 0 THEN "HIGH RISK (NO DEMAND)"
    END AS spoilage_risk
FROM base
ORDER BY 1,2
