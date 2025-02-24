/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

===============================================================================
-- QUALITY OF crm_cust_info

------ BRONZE table ------

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, count(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;


-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_last_name
FROM bronze.crm_cust_info
WHERE cst_last_name != TRIM(cst_last_name)

-- Data Standardization & Consistency
SELECT distinct cst_gndr
FROM bronze.crm_cust_info


SELECT distinct cst_marital_status
FROM bronze.crm_cust_info


------ SILVER table ------

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, count(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;


-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Data Standardization & Consistency
SELECT distinct cst_gndr
FROM silver.crm_cust_info


SELECT distinct cst_marital_status
FROM silver.crm_cust_info


SELECT * FROM silver.crm_cust_info;


===============================================================================
---- QUALITY OF crm_prd_info ----
------ BRONZE table ------

SELECT 
	prd_id, 
	prd_key,
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

---prd_id---
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT prd_id, count(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;


---prd_key---
-- Creating new columns to join wiht other tables
SELECT 
	prd_id, 
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
	prd_nm, 
	ISNULL(prd_cost, 0)  as prd_cost, 
	prd_line, 
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;
-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') not in (SELECT distinct id FROM bronze.erp_px_cat_g1v2);

-- We can join tables using cat_id
SELECT distinct id FROM bronze.erp_px_cat_g1v2;

-- We can join tables using prd_key
SELECT sls_prd_key FROM bronze.crm_sales_details;


SELECT 
	prd_id, 
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details)

---prd_nm---
-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


---prd_cost---
-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- Data Standardization & Consistency
SELECT distinct prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


SELECT 
	prd_id, 
	prd_key,
	prd_nm, 
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


------ SILVER table ------

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT prd_id, count(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;


-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


---prd_cost---
-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- Data Standardization & Consistency
SELECT distinct prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT * 
FROM silver.crm_prd_info



===============================================================================
---- QUALITY OF crm_sales_details ----
------ BRONZE table ------

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT 
	sls_ord_num,
	sls_prd_key, 
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Check for interity of columns which will be used to join
SELECT 
	sls_ord_num,
	sls_prd_key, 
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key from silver.crm_prd_info) 



SELECT 
	sls_ord_num,
	sls_prd_key, 
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id from silver.crm_cust_info) 


-- transfromation of integer columns to date
-- check fro invalid dates
-- 1) sls_order_dt 
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

SELECT NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- 2) sls_ship_dt 
SELECT NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

--3) sls_due_dt 
SELECT NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- 4) Check fro Invalid Date Orders
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt


-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales =  Quantity * Price
-- >> Values must not be Null, zero or negative

SELECT DISTINCT sls_sales as old_sls_sales, sls_quantity, sls_price as old_sls_price, 
	 CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * abs(sls_price)
		THEN sls_quantity * abs(sls_price)
		ELSE sls_sales
	 END AS sls_sales,
	 
	 CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	 END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL  
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price



------ SILVER table ------

-- 1) Check fro Invalid Date Orders
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt

-- 2) Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales =  Quantity * Price
-- >> Values must not be Null, zero or negative

SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL  
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


===============================================================================
---- QUALITY OF crm_prd_info ----
------ BRONZE table ------
SELECT cid, bdate, gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000'

SELECT * FROM [bronze].[crm_cust_info]

SELECT cid,
	   CASE 
			WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
	   END AS cid,
	   bdate, 
	   gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END NOT IN (SELECT DISTINCT cst_key FROM [bronze].[crm_cust_info])


-- Identity Out-Of-Range Dates
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT gen,
	   CASE
			WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
FROM bronze.erp_cust_az12


------ SILVER table ------

-- Identity Out-Of-Range Dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12


SELECT *
FROM silver.erp_cust_az12


===============================================================================
---- QUALITY OF erp_loc_a101 ----
------ BRONZE table ------

SELECT cid, 
	   cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM [silver].[crm_cust_info]



SELECT REPLACE(cid, '-', '') as cid,
	   cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM [silver].[crm_cust_info])


-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT REPLACE(cid, '-', '') as cid,
	   CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) in ('USA', 'US') THEN 'United States'
			WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry as old_cntry,
	   CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) in ('USA', 'US') THEN 'United States'
				WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry		
FROM bronze.erp_loc_a101
ORDER BY cntry



------ SILVER table ------

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT *
FROM silver.erp_loc_a101



===============================================================================
---- QUALITY OF erp_loc_a101 ----
------ BRONZE table ------

SELECT 
	id, 
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

SELECT * FROM [silver].[crm_prd_info];

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE id not in (SELECT cat_id FROM [silver].[crm_prd_info]);


-- Check for unwanted Spaces
-- Expectation: No Result
SELECT 
	id, 
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)


-- Data Standardization & Consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;


SELECT *
FROM silver.erp_px_cat_g1v2
