/*
==================================================================
Quality Checks
==================================================================
Script Purpose:
  This script performs various quality checks for data consistency, 
  accuracy, and Standardization across the 'silver' schema. It 
  includes checks for:
- Null of duplicate primary keys.
- Unwanted spaces in string fields.
- Data Standardization and Conssitency
- Invalid data ranges and orders.
- Data consistency between related fields.

Using Note:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
==================================================================
*/

-- Check For Nulls or Duplicates in Primary Key
-- Expection: No Result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 


-- Check unwanted spaces
-- Expection : No Result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_cust_info

-- Check Invalid Date Orders
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt



----------------------------------------

-- Identify Out-of_Range Dates

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' and bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'N/A'
END AS gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12



----------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT 
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	 ELSE TRIM(cntry)
END AS cntry
FROM silver.erp_loc_a101
ORDER BY cntry


---------------------------------------

-- Check for unwantedSpaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency 
SELECT DISTINCT 
maintenance
FROM 
bronze.erp_px_cat_g1v2
