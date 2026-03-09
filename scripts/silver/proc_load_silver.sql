/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


create or alter procedure silver.load_silver as 
begin 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '============================================================';
		print 'Loading silver layer';
		print '============================================================';

		print '============================================================';
		print 'Loading CRM Tables';
		print '============================================================';

	-- Loading silver.crm_cust_info
	set @start_time = GETDATE();
	print '>> Truncating Table: silver.crm_cust_info'
	truncate table silver.crm_cust_info
	print '>> Inserting Data Into: silver.crm_cust_info'
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 when upper(trim(cst_marital_status)) = 'M' then 'Married'
		else 'n/a'
		end cst_marital_status,  -- Normalize marital status values to readable format
		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male'
		else 'n/a'
		end cst_gndr, -- Normalize gender values to readable format
		cst_create_data
	from(
	select 
		*,
		row_number() over(partition by cst_id order by cst_create_data desc) flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)t where flag_last=1  -- Select the most recent reocrd per customer
	set @end_time = GETDATE();
	print '>> Load Duration: ' +cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
	print '>> --------------------';

	-- Loading silver.crm_prd_info
	set @start_time = GETDATE();
	print '>> Truncating Table: silver.crm_prd_info'
	truncate table silver.crm_cust_info
	print '>> Inserting Data Into: silver.crm_prd_info'
	insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5), '-', '_') as cat_id,  -- Extract Category ID
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,		  -- Extract Product Key
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end prd_line,			--Map product line codes to descriptive values
		cast(prd_start_dt as date) as prd_start_dt,
		cast(
			lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 
			as date
		) as prd_end_dt		-- Calculate end date as one day before the next start date
	from bronze.crm_prd_info
	set @end_time = GETDATE();
	print 'Load Duration: ' +cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
	print '>> -------------------'

	-- Loading silver.crm_sales_details
	set @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT '>> Inserting Data Into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL OR sls_price <= 0 
						THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price  -- Derive price if original value is invalid
				END AS sls_price
			FROM bronze.crm_sales_details;
			set @end_time = GETDATE();
			print 'Load Duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
			print '>> ---------------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
			-- Loading silver.erp_cust_az12
			set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT '>> Inserting Data Into: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12 (
				cid,
				bdate,
				gen
			)
			SELECT
				CASE
					WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
					ELSE cid
				END AS cid, 
				CASE
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate, -- Set future birthdates to NULL
				CASE
					WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					ELSE 'n/a'
				END AS gen -- Normalize gender values and handle unknown cases
			FROM bronze.erp_cust_az12;
			set @end_time = GETDATE();
			print 'Load Duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
			print '>> ---------------------';

	--Loading silver.erp_loc_a101
	set @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '>> Inserting Data Into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101 (
				cid,
				cntry
			)
			SELECT
				REPLACE(cid, '-', '') AS cid, 
				CASE
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					ELSE TRIM(cntry)
				END AS cntry -- Normalize and Handle missing or blank country codes
			FROM bronze.erp_loc_a101;
			set @end_time = GETDATE();
			print 'Load Duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
			print '>> ---------------------';

	-- Loading silver.erp_px_cat_g1v2
	set @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	set @end_time = GETDATE();
			print 'Load Duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds';
			print '>> ---------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end
