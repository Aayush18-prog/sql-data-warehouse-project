/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
	set @batch_start_time = GETDATE();
	print '========================================';
	print 'Loading Bronze Layer.....';
	print '========================================';
	print '                                        '
	print '---------------------------------------';
	print 'Loading crm tables';
	print '---------------------------------------';

	
	-- For cust_info
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;
	print '>> Inserting Data Into: bronze.crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'cust_info loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
	print '>> ---------------------------';

	
	-- For prd_info
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;
	print '>> Inserting Data Into: bronze.crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'prd_info loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '>> ------------------------';

	
	-- For sales_details
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details;
	print '>> Inserting Data Into: bronze.crm_sales_details';
	bulk insert bronze.crm_sales_details
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'sales_details loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '>> ------------------------';

	print '---------------------------------------'
	print 'Loading erp tables'
	print '---------------------------------------'


	-- For erp_cust_az12
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12;
	print '>> Inserting Data Into: bronze.erp_cust_az12';
	bulk insert bronze.erp_cust_az12
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'cust_az12 loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '>> ------------------------';


	-- For erp_loc_a101
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;
	print '>> Inserting Data Into: bronze.erp_loc_a101';
	bulk insert bronze.erp_loc_a101
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'loc_a101 loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '>> ------------------------';


	-- For erp_px_cat_g1v2
	set @start_time = GETDATE();
	print '>> Truncating Table: bronze.erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2;
	print '>> Inserting Data Into: bronze.erp_erp_cat_g1v2'
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
	with (
	  firstrow = 2,
	  fieldterminator = ',',
	  tablock
	);
	print 'px_cat_g1v2 loaded successfully'
	set @end_time = GETDATE();
	print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '>> ------------------------';

	set @batch_end_time = GETDATE();
	print '================================='
	print 'Loading Bronze Layer is Completed';
	print '-Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
	print '================================='
	end try
	begin catch
		print '=========================='
		print 'Error occured during loading bronze layer'
		print 'Error Message:' + ERROR_MESSAGE()
		print 'Error Message:' + cast(error_number() as nvarchar);
		print 'Error Message:' + cast(error_state() as nvarchar);
	end catch
end
