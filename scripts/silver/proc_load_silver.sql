CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,
	@end_time DATETIME,
	@batch_start_time DATETIME,
	@batch_end_time DATETIME;
	BEGIN TRY
	  SET @batch_start_time = GETDATE();
		PRINT '=======================================' 
		PRINT 'Loading Bronze Layer'
		PRINT '=======================================' 
		PRINT '-----------------'

		SET @start_time = GETDATE();
		PRINT 'Loading CRM Tables'
		PRINT '-----------------'
		TRUNCATE TABLE silver.crm_cust_info;
		BULK INSERT silver.crm_cust_info
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE silver.crm_prd_info;
		BULK INSERT silver.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE silver.crm_sales_details;
		BULK INSERT silver.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT('>>> ---------')
		SET @start_time = GETDATE();
			PRINT '-----------------'
		PRINT 'Loading ERP Tables'
		PRINT '-----------------'
		TRUNCATE TABLE silver.erp_cust_az12;
		BULK INSERT silver.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE silver.erp_loc_a101;
		BULK INSERT silver.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		BULK INSERT silver.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT('>>> ---------')

			SET @batch_end_time = GETDATE();
	PRINT '================================'
	PRINT'Bronzer Layer is completed'
	PRINT 'TOTAL DURATION :' + CAST( DATEDIFF(second, @batch_start_time, @batch_end_time)	as	nvarchar ) + ' seconds';
	PRINT '================================'


	END TRY 
	BEGIN CATCH
		PRINT '----------------'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '----------------'

	END CATCH
END
select * from silver.erp_loc_a101
