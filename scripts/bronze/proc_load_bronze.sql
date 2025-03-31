/*
====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================================
Script Purpose:
      This stored procedure loads data into the 'bronze' schema from external CSV files.
      It performs the following actions:
          - Truncates the bronze tables before loading data.
          - Uses the 'BULK INSERT command to load data from csv Files to bronze tables.
      
Parameters:
    None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze. load bronze;
====================================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    err_message TEXT;
	start_time TIMESTAMP;
    err_code TEXT;
	end_time TIMESTAMP;
	load_duration NUMERIC;
BEGIN

	RAISE NOTICE '===============================';
    RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '===============================';

	RAISE NOTICE '---------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '---------------------------------';


-------- CRM_FILE 1 -------

	start_time := NOW();

RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';

TRUNCATE TABLE bronze.crm_cust_info; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';

COPY bronze.crm_cust_info 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' 
DELIMITER ',' 
CSV HEADER;

	end_time := NOW();
	load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';

-------- CRM_FILE 2 -------
	start_time := NOW();

RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';

TRUNCATE TABLE bronze.crm_prd_info; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
COPY bronze.crm_prd_info 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' 
DELIMITER ',' 
CSV HEADER;

end_time := NOW();
   load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';



-------- CRM_FILE 3 -------

	start_time := NOW();
    

RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';

TRUNCATE TABLE bronze.crm_sales_details; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
COPY bronze.crm_sales_details 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' 
DELIMITER ',' 
CSV HEADER;

	end_time := NOW();

	load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';
	 
RAISE NOTICE '---------------------------------';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '---------------------------------';


-------- ERP_FILE 1 -------
	start_time := NOW();
    

RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';

TRUNCATE TABLE bronze.erp_cust_az12; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';

COPY bronze.erp_cust_az12 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' 
DELIMITER ',' 
CSV HEADER;

	end_time := NOW();

	load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';

-------- ERP_FILE 2 -------

	start_time := NOW();
    
	
RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';

TRUNCATE TABLE bronze.erp_loc_a101; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101 ';
COPY bronze.erp_loc_a101 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' 
DELIMITER ',' 
CSV HEADER;

	end_time := NOW();
    load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';
-------- ERP_FILE 3 -------

	start_time := NOW();
    

RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- Removes all rows from bronze.crm_cust_info

RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2  ';
COPY bronze.erp_px_cat_g1v2 
FROM '/Users/noahscomputer/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' 
DELIMITER ',' 
CSV HEADER;

	end_time := NOW();

   load_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	 RAISE NOTICE '>>> Load Duration: % seconds', load_duration;
	 RAISE NOTICE '>>> --------------';


 	RAISE NOTICE '=====================================';
   	RAISE NOTICE 'Bronze Layer Loaded Successfully';
    RAISE NOTICE '=====================================';

	EXCEPTION 
    WHEN others THEN
        err_message := SQLERRM;
        err_code := SQLSTATE;
        RAISE NOTICE '============================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', err_message;
        RAISE NOTICE 'Error Code: %', err_code;
		RAISE NOTICE '============================';
END;
$$;

CALL bronze.load_bronze();
