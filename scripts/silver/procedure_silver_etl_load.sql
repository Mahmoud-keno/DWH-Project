CREATE OR REPLACE PROCEDURE silver.silver_etl_load()
LANGUAGE plpgsql
AS $$
DECLARE
    proc_start_time timestamp := clock_timestamp();
    step_start_time timestamp;
    rows_affected bigint;
    error_message text;
    error_context text;
BEGIN
    RAISE NOTICE '========================================================================';
    RAISE NOTICE '                      SILVER LAYER ETL PROCESS                         ';
    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'Start Time: %', proc_start_time;
    RAISE NOTICE '========================================================================';

    -- Create schema if not exists
    CREATE SCHEMA IF NOT EXISTS silver;
    
    -- CRM Customer Info Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '1. Loading CRM Customer Data...';
        
        TRUNCATE TABLE silver.crm_cust_info;
        
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_first_name, cst_last_name, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id, cst_key,
            trim(' ' from cst_firstname) as cst_first_name,
            trim(' ' from cst_lastname) as cst_last_name,
            CASE
                WHEN UPPER(trim(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(trim(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END as cst_marital_status,
            CASE 
                WHEN UPPER(trim(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(trim(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END as cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
            RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date ASC) as flag_last
            FROM bronze.crm_cust_info
        ) subquery
        WHERE flag_last = 1 AND cst_id IS NOT NULL;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into crm_cust_info (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading crm_cust_info: % | Context: %', error_message, error_context;
    END;

    -- CRM Product Info Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '2. Loading CRM Product Data...';
        
        TRUNCATE TABLE silver.crm_prd_info;
        
        INSERT INTO silver.crm_prd_info(
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            replace(left(prd_key, 5), '-', '_') as cat_id,
            right(prd_key, length(prd_key) - 6) as prd_key,
            prd_nm,
            prd_cost,
            CASE upper(trim(prd_line))
                WHEN 'R' THEN 'Road' 
                WHEN 'M' THEN 'Mountain' 
                WHEN 'S' THEN 'Other Sales' 
                WHEN 'T' THEN 'Touring' 
                ELSE 'n/a'
            END as prd_line,
            prd_start_dt,
            (lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - interval '1 day') as prd_end_dt
        FROM bronze.crm_prd_info;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into crm_prd_info (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading crm_prd_info: % | Context: %', error_message, error_context;
    END;

    -- CRM Sales Details Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '3. Loading CRM Sales Data...';
        
        DROP TABLE IF EXISTS silver.crm_sales_details;
        CREATE TABLE silver.crm_sales_details(
            sls_ord_num varchar(50),
            sls_prd_key varchar(50),
            sls_cust_id int,
            sls_order_dt date,
            sls_ship_dt date,
            sls_due_dt date,
            sls_sales int,
            sls_quantity int,
            sls_price int,
            dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP 
        );
        
        INSERT INTO silver.crm_sales_details(
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN length(cast(sls_order_dt as varchar(8))) != 8 or sls_order_dt = 0 THEN null
                ELSE to_date(cast(sls_order_dt as varchar(8)),'YYYYMMDD')
            END as sls_order_dt,
            CASE
                WHEN length(cast(sls_ship_dt as varchar(8))) != 8 or sls_ship_dt = 0 THEN null
                ELSE to_date(cast(sls_ship_dt as varchar(8)),'YYYYMMDD')
            END as sls_ship_dt,
            CASE
                WHEN length(cast(sls_due_dt as varchar(8))) != 8 or sls_due_dt = 0 THEN null
                ELSE to_date(cast(sls_due_dt as varchar(8)),'YYYYMMDD')
            END as sls_due_dt,
            CASE
                WHEN sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
                THEN sls_quantity * abs(sls_price)
                ELSE sls_sales
            END as sls_sales,
            CASE
                WHEN sls_quantity <= 0 or sls_quantity is null or sls_quantity != abs(sls_sales)/abs(sls_price) THEN abs(sls_sales)/abs(sls_price) 
                ELSE sls_quantity
            END as sls_quantity,
            CASE
                WHEN sls_price <= 0 or sls_price is null or sls_price != abs(sls_sales)/abs(sls_quantity) THEN abs(sls_sales)/abs(sls_quantity)
                ELSE sls_price
            END as sls_price
        FROM bronze.crm_sales_details;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into crm_sales_details (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading crm_sales_details: % | Context: %', error_message, error_context;
    END;

    -- ERP Customer AZ12 Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '4. Loading ERP Customer AZ12 Data...';
        
        TRUNCATE TABLE silver.erp_cust_az12;
        
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT 
            CASE 
                WHEN cid like 'NAS%' THEN right(cid, length(cid)-3)
                ELSE cid
            END as cid,
            CASE
                WHEN bdate > current_date THEN null
                ELSE bdate
            END as bdate,
            CASE
                WHEN upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
                WHEN upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END as gen
        FROM bronze.erp_cust_az12;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into erp_cust_az12 (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading erp_cust_az12: % | Context: %', error_message, error_context;
    END;

    -- ERP Location A101 Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '5. Loading ERP Location A101 Data...';
        
        TRUNCATE TABLE silver.erp_loc_a101;
        
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT 
            replace(cid,'-',''),
            CASE
                WHEN upper(trim(cntry)) in ('DE') THEN 'Germany'
                WHEN upper(trim(cntry)) in ('US','USA') THEN 'United States'
                WHEN (trim(cntry)) = '' or cntry is null THEN 'n/a'
                ELSE trim(cntry)
            END as cntry
        FROM bronze.erp_loc_a101;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into erp_loc_a101 (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading erp_loc_a101: % | Context: %', error_message, error_context;
    END;

    -- ERP PX Category G1V2 Load
    BEGIN
        step_start_time := clock_timestamp();
        RAISE NOTICE '6. Loading ERP PX Category Data...';
        
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT * FROM bronze.erp_px_cat_g1v2;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        RAISE NOTICE '   - Loaded % rows into erp_px_cat_g1v2 (Duration: %)', 
            rows_affected, 
            (clock_timestamp() - step_start_time);
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT,
                                   error_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Error loading erp_px_cat_g1v2: % | Context: %', error_message, error_context;
    END;

    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'ETL Process Completed Successfully';
    RAISE NOTICE 'Total Duration: %', (clock_timestamp() - proc_start_time);
    RAISE NOTICE 'End Time: %', clock_timestamp();
    RAISE NOTICE '========================================================================';
END;
$$;
call silver.silver_etl_load();