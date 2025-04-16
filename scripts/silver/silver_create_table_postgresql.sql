-- Drop and create CRM customer info table
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id int,
    cst_key varchar(50),
    cst_first_name varchar(50),
    cst_last_name varchar(50),
    cst_material_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date DATE,
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create CRM product info table
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt timestamp,
    prd_end_dt timestamp,
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create CRM sales details table
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num varchar(50),
    sls_prd_key varchar(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create ERP customer AZ12 table
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    CID varchar(50),
    BDATE timestamp,
    GEN varchar(10),
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create ERP location A101 table
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    CID varchar(50),
    CNTRY varchar(50),
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Drop and create ERP product category table
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    ID varchar(50),
    CAT varchar(50),
    SUBCAT varchar(50),
    MAINTENANCE varchar(50),
    dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP
);
