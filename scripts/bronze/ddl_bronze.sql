/*
==========================================
DDL Scripts: create bronze tables 
Pusrose: this scripts create tables for bronze schema and drop tables if already exist 
==========================================
*/
if OBJECT_ID('bronze.crm_cust_info','U') is not null
	drop table bronze.crm_cust_info
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_first_name nvarchar(50),
cst_last_name nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date DATE
);
if OBJECT_ID('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);
if OBJECT_ID('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details
create table bronze.crm_sales_details(
sls_ord_num	nvarchar(50),
sls_prd_key	nvarchar(50),
sls_cust_id	int,
sls_order_dt int,	
sls_ship_dt	int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
if OBJECT_ID('bronze.erp_cust_az12','U') is not null
	drop table bronze.erp_cust_az12
create table bronze.erp_cust_az12(

CID nvarchar(50),	
BDATE datetime,
GEN nvarchar(10)
);
if OBJECT_ID('bronze.erp_loc_a101') is not null
	drop table bronze.erp_loc_a101
create table bronze.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50)
);
if OBJECT_ID('bronze.erp_px_cat_g1v2') is not null
	drop table bronze.erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2(
ID	nvarchar(50),
CAT	nvarchar(50),
SUBCAT	nvarchar(50),
MAINTENANCE nvarchar(50)
);
