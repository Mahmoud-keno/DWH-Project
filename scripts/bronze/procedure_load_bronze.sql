/*
Stored Procedure: Load bronze(Source -> Bronze)
Purpose: this script load data into tables bronze schema from external csv file
Action:
1- truncate bronze tables before loading
2- use insert bulk to load data from csv file
Usage:
ecex bronze.load_bronze
*/
create or alter procedure bronze.load_bronze 
as
begin
declare @start_time datetime,@end_time datetime
begin try
print '========================================================================'
print '                             Bronze CRM Layer                               '
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <crm_cust_info>                     '
set @start_time = GETDATE();
truncate table bronze.crm_cust_info
print '                   Inserting Table <crm_cust_info>                      '
bulk insert bronze.crm_cust_info
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds' 
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <crm_prd_info>                      '
set @start_time = GETDATE()
truncate table bronze.crm_prd_info
print '                   Inserting Table <crm_cust_info>                      '
bulk insert bronze.crm_prd_info
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds'  
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <crm_sales_detalis>                 '
set @start_time = GETDATE();
truncate table bronze.crm_sales_details
print '                   Inserting Table <crm_sales_detalis>                  '
bulk insert bronze.crm_sales_details
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds' 

print '========================================================================'
print '                             Bronze CRM Layer                               '
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <erp_cust_az12>                 '
set @start_time = GETDATE();
truncate table bronze.erp_cust_az12
print '                   Inserting Table <erp_cust_az12>                  '
bulk insert bronze.erp_cust_az12
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds' 
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <erp_loc_a101>                     '
set @start_time = GETDATE();
truncate table bronze.erp_loc_a101
print '                   Inserting Table <erp_loc_a101>                      '
bulk insert bronze.erp_loc_a101
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds' 
print '========================================================================'
print '========================================================================'
print '                   Truncating Table <erp_px_cat_g1v2>                      '
set @start_time = GETDATE();
truncate table bronze.erp_px_cat_g1v2
print '                   Inserting Table <erp_px_cat_g1v2>                       '
bulk insert bronze.erp_px_cat_g1v2
from 'D:\My\my study\Data Warehouse\DWH Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time = GETDATE();
print '>>> Load Time = ' + cast(datediff(second , @start_time , @end_time) as nvarchar(50)) + 'seconds' 
print '========================================================================'
end try
begin catch
print '========================================================================'
print '                                Error                              '
print 'Error Message: '+ error_message();
print 'Error Number ' + cast(error_number() as nvarchar(50));
print 'Error Status ' + cast(error_state() as nvarchar(50));
print '========================================================================'
end catch
end
exec bronze.load_bronze 
