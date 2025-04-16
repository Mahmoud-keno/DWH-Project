create view gold.dim_customers as 
select 
	row_number() over(order by c.cst_id) as customer_key,
	c.cst_id as customer_id,
	c.cst_key as customer_number,
	c.cst_first_name as first_name,
	c.cst_last_name as last_name,
	cl.cntry as country,
	c.cst_marital_status as marital_status,
	case
	when c.cst_gndr != 'n/a' then c.cst_gndr 
	else coalesce(null,'n/a')
	end as gender,
	ci.bdate as birthdate,
	c.cst_create_date as create_date
from silver.crm_cust_info c 
left join 
	silver.erp_cust_az12 ci
	on c.cst_key = ci.cid
left join 
	silver.erp_loc_a101 cl
	on c.cst_key = cl.cid