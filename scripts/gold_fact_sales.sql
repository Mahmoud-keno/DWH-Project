create view gold.fact_sales as
select 
	s.sls_ord_num as order_number,
	gc.customer_key ,
	gp.product_key,
	s.sls_order_dt as order_date,
	s.sls_ship_dt as shipping_date,
	s.sls_due_dt as due_date,
	s.sls_sales as sales_amount,
	s.sls_quantity as quantity,
	s.sls_price as price
from silver.crm_sales_details s
left join 
	gold.dim_customers gc
	on s.sls_cust_id = gc.customer_id
left join 
	gold.dim_products gp
	on s.sls_prd_key = gp.product_number
