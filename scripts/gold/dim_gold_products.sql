create view gold.dim_products as 
select 
row_number() over(order by p.prd_start_dt) as product_key,
p.prd_id as product_id,
p.prd_key as product_number,
p.prd_nm as product_name,
p.cat_id as category_id,
pc.cat as category,
pc.subcat as sub_category,
pc.maintenance,
p.prd_cost as cost,
p.prd_line as product_line,
p.prd_start_dt as start_date
from silver.crm_prd_info p
left join 
silver.erp_px_cat_g1v2 pc
on p.cat_id = pc.id
where p.prd_end_dt is null
