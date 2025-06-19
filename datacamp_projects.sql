-- DataCamp SQL projects 

/* 
Project 1. Analyzing and Formatting PostgreSQL Sales Data 

orders:
Column	Definition	Data type	Comments
row_id	Unique Record ID	INTEGER	
order_id	Identifier for each order in table	TEXT	Connects to order_id in returned_orders table
order_date	Date when order was placed	TEXT	
market	Market order_id belongs to	TEXT	
region	Region Customer belongs to	TEXT	Connects to region in people table
product_id	Identifier of Product bought	TEXT	Connects to product_id in products table
sales	Total Sales Amount for the Line Item	DOUBLE PRECISION	
quantity	Total Quantity for the Line Item	DOUBLE PRECISION	
discount	Discount applied for the Line Item	DOUBLE PRECISION	
profit	Total Profit earned on the Line Item	DOUBLE PRECISION

products:
Column	Definition	Data type
product_id	Unique Identifier for the Product	TEXT
category	Category Product belongs to	TEXT
sub_category	Sub Category Product belongs to	TEXT
product_name	Detailed Name of the Product	TEXT

1. Find the top 5 products from each category based on highest total sales. 
The output should be sorted by category in ascending order and by sales in descending order within each category, 
i.e. within each category product with highest margin should sit on the top. 
*/ 

-- top_five_products_each_category
with products_ranked as (
	select 
		o.product_id,
		p.product_name,
		p.category, 
		round(sum(o.sales)::numeric, 2) as product_total_sales,
		round(sum(o.profit)::numeric, 2) as product_total_profit,
		row_number() over(partition by p.category order by sum(o.sales) desc) as product_rank
	from orders as o
	join products as p on p.product_id = o.product_id
	group by o.product_id, p.product_name, p.category
)
select 
	category,
	product_name,
	product_total_sales,
	product_total_profit,
	product_rank
from products_ranked
where product_rank <= 5
order by category, product_total_sales desc

/* 
2. Calculate the quantity for orders with missing values in the quantity column by determining the unit price 
for each product_id using available order data, considering relevant pricing factors such as discount, market, or region. 
Then, use this unit price to estimate the missing quantity values. The calculated values should be stored in the calculated_
quantity column. Save query output as impute_missing_values, containing the following columns:
product_id, discount, market, region, sales, quantity, calculated_quantity (rounded to zero decimal places)
*/ 

with missing_quantity as (
	select 
		product_id,
		discount,
		market,
		region, 
		sales,
		quantity
	from orders 
	where quantity is null
),
calculated_unit_price as (
	select 
		product_id,
		market,
		region,
		avg(((sales / quantity) - discount)::numeric) as calculated_unit_price_avg
	from orders 
	where quantity is not null
	group by 1,2,3
	order by product_id, market, region
)
select 
	mq.product_id,
	mq.discount,
	mq.market,
	mq.region,
	mq.sales,
	mq.quantity,
	round((mq.sales::numeric / cup.calculated_unit_price_avg),0) as calculated_quantity
from missing_quantity as mq 
left join calculated_unit_price cup 
	on mq.product_id = cup.product_id
	and mq.market = cup.market
	and mq.region = cup.region