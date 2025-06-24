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
	
	
/* Project 2. Factors that Fuel Student Performance

student_performance (hours_studied, attendance, extracurricular_activities, sleep_hours, tutoring_sessions, teacher_quality, exam_score) 

1. Do more study hours and extracurricular activities lead to better scores? 
Analyze how studying more than 10 hours per week, while also participating in extracurricular activities, impacts exam performance. 
The output should include two columns: 
1) hours_studied and 
2) avg_exam_score. 
Group and sort the results by hours_studied in descending order. 
Save the query as avg_exam_score_by_study_and_extracurricular.
*/ 

SELECT 
	hours_studied,
	avg(exam_score) as avg_exam_score
FROM student_performance
where extracurricular_activities ilike 'Yes'
	and hours_studied > 10
group by hours_studied
order by hours_studied desc
limit 30

/* 2. Is there a sweet spot for study hours? 
Explore how different ranges of study hours impact exam performance by calculating the average exam score 
for each study range. Categorize students into four groups based on hours studied per week: 1-5 hours, 6-10 hours, 
11-15 hours, and 16+ hours. The output should contain two columns: 1) hours_studied_range and 2) avg_exam_score. 
Group the results by hours_studied_range and sort them by avg_exam_score in descending order.
*/ 

select 
	case 
		when hours_studied >= 1 and hours_studied <= 5 then '1-5 hours'
		when hours_studied >= 6 and hours_studied <= 10 then '6-10 hours'
		when hours_studied >= 11 and hours_studied <= 15 then '11-15 hours'
		when hours_studied >= 16 then '16+ hours'
	end as hours_studied_range,
	avg(exam_score) as avg_exam_score
from student_performance
group by hours_studied_range
order by avg_exam_score desc

/* 3. A teacher wants to show their students their relative rank in the class, without revealing their exam scores to each other. 
Use a window function to assign ranks based on exam_score, ensuring that students with the same exam score share the same rank 
and no ranks are skipped. Return the columns attendance, hours_studied, sleep_hours, tutoring_sessions, and exam_rank. 
The students with the highest exam score should be at the top of the results, so order your query by exam_rank in ascending order. 
Limit your query to 30 students.
*/ 

select 
	attendance,
	hours_studied, 
	sleep_hours,
	tutoring_sessions,
	dense_rank() over(order by exam_score desc) as exam_rank
from student_performance
order by exam_rank
limit 30

/* Project 3. Analyzing Motorcycle Part Sales

The board of directors wants to gain a better understanding of wholesale revenue by product line, 
and how this varies month-to-month and across warehouses. You have been tasked with calculating net revenue for each product line 
and grouping results by month and warehouse. 
The results should be filtered so that only "Wholesale" orders are included.

sales (order_number, date, warehouse, client_type, product_line, quantity, unit_price, total, payment, payment_fee) 

Task. Find out how much "Wholesale" net revenue each product_line generated per month per warehouse in the dataset. Contain the following: 
product_line, 
month (displayed as 'June', 'July', and 'August'), 
warehouse, 
net_revenue (the sum of total minus the sum of payment_fee). 

The results should be sorted by product_line and month, followed by net_revenue in descending order.
*/ 

select 
	product_line,
	case 
		when date between '2021-06-01' and '2021-06-30' then 'June'
		when date between '2021-07-01' and '2021-07-31' then 'July'
		when date between '2021-08-01' and '2021-08-31' then 'August'
	end as month, 
	warehouse, 
	sum(total) - sum(payment_fee) as net_revenue
from sales 
where client_type ilike 'Wholesale'
group by product_line, month, warehouse



