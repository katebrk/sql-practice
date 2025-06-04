/* 
Write and optimize a query to find top 5 clients by total transaction amount in the last year. 

Tables:

1 - clients
client_id INT 
client_name VARCHAR(100)

2 - transactions 
transaction_id INT 
client_id INT 
amount DECIMAL(18, 2) 
transaction_date DATE 
*/ 


-- 1st step, writing query 
select 
	c.client_id, 
	c.client_name,
	sum(t.amount) as total_amount 
from transactions as t 
join clients as c on c.client_id = t.client_id 
where 
	--date_trunc('year', transaction_date) = '2024' - this works in PostgreSQL, but not in SQL server 
	transaction_date >= '2024-01-01' and transaction_date < '2025-01-01' --for filtering exact dates for the LY 
	--transaction_date >= dateadd(year, -1, getdate()) //for filtering from today's date
group by c.client_id, c.client_name 
order by total_amount DESC
limit 5 

-- 2nd step, optimize by using CTE to isolate relevant data entry 
-- this reduces # of rows in memore before joining > imrpoved perfomance 
with last_year_transactions as (
	select 
		client_id,
		amount
	from transactions 
	where transaction_date >= '2024-01-01' and transaction_date < '2025-01-01'
	-- where transaction_date >= dateadd(year, -1, getdate()) //for filtering from today's date

)
select 
	c.client_id, 
	c.client_name,
	sum(t.amount) as total_amount 
from last_year_transactions as t 
join clients as c on c.client_id = t.client_id  
group by c.client_id, c.client_name 
order by total_amount DESC
limit 5 