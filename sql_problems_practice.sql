--SQL


/* 01
A Microsoft Azure Supercloud customer is defined as a customer who has purchased at least one product from every product category listed in the products table.
Write a query that identifies the customer IDs of these Supercloud customers.
*/

with supercloud_cust as (
select
  customer_id,
  count(DISTINCT product_category) as product_category_count
from customer_contracts as cc
inner join products as p on p.product_id = cc.product_id
group by customer_id
order by customer_id)

select 
  customer_id
from supercloud_cust
where product_category_count = (select count(DISTINCT product_category) from products)


/* 02
Assume you're given a table with measurement values obtained from a Google sensor over multiple days with measurements taken multiple times within each day.
Write a query to calculate the sum of odd-numbered and even-numbered measurements separately for a particular day and display the results in two different columns. 
Refer to the Example Output below for the desired format.
*/

with ranked_measurements as(
select 
  *,
  row_number() over(PARTITION by measurement_time::date order by measurement_time) as ranked
from measurements 
order by measurement_time)

select 
  measurement_time::date,
  sum(case when ranked%2 = 1 then measurement_value else 0 end) as odd_sum,
  sum(case when ranked%2 = 0 then measurement_value else 0 end) as even_sum
from ranked_measurements
group by measurement_time::date
order by measurement_time


/* 03
Zomato is a leading online food delivery service that connects users with various restaurants and cuisines, allowing them to browse menus, place orders, and get meals delivered to their doorsteps.
Recently, Zomato encountered an issue with their delivery system. Due to an error in the delivery driver instructions, each item's order was swapped with the item in the subsequent row. 
As a data analyst, you're asked to correct this swapping error and return the proper pairing of order ID and item.
If the last item has an odd order ID, it should remain as the last item in the corrected data. 
For example, if the last item is Order ID 7 Tandoori Chicken, then it should remain as Order ID 7 in the corrected data.
In the results, return the correct pairs of order IDs and items.
*/

with order_counts as(
select count(distinct order_id) as total_orders
from orders)

select 
  --order_id,
  case 
    when order_id%2 != 0 and order_id!= total_orders then order_id+1
    when order_id%2 != 0 and order_id = total_orders then order_id
    else order_id-1 
  end as corrected_order_id,
  item
from orders
cross join order_counts
order by corrected_order_id


/* 04
Assume you are given the table below on Uber transactions made by users. Write a query to obtain the third transaction of every user. Output the user id, spend and transaction date.
*/

with ranked_transactions as(
select *,
  dense_rank() over(partition by user_id order by transaction_date asc) as transaction_number
from transactions)

select 
  user_id,
  spend,
  transaction_date
from ranked_transactions
where transaction_number = 3


/* 05
Imagine you're an HR analyst at a tech company tasked with analyzing employee salaries. 
Your manager is keen on understanding the pay distribution and asks you to determine the second highest salary among all employees.
It's possible that multiple employees may share the same second highest salary. In case of duplicate, display the salary only once.
*/

with ranked_salaries as(
select *,
  dense_rank() over(order by salary desc) as ranked_salary
from employee)

select 
  salary as second_highest_salary
from ranked_salaries
where ranked_salary=2


/* 06
Given a table of tweet data over a specified time period, calculate the 3-day rolling average of tweets for each user. 
Output the user ID, tweet date, and rolling averages rounded to 2 decimal places.
*/

select 
  user_id,
  tweet_date,
  --tweet_count,
  round(avg(tweet_count) 
      over(PARTITION BY user_id order by tweet_date 
      rows between 2 preceding and current row), 2) as rolling_avg_3d
from tweets

/* 08
The Bloomberg terminal is the go-to resource for financial professionals, offering convenient access to a wide array of financial datasets. 
As a Data Analyst at Bloomberg, you have access to historical data on stock performance.

Currently, you're analyzing the highest and lowest open prices for each FAANG stock by month over the years.

For each FAANG stock, display the ticker symbol, the month and year ('Mon-YYYY') with the corresponding highest and lowest open prices (refer to the Example Output format). 
Ensure that the results are sorted by ticker symbol.
*/

with ranked_open_price as(
select 
  TO_CHAR(date, 'Mon-YYYY') AS month_year,
  ticker,
  open,
  row_number() over(PARTITION by ticker order by open asc) as rnk_open_lowest,
  row_number() over(PARTITION by ticker order by open desc) as rnk_open_highest
from stock_prices)

select 
  ticker,
  max(month_year) filter (where rnk_open_highest = 1) as highest_mth,
  max(open) filter (where rnk_open_highest = 1) as highest_open,
  max(month_year) filter (where rnk_open_lowest = 1) as lowest_mth,
  max(open) filter (where rnk_open_lowest = 1) as lowest_open
from ranked_open_price
group by ticker
order by ticker



/* 09
In an effort to identify high-value customers, Amazon asked for your help to obtain data about users who go on shopping sprees. 
A shopping spree occurs when a user makes purchases on 3 or more consecutive days.

List the user IDs who have gone on at least 1 shopping spree in ascending order.
*/

SELECT 
  distinct T1.user_id
FROM transactions AS T1
INNER JOIN transactions AS T2
  ON DATE(T2.transaction_date) = DATE(T1.transaction_date) + 1
INNER JOIN transactions AS T3
  ON DATE(T3.transaction_date) = DATE(T1.transaction_date) + 2
WHERE t1.user_id = t2.user_id and t2.user_id = t3.user_id
order by T1.user_id



/* 10
Assume you're given a table on Walmart user transactions. Based on their most recent transaction date, write a query that retrieve the users along with the number of products they bought.

Output the user's most recent transaction date, user ID, and the number of products, sorted in chronological order by the transaction date.
*/

with ranked_date as(
select 
  user_id,
  transaction_date,
  count(product_id) as purchase_count,
  rank() over(PARTITION by user_id order by transaction_date desc) as rank_date
from user_transactions
group by user_id, transaction_date
order by user_id)

select 
  transaction_date,
  user_id,
  purchase_count
from ranked_date
where rank_date = 1
order by transaction_date


/* 11
You're given a table containing the item count for each order on Alibaba, along with the frequency of orders that have the same item count. 
Write a query to retrieve the mode of the order occurrences. 
Additionally, if there are multiple item counts with the same mode, the results should be sorted in ascending order.
*/ 

with ranked_oo as (
select 
  item_count,
  order_occurrences,
  dense_rank() over(order by order_occurrences desc) as ranked_order_occurrences
from items_per_order)

select 
  item_count as mode
from ranked_oo
where ranked_order_occurrences = 1
order by item_count


/* 12
Your team at JPMorgan Chase is soon launching a new credit card. You are asked to estimate how many cards you'll issue in the first month.

Before you can answer this question, you want to first get some perspective on how well new credit card launches typically do in their first month.

Write a query that outputs the name of the credit card, and how many cards were issued in its launch month. 
The launch month is the earliest record in the monthly_cards_issued table for a given card. Order the results starting from the biggest issued amount.
*/


SELECT 
  DISTINCT card_name,
  FIRST_VALUE(issued_amount) OVER(PARTITION BY card_name ORDER BY issue_year, issue_month) AS issued_amount
FROM monthly_cards_issued
ORDER BY issued_amount DESC


/* 13
A phone call is considered an international call when the person calling is in a different country than the person receiving the call.

What percentage of phone calls are international? Round the result to 1 decimal.

Assumption: The caller_id in phone_info table refers to both the caller and receiver.
*/

with table1 as(
select 
  c.caller_id,
  c.receiver_id,
  i1.country_id as caller_country,
  i2.country_id as receiver_country
from phone_calls as c
left join phone_info as i1 on c.caller_id = i1.caller_id
left join phone_info as i2 on c.receiver_id = i2.caller_id)

select 
  round(100.0 * sum(case when caller_country != receiver_country then 1 else 0 end) 
  / count(*), 1) as international_calls_pct
from table1


/* 14
UnitedHealth Group (UHG) has a program called Advocate4Me, which allows policy holders (or, members) to call an advocate and receive support for their health care needs – whether that's claims and benefits support, drug coverage, pre- and post-authorisation, medical records, emergency assistance, or member portal services.

Calls to the Advocate4Me call centre are classified into various categories, but some calls cannot be neatly categorised. These uncategorised calls are labeled as “n/a”, or are left empty when the support agent does not enter anything into the call category field.

Write a query to calculate the percentage of calls that cannot be categorised. Round your answer to 1 decimal place. For example, 45.0, 48.5, 57.7.
*/ 

select 
  round(100.0 * count(distinct case_id) filter (where call_category = 'n/a' or call_category is null) 
  / count(distinct case_id), 1) as uncategorised_call_pct
from callers


/* 15
Assume you're given a table containing information on Facebook user actions. Write a query to obtain number of monthly active users (MAUs) in July 2022, including the month in numerical format "1, 2, 3".

Hint: An active user is defined as a user who has performed actions such as 'sign-in', 'like', or 'comment' in both the current month and the previous month.
*/

SELECT 
  EXTRACT(MONTH FROM event_date) AS MONTH, 
  COUNT(DISTINCT user_id) AS monthly_active_users
FROM user_actions
WHERE 
  EXTRACT(MONTH FROM event_date) = 7
  AND user_id IN (SELECT user_id FROM user_actions WHERE EXTRACT(MONTH FROM event_date) = 6)
GROUP BY MONTH


/* 16
Assume you're given a table containing information about Wayfair user transactions for different products. Write a query to calculate the year-on-year growth rate for the total spend of each product, grouping the results by product ID.

The output should include the year in ascending order, product ID, current year's spend, previous year's spend and year-on-year growth percentage, rounded to 2 decimal places.
*/ 

with agg_spend as (
select 
  extract(year from transaction_date) as year,
  product_id,
  sum(spend) as spend
from user_transactions
group by extract(year from transaction_date), product_id
order by year, product_id)

, yoy_spend as(
select 
  year,
  product_id,
  spend as curr_year_spend,
  lag(spend, 1) over(PARTITION by product_id order by year) as prev_year_spend
from agg_spend)

select 
  *,
  round(100.0*(curr_year_spend-prev_year_spend) / prev_year_spend,2) as yoy_rate
from yoy_spend


/* 17
You're provided with two tables: the advertiser table contains information about advertisers and their respective payment status, and the daily_pay table contains the current payment information for advertisers, and it only includes advertisers who have made payments.

Write a query to update the payment status of Facebook advertisers based on the information in the daily_pay table. The output should include the user ID and their current payment status, sorted by the user id.

The payment status of advertisers can be classified into the following categories:

New: Advertisers who are newly registered and have made their first payment.
Existing: Advertisers who have made payments in the past and have recently made a current payment.
Churn: Advertisers who have made payments in the past but have not made any recent payment.
Resurrect: Advertisers who have not made a recent payment but may have made a previous payment and have made a payment again recently.
*/

select 
  COALESCE(a.user_id, p.user_id),
  --a.status as current_status,
  --paid,
  case 
    when paid is NULL then 'CHURN'
    when paid is not null and a.status is null then 'NEW'
    when (paid is not null and a.status IN ('CHURN')) then 'RESURRECT'
    else 'EXISTING' end as new_status
from advertiser as a
full join daily_pay as p on a.user_id = p.user_id
order by COALESCE(a.user_id, p.user_id)


/* 18
You’re a consultant for a major pizza chain that will be running a promotion where all 3-topping pizzas will be sold for a fixed price, and are trying to understand the costs involved.

Given a list of pizza toppings, consider all the possible 3-topping pizzas, and print out the total cost of those 3 toppings. Sort the results with the highest total cost on the top followed by pizza toppings in ascending order.

Break ties by listing the ingredients in alphabetical order, starting from the first ingredient, followed by the second and third.

P.S. Be careful with the spacing (or lack of) between each ingredient. Refer to our Example Output.

Notes:
Do not display pizzas where a topping is repeated. For example, ‘Pepperoni,Pepperoni,Onion Pizza’.
Ingredients must be listed in alphabetical order. For example, 'Chicken,Onions,Sausage'. 'Onion,Sausage,Chicken' is not acceptable.
*/ 

SELECT 
  concat(p1.topping_name, ',', p2.topping_name, ',', p3.topping_name) as pizza,
  p1.ingredient_cost + p2.ingredient_cost + p3.ingredient_cost as total_cost
FROM pizza_toppings AS p1
CROSS JOIN
  pizza_toppings AS p2,
  pizza_toppings AS p3
where p1.topping_name < p2.topping_name
  and p2.topping_name < p3.topping_name
order by total_cost desc, pizza


/* 19
You work as a data analyst for a FAANG company that tracks employee salaries over time. The company wants to understand how the average salary in each department compares to the company's overall average salary each month.

Write a query to compare the average salary of employees in each department to the company's average salary for March 2024. Return the comparison result as 'higher', 'lower', or 'same' for each department. Display the department ID, payment month (in MM-YYYY format), and the comparison result.
*/

with avg_salary as(
select 
  payment_date,
  avg(amount) as avg_salary
from salary as s
where payment_date = '03/31/2024'
group by payment_date)

, dept_avg_salary as(
select 
  e.department_id,
  s.payment_date,
  avg(s.amount) as dept_avg_salary
from salary as s
inner join employee as e
on s.employee_id = e.employee_id
where payment_date = '03/31/2024'
group by e.department_id, s.payment_date)

select 
  ds.department_id,
  TO_CHAR(ds.payment_date, 'MM-YYYY') as payment_date,
  case 
    when dept_avg_salary < avg_salary then 'lower'
    when dept_avg_salary = avg_salary then 'same'
    else 'higher' end as comparison
from dept_avg_salary as ds
inner join avg_salary as s
on ds.payment_date = s.payment_date


/* 20
Sometimes, payment transactions are repeated by accident; it could be due to user error, API failure or a retry error that causes a credit card to be charged twice.

Using the transactions table, identify any payments made at the same merchant with the same credit card for the same amount within 10 minutes of each other. Count such repeated payments.

Assumptions: The first transaction of such payments should not be counted as a repeated payment. This means, if there are two transactions performed by a merchant with the same credit card and for the same amount within 10 minutes, there will only be 1 repeated payment.
*/ 

with transactions as (
select 
  transaction_id,
  merchant_id,
  credit_card_id,
  transaction_timestamp,
  lag(transaction_timestamp) over(PARTITION by merchant_id, credit_card_id, amount order by transaction_timestamp) as prev_transaction
from transactions)

, transactions_diff as (
select
  *,
  round(extract(epoch from (transaction_timestamp - prev_transaction)) / 60, 1) as min_difference
from transactions)

select 
  count(DISTINCT transaction_id) as payment_count
from transactions_diff
where min_difference <= 10


/* 21
Amazon Web Services (AWS) is powered by fleets of servers. Senior management has requested data-driven solutions to optimize server usage.

Write a query that calculates the total time that the fleet of servers was running. The output should be in units of full days.

Assumptions: 
Each server might start and stop several times. 
The total time in which the server fleet is running can be calculated as the sum of each server's uptime.
*/ 
with server_times as(
select *,
  lag(status_time) over(partition by server_id order by status_time) as prev_status_time
from server_utilization
order by 1)

, server_times_diff as(
select 
  server_id,
  --session_status,
  prev_status_time as start_time,
  status_time as stop_time,
  extract(epoch from(status_time - prev_status_time)) / 86400 as diff_in_days
from server_times
where session_status = 'stop')

select 
  trunc(sum(diff_in_days)) as total_uptime_days
from server_times_diff



/* 22 
For each signup source, find top 3 users in 2023 by their total subscription revenue. 
If multiple users have the same total revenue, rank them by their signup_date. 

Tables: 

users
user_id | signup_date | signup_source | plan_type
―――――――――――――――――――――――――――――――――――――――――――――――――
1		| 2023-01-01  | referral      | basic

subscriptions
subscription_id | user_id | start_date | end_date   | monthly_fee
―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
101				| 1		  | 2023-01-15 | 2023-04-01 | 25.00

*/ 
--1st
WITH users_revenue AS(
SELECT *
	user_id,
	SUM(
		(MONTH(LEAST(COALESCE(end_date, '2100-01-01'::date), '2023-12-01'::DATE)) - 
		MONTH(GREATEST(start_date, '2023-01-01'::DATE)) + 1
		) * monthly_fee as revenue
from subscriptions 
group by user_id)
, ranked_users as(
select 
	r.user_id,
	u.signup_source,
	r.revenue,
	row_number() over(partition by u.signup_source order by r.revenue desc, signup_date) as user_rnk
from users_revenue as r 
inner join users as u on r.user_id = u.user_id)

select 
	signup_source,
	user_id
from ranked_users
where user_rnk <= 3


----2nd 
with subs as(
select 
	subscription_id,
	user_id,
	start_date,
	end_date,
	monthly_fee,
	case when start_date < '2023-01-01' and (end_date is NULL or end_date > '2023-01-01') then start_date = '2023-01-01'
		else start_date 
		end as start_date_new,
	case when end_date is NULL or end_date > '2023-12-01' then '2023-12-01'
		else end_date
		end as end_date_new
from subscriptions as s)
, revenues as(
select 
	subscription_id,
	user_id,
	start_date_new,
	end_date_new, 
	end_date_new - start_date_new as month_diff,
	monthly_fee * (end_date_new - start_date_new) as total_revenue
from subs
where extract(year from start_date_new = 2023)
	and extract(year from end_date_new = 2023))
, ranked_users as(
select 
	u.signup_source,
	u.signup_date,
	r.total_revenue,
	row_number(u.user_id) over(partition by u.signup_source order by r.total_revenue desc) as user_rnk 
from revenues as r
inner join users as u on r.user_id = u.user_id)

select 
	u.signup_source,
	r.total_revenue
from ranked_users
where user_rnk <= 3
order by r.total_revenue desc, signup_date


/* 23 
Calculate monthly retention rate for each plan type for each month in 2023. 
Retention rate is defined as % of customers who were active in previous month and are still active in the current month. 
Assume a customer is active if they have made at least one transaction in a month. 

Tables:
customers
customer_id | created_at | churn_date | plan_type
――――――――――――――――――――――――――――――――――――――――――――――――――
1 			| 2023-01-15 | NULL 	  | basic
2 			| 2023-02-10 | 2023-05-20 | premium

transactions
transaction_id | customer_id | amount | transaction_date
――――――――――――――――――――――――――――――――――――――――――――――――――――――――
1001		   | 1		     | 50.00  | 2023-02-15

*/ 

--First, join both tables and show only customer_id, plan_type, and month of each transaction
with transactions_month as(
select 
	customer_id,
	plan_type,
	extract(month from transaction_date) as transaction_month
from transactions as t 
inner join customers as c on t.customer_id=t.customer_id
where extract(year from transaction_date) = 2023)

-- Show previous activity month 
, previous_month as(
select 
	customer_id,
	plan_type,
	transaction_month as curr_transaction_month,
	lag(transaction_month) over(partition by customer_id, plan_type order by transaction_month) as prev_active_month
from transactions_month)

--show active customers for current and previous Mmonths by plan_type and transaction_month
, retention_per_month as(
select 
	plan_type,
	transaction_month,
	count(distinct customer_id) as active_customers,
	count(case when prev_active_month is not null 
		and (curr_transaction_month-prev_active_month=1) then customer_id end) as prev_active_customers
from previous_month
group by plan_type, transaction_month)

--calculate retention 
select 
	plan_type,
	transaction_month,
	case 
		when prev_active_customers<>0 then (active_customers/prev_active_customers)*100.0
		else 0
	end as retention_perc
from retention_per_month
order by plan_type, transaction_month


/* 24
Given a table containing information about bank deposits and withdrawals made using Paypal, 
write a query to retrieve the final account balance for each account, taking into account all the transactions 
recorded in the table with the assumption that there are no missing transactions.

transaction_id | account_id | amount | transaction_type
――――――――――――――――――――――――――――――――――――――――――――――――――
123 		   | 101        | 10.00  | Deposit
124	           | 101        | 20.00  | Withdrawal
*/ 

select 
  account_id,
  sum(case 
    when transaction_type = 'Withdrawal' then amount * (-1)
    else amount
  end) as final_balance
from transactions
group by account_id
order by account_id


/* 25
Company provides a range of tax filing products, including TurboTax and QuickBooks, available in various versions.
Write a query to determine the total number of tax filings made using TurboTax and QuickBooks. Each user can file taxes once a year using only one product.
*/ 

select 
  sum(case when product ilike 'TurboTax%' then 1 else 0 end) as turbotax_total,
  sum(case when product ilike 'QuickBooks%' then 1 else 0 end) as quickbooks_total
from filed_taxes


/* 26
Identify Subject Matter Experts (SMEs) based on their work experience in specific domains. 
An employee qualifies as an SME if they meet either of the following criteria:
- They have 8 or more years of work experience in a single domain.
- They have 12 or more years of work experience across two different domains.
Write a query to return the employee IDs of all the SMEs.
*/ 
select employee_id
from employee_expertise
group by employee_id
having (count(distinct domain) = 1 and sum(years_of_experience) >= 8)
  or (count(distinct domain) = 2 and sum(years_of_experience) >= 12)


/* 27
You observe that the category column in products table contains null values. 
Write a query that returns the updated product table with all the category values filled in, 
taking into consideration the assumption that the first product in each category will always have a defined category value.

Assumptions:
- Each category is expected to be listed only once in the column and products within 
the same category should be grouped together based on sequential product IDs.
- The first product in each category will always have a defined category value.
	- For instance, the category for product ID 1 is 'Shoes', then the subsequent product IDs 2 and 3 will be categorised as 'Shoes'.
	- Similarly, product ID 4 is 'Jeans', then the following product ID 5 is categorised as 'Jeans' category, and so forth.
	
product_id | category | name 
――――――――――――――――――――――――――――――
1 		   | Shoes    | Adidas
2	       | NULL     | Vans
3 		   | Jeans    | Levi
4	       | NULL     | Gloria Jeans

*/
--1st solution using FIRST_VALUE
with categories as(
select 
  *,
  count(category) over (order by product_id) as numbered_category --count of categories by each row 
from products)

select 
  product_id,
  first_value(category) over(partition by numbered_category) as category, 
  name
from categories

--2nd solution using COALESCE
WITH filled_category AS (
SELECT
  *,
  COUNT(category) OVER (ORDER BY product_id) AS numbered_category
FROM products
)

SELECT
  product_id,
  COALESCE(
    category, 
    MAX(category) OVER (PARTITION BY numbered_category)) AS category,
  name
FROM filled_category


/* 28 
Microsoft Azure's capacity planning team wants to understand how much data its customers are using, 
and how much spare capacity is left in each of its data centers. 
You’re given three tables: customers, data centers, and forecasted_demand.
Write a query to find each data centre’s total unused server capacity. Output the data center id in ascending order and the total spare capacity.

Definitions:
- monthly_capacity is the total monthly server capacity for each centers.
- monthly_demand is the server demand for each customer.
*/ 

--1st 
with servers_demand as(
select 
  datacenter_id,
  sum(monthly_demand) as monthly_demand
from forecasted_demand
group by datacenter_id)

select 
  dc.datacenter_id,
  dc.monthly_capacity - sd.monthly_demand as spare_capacity
from datacenters  as dc
inner join servers_demand as sd on dc.datacenter_id=sd.datacenter_id
order by dc.datacenter_id

--2nd 
SELECT 
  centers.datacenter_id, 
  centers.monthly_capacity - SUM(demands.monthly_demand) AS spare_capacity
FROM forecasted_demand AS demands
INNER JOIN datacenters AS centers
  ON demands.datacenter_id = centers.datacenter_id
GROUP BY centers.datacenter_id, centers.monthly_capacity
ORDER BY centers.datacenter_id


/* 29
Assume you are given the table below containing information on user reviews. Write a query to obtain the number and percentage of businesses that are top rated. 
A top-rated busines is defined as one whose reviews contain only 4 or 5 stars.
Output the number of businesses and percentage of top rated businesses rounded to the nearest integer.

Assumption:
Each business has only one review (which is the business' average rating).
*/

--1st
SELECT 
  COUNT(business_id) AS business_count,
  ROUND(100.0 * COUNT(business_id)/
    (SELECT COUNT (business_id) FROM reviews),0) AS top_rated_pct
FROM reviews
WHERE review_stars IN (4, 5);

--2nd
select 
  count(distinct business_id) filter(where review_stars IN (4,5)) as business_count,
  100 * count(distinct business_id) filter(where review_stars IN (4,5)) 
    / count(distinct business_id) as top_rated_pct
from reviews

--3rd
SELECT 
	sum(case when review_stars = 4 or review_stars=5 then 1 else 0 end) as business_count,
	(sum(case when review_stars = 4 or review_stars= 5 then 1 else 0 end) 
		/ count(*):: float) * 100 as top_rated_pct
FROM reviews


/* 30
Google marketing managers are analyzing the performance of various advertising accounts over the last month. 
They need your help to gather the relevant data.
Write a query to calculate the return on ad spend (ROAS) for each advertiser across all ad campaigns. 
Round your answer to 2 decimal places, and order your output by the advertiser_id.

Hint: ROAS = Ad Revenue / Ad Spend
*/

select 
  advertiser_id,
  round(((sum(revenue)/sum(spend))::decimal),2) as ROAS --PostgreSQL requires the input to the ROUND function to be a numeric(=decimal) data type >> need to convert roas from double precision to a decimal type before rounding
from ad_campaigns
group by advertiser_id
order by advertiser_id


/* 31
You're given two tables containing data on Spotify users' streaming activity: songs_history which has historical streaming data, 
and songs_weekly which has data from the current week.

Write a query that outputs the user ID, song ID, and cumulative count of song plays up to August 4th, 2022, sorted in descending order.

Assume that there may be new users or songs in the songs_weekly table that are not present in the songs_history table.

Definitions:
-song_weeklytable only contains data for the week of August 1st to August 7th, 2022.
-songs_history table contains data up to July 31st, 2022. The query should include historical data from this table.
*/ 

--1st
with songs_history_last_week as(
select 
  user_id,
  song_id,
  count(listen_time) as song_plays
from songs_weekly
where listen_time < '2022-08-05'
group by user_id, song_id)

select
  COALESCE(h.user_id, h_lw.user_id) as user_id,
  COALESCE(h.song_id, h_lw.song_id) as song_id,
  COALESCE(h.song_plays,0) + COALESCE(h_lw.song_plays,0) as song_plays 
from songs_history as h
full join songs_history_last_week as h_lw 
  on h.user_id=h_lw.user_id 
    and h.song_id=h_lw.song_id
order by song_plays desc

--2nd 
WITH history AS (
  SELECT 
    user_id, 
    song_id, 
    song_plays
  FROM songs_history

  UNION ALL

  SELECT 
    user_id, 
    song_id, 
    COUNT(song_id) AS song_plays
  FROM songs_weekly
  WHERE listen_time <= '08/04/2022 23:59:59'
  GROUP BY user_id, song_id
)

SELECT 
  user_id, 
  song_id, 
  SUM(song_plays) AS song_count
FROM history
GROUP BY 
  user_id, 
  song_id
ORDER BY song_count DESC


/* 32
Your team at Accenture is helping a Fortune 500 client revamp their compensation and benefits program. 
The first step in this analysis is to manually review employees who are potentially overpaid or underpaid.

An employee is considered to be potentially overpaid if they earn more than 2 times the average salary for people with the same title. 
Similarly, an employee might be underpaid if they earn less than half of the average for their title. 
We'll refer to employees who are both underpaid and overpaid as compensation outliers for the purposes of this problem.

Write a query that shows the following data for each compensation outlier: employee ID, salary, 
and whether they are potentially overpaid or potentially underpaid
*/ 

--1st
with avg_salaries as(
select 
  *,
  round(avg(salary) over(PARTITION by title),0) as avg_title_salary
from employee_pay)
, salaries_status as(
select 
  employee_id,
  salary,
  case when salary / avg_title_salary >= 2 then 'Overpaid'
    when salary / avg_title_salary <= 0.5 then 'Underpaid'
    when salary / avg_title_salary between 0.5 and 2 then NULL
  end as status
from avg_salaries)

select *
from salaries_status
where status is not null
order by employee_id

--2nd
WITH payout AS (
SELECT
  employee_id,
  salary,
  title,
  (AVG(salary) OVER (PARTITION BY title)) * 2 AS double_average,
  (AVG(salary) OVER (PARTITION BY title)) / 2 AS half_average
FROM employee_pay)

SELECT
  employee_id,
  salary,
  CASE WHEN salary > double_average THEN 'Overpaid'
    WHEN salary < half_average THEN 'Underpaid'
  END AS outlier_status
FROM payout
WHERE salary > double_average
  OR salary < half_average
  
  
 /* 33
 The Airbnb marketing analytics team is trying to understand what are the most common marketing channels 
 that lead users to book their first rental on Airbnb.

Write a query to find the top marketing channel and percentage of first rental bookings from the aforementioned marketing channel. 
Round the percentage to the closest integer. Assume there are no ties.

Assumptions:
-Marketing channel with null values should be incorporated in the percentage of first bookings calculation, but the top channel should not be a null value. Meaning, we cannot have null as the top marketing channel.
-To avoid integer division, multiple the percentage with 100.0 and not 100.
 */
 
 with bookings_rnk as (
select 
  b.booking_id,
  channel,
  row_number() over(partition by user_id order by booking_date) as order_number
from bookings as b
inner join booking_attribution as ba 
  on b.booking_id=ba.booking_id)
  
select 
  channel,
  round(100 * count(*) / sum(count(*)) over(),0) as first_booking_pct
from bookings_rnk
where order_number=1
group by channel
order by count(*) desc
limit 1


/* 34
As a data analyst at Uber, it's your job to report the latest metrics for specific groups of Uber users. 
Some riders create their Uber account the same day they book their first ride; the rider engagement team calls them "in-the-moment" users.

Uber wants to know the average delay between the day of user sign-up and the day of their 2nd ride. 
Write a query to pull the average 2nd ride delay for "in-the-moment" Uber users. Round the answer to 2-decimal places.
*/

--1st 
with in_the_moment_users as (
select 
  distinct u.user_id,
  r.ride_date
from users as u 
inner join rides as r 
  on u.user_id=r.user_id
  and u.registration_date=r.ride_date)
  
, rides_ranked as(
select 
  *,
  row_number() over(PARTITION by user_id order by ride_date) as ride_rank
from rides)

select 
  round(1.0*sum(s.ride_date::date - ft.ride_date::date) / count(*), 2) as average_delay
from rides_ranked as s
inner join in_the_moment_users as ft on s.user_id=ft.user_id
where ride_rank=2

--2nd
WITH CTE AS (
SELECT u.user_id
	,u.registration_date
    ,r.ride_date
    ,ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY r.ride_date) AS ride_number
FROM users u   
JOIN rides r ON u.user_id = r.user_id)

SELECT 
	ROUND(AVG(ride_date-registration_date),2) AS average_delay
FROM CTE 
WHERE user_id IN (SELECT user_id FROM CTE WHERE registration_date = ride_date)
     AND ride_number = 2
	 
/* 35
As a Data Analyst on the Google Maps User Generated Content team, you and your Product Manager are investigating u
ser-generated content (UGC) – photos and reviews that independent users upload to Google Maps.

Write a query to determine which type of place (place_category) attracts the most UGC tagged as 
"off-topic". In the case of a tie, show the output in ascending order of place_category.

Assumptions:
-Some places may not have any tags.
-Each UGC upload with the "off-topic" tag will be counted separately.
*/

--1st
with mentions as(
select 
  place_category,
  count(r.content_tag) as count_mentioned
from maps_ugc_review as r 
inner join place_info as i on r.place_id=i.place_id
where lower(r.content_tag) = 'off-topic'
group by place_category
order by count(r.content_tag) desc, place_category)

select
  place_category
from mentions
where count_mentioned = (select max(count_mentioned) from mentions)

--2nd 
WITH reviews AS (
  SELECT
    place_category,
    COUNT(ugc.content_id) AS content_count
  FROM place_info place
  INNER JOIN maps_ugc_review ugc
    ON place.place_id = ugc.place_id
  WHERE LOWER(content_tag) = 'off-topic'
  GROUP BY place_category
)
, top_place_category AS (
  SELECT
    place_category,
    content_count,
    RANK() OVER (
      ORDER BY content_count DESC) AS top_place
  FROM reviews
)

SELECT place_category AS off_topic_places
FROM top_place_category
WHERE top_place = 1


/* 36
As a Data Analyst on Snowflake's Marketing Analytics team, you're analyzing 
the CRM to determine what percent of marketing touches were of type "webinar" in April 2022. 
Round your percentage to the nearest integer.
*/ 

--1st 
select 
  round(100.0 * count(event_type) filter(where event_type = 'webinar') 
    / count(*), 0) as webinar_pct
from marketing_touches
where event_date between '2022-04-01' and '2022-04-30'

--2nd
SELECT 
  ROUND(100 *
    SUM(CASE WHEN event_type='webinar' THEN 1 ELSE 0 END)/
    COUNT(*)) as webinar_pct
FROM marketing_touches
WHERE DATE_TRUNC('month', event_date) = '04/01/2022'


/* 37
You're given a list of numbers representing the number of emails in the inbox of Microsoft Outlook users. 
Before the Product Management team can start developing features related to bulk-deleting email or achieving inbox zero, 
they simply want to find the mean, median, and mode for the emails.

Display the output of mean, median and mode (in this order), with the mean rounded to the nearest integer. 
It should be assumed that there are no ties for the mode.

user_id | email_count 
―――――――――――――――――――――
123 	| 100   
234 	| 200   
*/

select 
  round(avg(email_count)) as mean,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY email_count) AS median,
  mode() WITHIN GROUP (ORDER BY email_count) as mode
from inbox_stats


/* 38 [Transformations, pivot rows into columns]
Each user can designate a personal email address, a business email address, and a recovery email address.
The table is currently in the wrong format, so you need to transform its structure to show the 
following columns (see example output): user id, personal email, business email, and recovery email. 
Sort your answer by user id in ascending order.

user_id | email_type | email
――――――――――――――――――――――――――――
123 	| 100   	 | email@host.com
234 	| 200   	 | email001@host.com
*/ 

--1st
with personal_emails as(
select *
from users
where email_type ilike 'personal')
, business_emails as(
select *
from users
where email_type ilike 'business')
, recovery_emails as(
select *
from users
where email_type ilike 'recovery')

select 
  COALESCE(p.user_id, b.user_id, r.user_id),
  p.email as personal,
  b.email as business,
  r.email as recovery
from personal_emails as p 
full join business_emails as b on p.user_id=b.user_id
full join recovery_emails as r on p.user_id=r.user_id

--2nd, For this using aggregate data using MAX, because it ignores NULLs. 
--You need the max of each column grouped by the user_id so you group all of the entries for each user id together.
SELECT
  user_id,
  MAX(CASE WHEN email_type = 'personal' THEN email 
    ELSE NULL END) AS personal,
  MAX(CASE WHEN email_type = 'business' THEN email
    ELSE NULL END) AS business,
  MAX(CASE WHEN email_type = 'recovery' THEN email
    ELSE NULL END) AS recovery
FROM users
GROUP BY user_id
ORDER BY user_id

--3rd, Also with MAX
SELECT
  user_id,
  MAX (email) FILTER (WHERE email_type = 'personal') AS personal, 
  MAX (email) FILTER (WHERE email_type = 'business') AS business, 
  MAX (email) FILTER (WHERE email_type = 'recovery') AS recovery
FROM users
GROUP BY user_id
ORDER BY user_id


/* 39
For every customer that bought Photoshop, return a list of the customers, 
and the total spent on all the products except for Photoshop products.
Sort your answer by customer ids in ascending order.
*/ 

select 
  customer_id,
  sum(revenue) as revenue
from adobe_transactions
where customer_id IN (select distinct customer_id from adobe_transactions where product = 'Photoshop')
  and product != 'Photoshop'
group by customer_id
order by customer_id


/* 40
The Growth Team at DoorDash wants to ensure that new users, who make orders within their first 14 days on the 
platform, have a positive experience. However, they have noticed several issues with deliveries that result in a bad experience.

These issues include:
- Orders being completed incorrectly, with missing items or wrong orders.
- Orders not being received due to incorrect addresses or drop-off spots.
- Orders being delivered late, with the actual delivery time being 30 minutes later than the order placement time. 
  Note that the estimated_delivery_timestamp is automatically set to 30 minutes after the order_timestamp.

Write a query that calculates the bad experience rate for new users who signed up in June 2022 during their first 14 days on the platform. 
The output should include the percentage of bad experiences, rounded to 2 decimal places.

orders:
order_id | customer_id | trip_id | status | order_timestamp
―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
123 	 | 100   	   | 001     |...
234 	 | 200   	   | 011     |...

trips:
dasher_id | trip_id | estimated_delivery_timestamp | actual_delivery_timestamp
―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
... 	  | ...   	| ...                          |...

customers:
customer_id | signup_timestamp
――――――――――――――――――――――――――――――
123 	 	| 05/30/2022 00:00:00
*/


--1st
with users_june as(
select 
  distinct customer_id,
  signup_timestamp
from customers 
where signup_timestamp BETWEEN '2022-06-01' and '2022-06-30')
, late_trips as(
select 
  trip_id,
  round(extract(epoch from (actual_delivery_timestamp - estimated_delivery_timestamp)) / 60) as late_time
from trips)
, orders_within_14d as(
select 
  o.order_id,
  o.customer_id,
  o.trip_id,
  o.status
from orders as o
inner join users_june as u on o.customer_id=u.customer_id
where signup_timestamp::date + 14 >= order_timestamp::date)

select 
  round(100.0 * sum(case when (status IN ('completed incorrectly', 'never received') or late_time>0) then 1 else 0 end)
  / count(*), 2) as bad_experience_pct
from orders_within_14d as o
left join late_trips as t on o.trip_id = t.trip_id
where o.customer_id IN (select distinct customer_id from users_june)

--2nd 
WITH june22_cte AS (
SELECT 
  orders.order_id,
  orders.trip_id,
  orders.status
FROM customers
INNER JOIN orders
  ON customers.customer_id = orders.customer_id
WHERE EXTRACT(MONTH FROM customers.signup_timestamp) = 6
  AND EXTRACT(YEAR FROM customers.signup_timestamp) = 2022
  AND orders.order_timestamp BETWEEN customers.signup_timestamp 
    AND customers.signup_timestamp + INTERVAL '14 DAYS'
)

SELECT 
  ROUND(
    100.0 *
      COUNT(june22.order_id)
      / (SELECT COUNT(order_id) FROM june22_cte)
  ,2) AS bad_experience_pct
FROM june22_cte AS june22
INNER JOIN trips
  ON june22.trip_id = trips.trip_id
WHERE june22.status IN ('completed incorrectly', 'never received')

/* 41
You are given a table of PayPal payments showing the payer, the recipient, and the amount paid. 
A two-way unique relationship is established when two people send money back and forth. 
Write a query to find the number of two-way unique relationships in this data.

Assumption: 
- A payer can send money to the same recipient multiple times.

payments:
payer_id | recipient_id | amount 
――――――――――――――――――――――――――――――――――
101 	 | 201   	    | 30     
201 	 | 101   	    | 10    
*/ 

--1st, using ROW_NUMBER()
with duplicated_payments as (
select 
  p1.payer_id,
  p1.recipient_id,
  p2.payer_id,
  p2.recipient_id,
  row_number() over(PARTITION by p1.payer_id, p1.recipient_id, p2.payer_id, p2.recipient_id) as rnk
from payments as p1
inner join payments as p2 
  on p1.payer_id=p2.recipient_id
  and p1.recipient_id=p2.payer_id)
  
select
  count(*) / 2 as unique_relationships --dividing by 2 because earlier we got all pairs of 2-way transactions
from duplicated_payments
where rnk=1

--2nd, using INTERSECT operator
/* The INTERSECT operator combines two SELECT statements and returns only the distinct results that are common to 
both queries (meaning there are no duplicates). That is, if there are many back-and-forth transactions between two people, 
we'll only obtain two rows, as displayed above. If two people have a one-way transaction relationship, those will be 
eliminated because the payer never becomes the recipient in this situation.*/ 
WITH relationships AS (
SELECT payer_id, recipient_id
FROM payments
INTERSECT
SELECT recipient_id, payer_id
FROM payments)

SELECT COUNT(payer_id) / 2 AS unique_relationships
FROM relationships


/* 42 
In consulting, being "on the bench" means you have a gap between two client engagements. Google wants to know how many days 
of bench time each consultant had in 2021. Assume that each consultant is only staffed to one consulting engagement at a time.
Write a query to pull each employee ID and their total bench time in days during 2021.

Assumptions:
- All listed employees are current employees who were hired before 2021.
- The engagements in the consulting_engagements table are complete for the year 2022.

staffing:
employee_id | is_consultant | job_id 
――――――――――――――――――――――――――――――――――
111 	    | true   	    | 7898     
121 	    | false   	    | 2353    

consulting_engagements:
job_id | client_id | start_date          | end_date 			| contract_amount
―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
7898   | 20076     | 05/25/2021 00:00:00 | 06/30/2021 00:00:00	| 11290.00
2353   | 20045     | 06/01/2021 00:00:00 | 11/12/2021 00:00:00	| 33040.00
*/ 

with consultants as(
select *
from staffing
where is_consultant = 'true')

, working_days as (
select 
  c.employee_id,
  sum(end_date - start_date) as working_days,
  count(e.job_id) as inclusive_days --adding 1 day for each job, counting the number of job IDs for each employee is the same as adding an additional day for each job
from consulting_engagements as e
inner join consultants as c 
  on e.job_id=c.job_id
group by c.employee_id)

select 
  employee_id,
  365 - (working_days + inclusive_days) as bench_days
from working_days


/* 43 - Cumulative orders by product type

Assume you're given a table containing Amazon purchasing activity. 
Write a query to calculate the cumulative purchases for each product type, ordered chronologically.
The output should consist of the order date, product, and the cumulative sum of quantities purchased.
*/ 

select 
  order_date,
  product_type,
  sum(quantity) over(PARTITION by product_type order by order_date) as cum_purchased
from total_trans
order by order_date


/* 44 
Say you have access to all the transactions for a given merchant account. Write a query to print the cumulative balance 
of the merchant account at the end of each day, with the total balance reset back to zero at the end of the month. 
Output the transaction date and cumulative balance.

transactions:
transaction_id | type    | amount | transaction_date
―――――――――――――――――――――――――――――――――――――――――――――――――――――
19153 	       | deposit | 65.90  | 07/10/2022 10:00:00
*/ 

with daily_balance as(
select 
  DATE_TRUNC('day', transaction_date) as transaction_day,
  DATE_TRUNC('month', transaction_date) as transaction_month,
  sum(case when type = 'withdrawal' then -1 * amount
      else amount end) as balance
from transactions
group by DATE_TRUNC('day', transaction_date), 
         DATE_TRUNC('month', transaction_date)
order by transaction_day)

select 
  transaction_day as transaction_date,
  sum(balance) over(partition by transaction_month order by transaction_day) as balance
from daily_balance


/* 45
Assume you are given the table below containing information on user purchases. 
Write a query to obtain the number of users who purchased the same product on 
two or more different days. Output the number of unique users.

purchases:
user_id	integer
product_id	integer
quantity	integer
purchase_date	datetime
*/ 

--1st
with repeat_orders as(
select 
  user_id,
  product_id,
  count(distinct purchase_date::date) as count_days
from purchases
group by user_id, product_id 
having count(distinct purchase_date::date) >= 2)

select count(DISTINCT user_id) as repeat_purchasers
from repeat_orders

--2nd, with subquery
SELECT COUNT(DISTINCT users) AS repeated_purchasers
FROM (
  SELECT DISTINCT user_id AS users
  FROM purchases
  GROUP BY user_id, product_id
  HAVING COUNT(DISTINCT purchase_date::DATE) > 1
) AS repeat_purchases;

--3rd, with self-join
SELECT COUNT(DISTINCT p1.user_id) AS repeated_purchasers
FROM purchases AS p1
INNER JOIN purchases AS p2
  ON p1.product_id = p2.product_id
    AND p1.purchase_date::DATE <> p2.purchase_date::DATE
	
	
/* 46 Average vacant days 

The strategy team in Airbnb is trying to analyze the impact of Covid-19 during 2021. To do so, they need you 
to write a query that outputs the average vacant days across the AirBnbs in 2021. Some properties have gone out of business, 
so you should only analyze rentals that are currently active. Round the results to a whole number.

Assumptions:
- is_active field equals to 1 when the property is active, and 0 otherwise.
- In cases where the check-in or check-out date is in another year other than 2021, limit the calculation 
to the beginning or end of the year 2021 respectively.
- Listing can be active even if there are no bookings throughout the year. 

bookings Table:
Column Name	Type
listing_id	integer
checkin_date	date
checkout_date	date

listings Table:
Column Name	Type
listing_id	integer
is_active	integer
*/ 

--1st 
with bookings_2021 as(
select 
  l.listing_id, 
  365 - COALESCE( sum(
    case when checkout_date>'12/31/2021' then '2021-12-31' else checkout_date end - --checkout_date
    case when checkin_date<'01/01/2021' then '2021-01-01' else checkin_date end) -- checkin_date
  ,0) as booked_days 
from listings as l
left join bookings as b
  on l.listing_id=b.listing_id
where is_active=1
group by l.listing_id)

select round(avg(booked_days)) as avg_vacant_days
from bookings_2021

--2nd 
WITH final AS 
(
  SELECT l.listing_id, checkin_date, checkout_date,
         CASE
         WHEN EXTRACT(year FROM checkin_date) = 2021 AND EXTRACT(year FROM checkout_date) = 2021 THEN checkout_date - checkin_date
         WHEN EXTRACT(year FROM checkin_date) = 2020 AND EXTRACT(year FROM checkout_date) = 2021 THEN checkout_date - '01/01/2021' 
         WHEN EXTRACT(year FROM checkin_date) = 2021 AND EXTRACT(year FROM checkout_date) = 2022 THEN '12/31/2021' - checkin_date 
         ELSE 0 END AS occupied_days
  FROM listings AS l
  LEFT OUTER JOIN bookings AS b
  ON l.listing_id = b.listing_id 
  WHERE is_active = 1
)

SELECT ROUND(AVG(vacant_days)) AS avg_vacant_days
FROM
(
  SELECT (365 - SUM(occupied_days)) AS vacant_days
  FROM final 
  GROUP BY listing_id
) AS vacant_days_table


/* 47 
As the Sales Operations Analyst at Oracle, you have been tasked with assisting the VP of Sales in determining 
the final compensation earned by each salesperson for the year. The compensation structure includes a fixed base salary, 
a commission based on total deals, and potential accelerators for exceeding their quota.

Each salesperson earns a fixed base salary and a percentage of commission on their total deals. Also, if they beat their 
quota, any sales after that receive an accelerator, which is just a higher commission rate applied to their commissions after they hit the quota.

Write a query that calculates the total compensation earned by each salesperson. The output should include the employee ID 
and their corresponding total compensation, sorted in descending order. In the case of ties, the employee IDs should be sorted in ascending order.

Assumptions:
-A salesperson is considered to have hit their target (quota) if their total deals meet or exceed the assigned quota.
-If a salesperson does not meet the target, their compensation package consists of the fixed base salary and a commission based on the total deals.
-If a salesperson meets the target, their compensation package includes
--The fixed base salary,
--A commission on target (quota), and
--An additional commission, including any accelerator on the remaining balance of the total deals (total deals - quota). 
The accelerator represents a higher commission rate for sales made beyond the quota.

employee_contract Table:
Column Name	Type
employee_id	integer
base	integer
commission	double
quota	integer
accelerator	double

deals Table:
Column Name	Type
employee_id	integer
deal_size	integer
*/ 

--1st
with total_deals as(
select 
  employee_id,
  sum(deal_size) as total_deals
from deals
group by employee_id)
, full_salaries as(
select 
  e.employee_id,
  quota,
  total_deals,
  accelerator,
  commission,
  accelerator,
  base, 
  case 
    when total_deals >= quota then commission*quota
    else commission*total_deals end as commission_on_target,
  case 
    when total_deals > quota then commission*(total_deals-quota)*accelerator
    else 0 end as commission_on_excess_sales
from employee_contract as e
inner join total_deals as d
  on e.employee_id=d.employee_id)
  
select 
  employee_id,
  base + commission_on_target + commission_on_excess_sales as total_compensation
from full_salaries
order by total_compensation desc, employee_id

--2nd
SELECT 
  deals.employee_id,
  CASE 
    WHEN SUM(deals.deal_size) <= employee.quota 
      THEN employee.base + (employee.commission * SUM(deals.deal_size)) -- #1
    ELSE employee.base + (employee.commission * employee.quota) + 
      ((SUM(deals.deal_size) - employee.quota) * employee.commission * employee.accelerator) -- #2
  END AS total_compensation
FROM deals
INNER JOIN employee_contract AS employee
  ON deals.employee_id = employee.employee_id
GROUP BY deals.employee_id, employee.quota, employee.base, employee.commission, employee.accelerator
ORDER BY total_compensation DESC, deals.employee_id


/* 48
Assume you are given the table below containing the information on the searches attempted and the percentage of invalid 
searches by country. Write a query to obtain the percentage of invalid searches.

Output the country in ascending order, total searches and overall percentage of invalid searches rounded to 2 decimal places.

Notes:
-num_search = Number of searches attempted; invalid_result_pct = Percentage of invalid searches.
-In cases where countries have search attempts but do not have a percentage of invalid searches in 
invalid_result_pct, it should be excluded, and vice versa.
-To find the percentages, multiply by 100.0 and not 100 to avoid integer division.

search_category Table:
Column Name	Type
country	string
search_cat	string
num_search	integer
invalid_result_pct	decimal
*/ 

with invalid_searches as(
select 
  country,
  search_cat,
  num_search,
  num_search*round(invalid_result_pct/100, 4) as invalid_num_search
from search_category
where invalid_result_pct is not null 
order by country)

select 
  country,
  sum(num_search) as total_search,
  round(100.0 * sum(invalid_num_search) / sum(num_search),2) as invalid_searches_pct
from invalid_searches
group by country
having sum(num_search) is not null 
order by country

--2nd
SELECT 
  country,
  SUM(num_search) AS total_searches,
  ROUND(SUM(num_search * invalid_result_pct)/SUM(num_search),2) AS invalid_searches_pct
FROM search_category
WHERE invalid_result_pct IS NOT NULL
GROUP BY country
ORDER BY country


/* 
The LinkedIn Creator team is looking for power creators who use their personal profile as a company or influencer page. 
This means that if someone's Linkedin page has more followers than all the company they work for, we can safely assume 
that person is a Power Creator. Keep in mind that if a person works at multiple companies, we should take into account 
the company with the most followers.
Write a query to return the IDs of these LinkedIn power creators in ascending order.

Assumptions:
-A person can work at multiple companies.
-In the case of multiple companies, use the one with largest follower base.
*/

with ranked_companies as(
select 
  personal_profile_id,
  ec.company_id,
  c.followers,
  max(c.followers) over(PARTITION by personal_profile_id) as max_followers
from employee_company as ec 
left join company_pages as c 
  on ec.company_id=c.company_id)
  
select 
  distinct p.profile_id
from personal_profiles as p 
left join ranked_companies as c 
  on p.profile_id=c.personal_profile_id
where p.followers > c.max_followers
order by p.profile_id

--2nd 
SELECT 
	pp.profile_id 
FROM personal_profiles pp 
JOIN employee_company  ec 
	ON ec.personal_profile_id = pp.profile_id
JOIN company_pages cp 
	ON cp.company_id = ec.company_id
GROUP BY pp.profile_id,pp.followers
HAVING pp.followers > max(cp.followers)
ORDER BY  pp.profile_id

/* 
As a Data Analyst on the People Operations team at Accenture, you are tasked with understanding how many 
consultants are staffed to each client, and specifically how many consultants are exclusively staffed to a single client.

Write a query that displays the client name along with the total number of consultants attached to each client, 
and the number of consultants who are exclusively staffed to each client (consultants working exclusively for that client). 
Ensure the results are ordered alphabetically by client name.

employees Table:
Column Name	Type
employee_id	integer
engagement_id	integer

consulting_engagements Table:
Column Name	Type
engagement_id	integer
project_name	string
client_name	string
*/ 

with total_cons as(
select 
  client_name,
  count(distinct employee_id) as total_consultants
from employees as e 
left join consulting_engagements as c
  on e.engagement_id=c.engagement_id
group by client_name
order by client_name)

, unique_consultants as (
select 
  e.employee_id
from employees as e 
left join consulting_engagements as c
  on e.engagement_id=c.engagement_id 
group by e.employee_id
having count(distinct client_name)=1)

, unique_consultants_client as(
select 
  client_name,
  count(DISTINCT e.employee_id) as single_client_consultants
from employees as e 
left join consulting_engagements as c 
  on e.engagement_id=c.engagement_id
where e.employee_id IN (select * from unique_consultants)
group by client_name)

select 
  t.client_name,
  t.total_consultants,
  COALESCE(u.single_client_consultants,0) as single_client_consultants
from total_cons as t
full outer join unique_consultants_client as u
  on t.client_name=u.client_name
order by t.client_name

--2nd
WITH single_client_consultants AS (
  SELECT employees.employee_id
  FROM employees
  INNER JOIN consulting_engagements AS engagements
    ON employees.engagement_id = engagements.engagement_id
  GROUP BY employees.employee_id
  HAVING COUNT(DISTINCT engagements.client_name) = 1
)

SELECT 
  engagements.client_name, 
  COUNT(DISTINCT employees.employee_id) AS total_consultants,
  COUNT(DISTINCT single.employee_id) AS single_client_consultants
FROM employees
INNER JOIN consulting_engagements AS engagements 
  ON employees.engagement_id = engagements.engagement_id
LEFT JOIN single_client_consultants AS single
  ON employees.employee_id = single.employee_id
GROUP BY engagements.client_name
ORDER BY engagements.client_name



/*
Assuming Salesforce operates on a per user (per seat) pricing model, we have a table containing contracts data.

Write a query to calculate the average annual revenue per Salesforce customer in three market segments: SMB, Mid-Market, and Enterprise. Each customer is represented by a single contract. Format the output to match the structure shown in the Example Output section below.

Assumptions:

Yearly seat cost refers to the cost per seat.
Each customer is represented by one contract.
The market segments are categorized as:-
SMB (less than 100 employees)
Mid-Market (100 to 999 employees)
Enterprise (1000 employees or more)
The terms "average deal size" and "average revenue" refer to the same concept which is the average annual revenue generated per customer in each market segment.

contracts Table:
Column Name	Type
customer_id	integer
num_seats	integer
yearly_seat_cost	integer

customers Table:
Column Name	Type
customer_id	integer
name	varchar
employee_count	integer (0-100,000)
*/ 

with customer_segments as(
select 
  customer_id,
  case when employee_count<100 then 'SMB'
    when employee_count>=100 and employee_count<999 then 'Mid-Market'
    when employee_count>=1000 then 'Enterprise'
  end as market_segment
from customers) 

, revenue_per_segment as(
select 
  cs.market_segment,
  sum(num_seats*yearly_seat_cost) / COUNT(DISTINCT cs.customer_id) as revenue_per_deal
from contracts as c 
inner join customer_segments as cs 
on c.customer_id=cs.customer_id
group by cs.market_segment)

select 
  sum(case when market_segment='SMB' then revenue_per_deal end) as smb_avg_revenue,
  sum(case when market_segment='Mid-Market' then revenue_per_deal end) as mid_avg_revenue,
  sum(case when market_segment='Enterprise' then revenue_per_deal end) as enterprise_avg_revenue
from revenue_per_segment


/* 
The Bloomberg terminal is the go-to resource for financial professionals, offering convenient access to a wide array of financial datasets. As a Data Analyst at Bloomberg, you have access to historical data on stock performance for the FAANG stocks.

Your task is to analyze the inter-month change in percentage for each FAANG stock by month over the years. This involves calculating the percentage change in closing price from one month to the next using the following formula:

Inter-month change in percentage = (Current month's closing price - Previous month's closing price) / Previous month's closing price x 100

For each FAANG stock, display the ticker symbol, the last day of the month, closing price, and the inter-month value change in percentage rounded to two decimal places for each stock. Ensure that the results are sorted by ticker symbol and date in chronological order.

stock_prices Schema:
Column Name	Type	Description
date	datetime	The specified date (mm/dd/yyyy) of the stock data.
ticker	varchar	The stock ticker symbol (e.g., AAPL) for the corresponding company.
open	decimal	The opening price of the stock at the start of the trading day.
high	decimal	The highest price reached by the stock during the trading day.
low	decimal	The lowest price reached by the stock during the trading day.
close	decimal	The closing price of the stock at the end of the trading day.
*/ 

WITH intermonth_prices AS (
  SELECT
    ticker,
    date,
    close,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
  FROM stock_prices
)

SELECT 
  ticker,
  date,
  close,
  ROUND((close - prev_close)/prev_close*100,2) AS intermth_change_pct
FROM intermonth_prices
ORDER BY ticker, date;




/* Consecutive Filing Years

Intuit, a company known for its tax filing products like TurboTax and QuickBooks, offers multiple versions of these products.

Write a query that identifies the user IDs of individuals who have filed their taxes using any version of TurboTax for three or
 more consecutive years. Each user is allowed to file taxes once a year using a specific product. Display the output in the ascending order of user IDs.

filed_taxes Table:
Column Name	Type
filing_id	integer
user_id	varchar
filing_date	datetime
product	varchar
*/ 

with turbotax_filings_cte as (
select 
  filing_id, 
  user_id,
  date_trunc('year', filing_date) as filing_year,
  lag(date_trunc('year', filing_date)) over(PARTITION by user_id order by filing_date) as previous_year,
  lead(date_trunc('year', filing_date)) over(PARTITION by user_id order by filing_date) as following_year
from filed_taxes 
where product ilike '%TurboTax%')

select 
  user_id
from turbotax_filings_cte
where (previous_year = filing_year - interval '1 year')
  and (following_year = filing_year + interval '1 year')
group by user_id

/* 
As a Data Analyst on Snowflake's Marketing Analytics team, your objective is to analyze customer relationship 
management (CRM) data and identify contacts that satisfy two conditions:
1. Contacts who had a marketing touch for three or more consecutive weeks.
2. Contacts who had at least one marketing touch of the type 'trial_request'.
Marketing touches, also known as touch points, represent the interactions or points of contact between a brand and its customers.

Your goal is to generate a list of email addresses for these contacts.

marketing_touches Table:
Column Name	Type
event_id	integer
contact_id	integer
event_type	string ('webinar', 'conference_registration', 'trial_request')
event_date	date

crm_contacts Table:
Column Name	Type
contact_id	integer
email	string
*/

with trial_requests_customers as(
select 
  m.contact_id,
  c.email
from marketing_touches as m
inner join crm_contacts as c 
  on m.contact_id=m.contact_id
where m.event_type ilike 'trial_request')

, touch_weeks as(
select 
  contact_id,
  date_trunc('week', event_date) as event_week,
  lag(date_trunc('week', event_date)) over(PARTITION by contact_id order by event_date) as prev_week,
  lead(date_trunc('week', event_date)) over(PARTITION by contact_id order by event_date) as foll_week
from marketing_touches)

select 
  email
from touch_weeks as w
left join crm_contacts as c
  on w.contact_id=c.contact_id
where (prev_week=event_week - interval '7 days')
  and (foll_week=event_week + interval '7 days')
group by w.contact_id, email


/* 

UnitedHealth Group (UHG) has a program called Advocate4Me, which allows policy holders (or, members) to call 
an advocate and receive support for their health care needs – whether that's claims and benefits support, drug 
coverage, pre- and post-authorisation, medical records, emergency assistance, or member portal services.

Write a query to obtain the number of unique callers who made calls within a 7-day interval of their previous calls. 
If a caller made more than two calls within the 7-day period, count them only once.

callers Table:
Column Name	Type
policy_holder_id	integer
case_id	varchar
call_category	varchar
call_date	timestamp
call_duration_secs	integer
*/

--1st 
with diff_seconds as(
SELECT
  policy_holder_id,
  call_date,
  LAG(call_date) OVER (
  	PARTITION BY policy_holder_id ORDER BY call_date) AS previous_call,
  round(extract(epoch from call_date 
  - LAG(call_date) OVER (
  	PARTITION BY policy_holder_id ORDER BY call_date))
  /(24*60*60),2) AS time_diff_secs --1 day = 24 hours x 60 minutes x 60 seconds
FROM callers)

select 
  count(distinct(policy_holder_id)) as policy_holder_count
from diff_seconds
where time_diff_secs <= 7

--2nd using interval
WITH call_history AS (
  SELECT 
    policy_holder_id,
    call_date AS current_call,
    LEAD(call_date) OVER (
      PARTITION BY policy_holder_id ORDER BY call_date) AS next_call
  FROM callers
)

SELECT COUNT(DISTINCT policy_holder_id) AS policy_holder_count
FROM call_history
WHERE current_call + INTERVAL '168 hours' >= next_call

--3rd 
with cte as(
select 
  policy_holder_id,	
  case_id,	
  call_date,  
  count(policy_holder_id) over(partition by policy_holder_id order by call_date 
    RANGE INTERVAL '7 days' PRECEDING)
FROM callers)

select 
  count(distinct policy_holder_id) as policy_holder_count
from cte
where count = 2

/* Month over month growth 

UnitedHealth Group (UHG) has a program called Advocate4Me, which allows policy holders (or, members) to call an 
advocate and receive support for their health care needs – whether that's claims and benefits support, 
drug coverage, pre- and post-authorisation, medical records, emergency assistance, or member portal services.

To analyze the performance of the program, write a query to determine the month-over-month growth rate specifically 
for long-calls. A long-call is defined as any call lasting more than 5 minutes (300 seconds).

Output the year and month in numerical format and chronological order, along with the growth percentage rounded 
to 1 decimal place.

callers Table:
Column Name	Type
policy_holder_id	integer
case_id	varchar
call_category	varchar
call_date	timestamp
call_duration_secs	integer
*/

with long_calls_count as (
select 
  extract(year from call_date) as call_year,
  extract(month from call_date) as call_month,
  count(case_id) as calls_count
from callers
where call_duration_secs>300
group by 1,2
order by 1,2)

select 
  call_year as yr,
  call_month as mth, 
  --lag(calls_count) over() as prev_month_calls_count,
  round(
    100.0 * (calls_count - lag(calls_count) over()) 
    / lag(calls_count) over()
    ,1) as long_calls_growth_pct
from long_calls_count


/*
You are given the two tables containing information on Etsy’s user signups and purchases. Write a query to obtain the percentage of users 
who signed up and made a purchase within 7 days of signing up. The result should be rounded to the nearest 2 decimal places.

Assumptions:

Signups who didn't buy any products yet should also count as part of the percentage of users who signed up and made a purchase within 7 days of signing up
If the signup date is on 06/21/2022 and the purchase date on 06/26/2022, then the user makes up part of the percentage of users who signed up 
and purchased within the 7 days of signing up.

signups Table:
Column Name	Type
user_id	integer
signup_date	datetime

user_purchases Table:
Column Name	Type
user_id	integer
product_id	integer
purchase_amount	decimal
purchase_date	datetime
*/ 

with users as(
SELECT 
  COALESCE(p.user_id, s.user_id) as user_id,
  case when p.purchase_date::date is not null then p.purchase_date::date 
    else '2030-01-01' end as purchase_date,
  s.signup_date::date,
  (case when p.purchase_date::date is not null then p.purchase_date::date 
    else '2030-01-01' end) - s.signup_date::date as days_diff
FROM user_purchases as p
full outer join signups as s
on s.user_id=p.user_id)

select 
  round(100.0*count(distinct user_id) filter (where days_diff<7) 
    / count(distinct user_id), 2) as same_week_purchases_pct
from users 

/*
The Apple retention team needs your help to investigate buying patterns. Write a query to determine the percentage of 
buyers who bought AirPods directly after they bought iPhones. Round your answer to a percentage (i.e. 20 for 20%, 50 for 50) with no decimals.

Clarifications:
- The users were interested in buying iPhones and then AirPods, with no intermediate purchases in between.
- Users who buy iPhones and AirPods at the same time, with the iPhone logged first, can still be counted.

transactions Table:
Column Name	Type
transaction_id	integer
customer_id	integer
product_name	varchar
transaction_timestamp	datetime
*/

with transactions_ranked as (
select 
  *,
  lead(product_name) over(PARTITION by customer_id order by transaction_id) as next_purchase
from transactions)

select 
  round(100.0 * count(distinct customer_id) filter (
    where product_name = 'iPhone' and next_purchase = 'AirPods')
  / count(distinct customer_id), 0) as follow_up_percentage
from transactions_ranked

/*
Write a query to recommend a page to a user. A recommendation is based on a page liked by user friends. 
Assume you have two tables: a two-column table of users and their friends, and a two-column table of users and the pages they liked.

Assumptions:
- Only recommend the top page to the user, and do not recommend pages that were already liked by the user.
  Top page is defined as the page with the highest number of followers.
- Output the user id and page recommended. Order the result in ascending order by user id.

friendship Table:
Column Name	Type
id	integer
user_id	string
friend_id	string

page_following Table:
Column Name	Type
id	integer
user_id	string
page_id	string
*/

--establish 2way relationship between user and their friends by merging the table into itself 
WITH two_way_friendship AS (
SELECT 
	user_id, 
	friend_id 
FROM friendship
UNION 
SELECT 
	friend_id, 
	user_id 
FROM friendship)

, cte AS (
SELECT 
  f.user_id, 
  pf.page_id, 
  COUNT(*) AS page_rnk,
  DENSE_RANK() OVER(PARTITION BY f.user_id ORDER BY COUNT(*) DESC) AS rnk
FROM two_way_friendship as f
JOIN page_following AS pf ON f.friend_id = pf.user_id
WHERE (f.user_id, pf.page_id) NOT IN (
  SELECT DISTINCT user_id, page_id FROM page_following)
GROUP BY f.user_id, pf.page_id)

SELECT user_id,	page_id 
FROM cte 
WHERE rnk = 1

/*
Imagine you're provided with a table containing information about user logins on Facebook in 2022. Write a query that determines 
the number of reactivated users for a given month. Reactivated users are those who were inactive 
the previous month but logged in during the current month.

Output the month in numerical format along with the count of reactivated users.

Here's some important assumptions to consider:
- The user_logins table only contains data for the year 2022 and there are no missing dates within that period.
- For instance, if a user whose first login date is on 3 March 2022, we assume that they had previously logged in during 
the year 2021. Although the data for their previous logins is not present in the user_logins table, we consider these users as reactivated users.

user_logins Table:
Column Name	Type
user_id	integer
login_date	datetime
*/

--1st
with prev_logins as (
select 
  user_id,
  extract(month from login_date) as month_login,
  lag(extract(month from login_date)) over(
    PARTITION by user_id order by login_date) as prev_month_login
from user_logins 
order by user_id, login_date)

, reactivated_users as (
select 
  *,
  case when month_login-prev_month_login IN (0,1) then 0
    else 1 end as is_reactivated
from prev_logins)

select 
  month_login as mth,
  count(distinct user_id) as reactivated_users
from reactivated_users
where is_reactivated=1
group by month_login
order by mth

--2nd
SELECT 
  EXTRACT(MONTH FROM curr_month.login_date) AS mth, 
  COUNT(DISTINCT curr_month.user_id) AS reactivated_users
FROM user_logins AS curr_month 
WHERE NOT EXISTS (
  SELECT * 
  FROM user_logins AS last_month 
  WHERE curr_month.user_id = last_month.user_id 
    AND EXTRACT(MONTH FROM last_month.login_date) = 
      EXTRACT(MONTH FROM curr_month.login_date - '1 month' :: INTERVAL)
) 
GROUP BY EXTRACT(MONTH FROM curr_month.login_date)
ORDER BY mth


/*
Assume we have a table of Google employees with their corresponding managers.

A manager is an employee with a direct report. A senior manager is an employee who manages at least one manager, 
but none of their direct reports is senior managers themselves. Write a query to find the senior managers and their direct reports.

Output the senior manager's name and the count of their direct reports. The senior manager with 
the most direct reports should be the first result.

Assumption:
An employee can report to two senior managers.

employees Table:
Column Name	Type
emp_id	integer
manager_id	integer
manager_name	string
*/

SELECT 
  managers.manager_name,
  COUNT(DISTINCT managers.emp_id) AS direct_reportees
FROM employees -- Represent employees
JOIN employees AS managers -- Represent managers
  ON employees.manager_id = managers.emp_id
JOIN employees AS senior_managers -- Represent senior managers
  ON managers.manager_id = senior_managers.emp_id
GROUP BY managers.manager_name
ORDER BY direct_reportees DESC


/* ??
Assume you're given a table containing information about user sessions, including the start and end times of each session. 
Write a query to retrieve the user session(s) that occur concurrently with the other user sessions.

Output the session ID and the number of concurrent user sessions, sorted in descending order.

Assumptions:
- Concurrent sessions are defined as sessions that overlap with each other. For instance, if session 1 starts before session 2, 
  session 2's start time should fall either before or after session 1's end time.
- Sessions with identical start and end times should not be considered concurrent sessions.

sessions Table:
Column Name	Type
session_id	integer
start_time	datetime
end_time	datetime
*/ 

select 
  s1.session_id,
  count(s2.session_id) as concurrent_sessions
from sessions as s1
inner join sessions as s2 
  on s1.session_id != s2.session_id 
  and (s2.start_time between s1.start_time and s1.end_time --condition 1: If session 1 starts before session 2, session 2's start time should fall either before or after session 1's end time
  or s1.start_time between s2.start_time and s2.end_time) --condition 2: If session 2 starts before session 1, session 1's end time should be greater than session 2's start time
group by s1.session_id
order by count(s2.session_id) desc

/*
Facebook wants to recommend new friends to people who show interest in attending 2 or more of the same private events.

Sort your results in order of user_a_id and user_b_id (refer to the Example Output below).

Notes:
-A user interested in attending would have either 'going' or 'maybe' as their attendance status.
-Friend recommendations are unidirectional, meaning if user x and user y should be recommended to each other, 
the result table should have both user x recommended to user y and user y recommended to user x.
-The result should not contain duplicates (i.e., user y should not be recommended to user x multiple times).

friendship_status Table:
Column Name	Type
user_a_id	integer
user_b_id	integer
status	enum ('friends', 'not_friends')

event_rsvp Table:
Column Name	Type
user_id	integer
event_id	integer
event_type	enum ('public', 'private')
attendance_status	enum ('going', 'maybe', 'not_going')
event_date	date
*/

--1st 
--find users who are interested in attending private events and count # of users' pairs
with users_interested_in_same_events as( 
select 
  e1.event_id,
  e1.user_id as user_a_id,
  e2.user_id as user_b_id,
  count(e1.event_id) over(PARTITION by e1.user_id, e2.user_id) as events_interested_count
from event_rsvp as e1
inner join event_rsvp as e2 
  on e1.event_id=e2.event_id 
  and e1.user_id!=e2.user_id
where e1.attendance_status IN ('going', 'maybe')
  and e1.event_type = 'private'
order by e1.event_id)

--find pairs of potential friends who are interested in the same 2+ events
, recommended_by_events as( 
select 
  distinct user_a_id,
  user_b_id
from users_interested_in_same_events
where events_interested_count>=2
order by user_a_id)

--find pairs who are not friends and merge with potential friends table 
select 
  e.user_a_id,
  e.user_b_id
from friendship_status as f
left join recommended_by_events as e
  on f.user_a_id=e.user_a_id 
  and f.user_b_id=e.user_b_id
where status ilike 'not_friends'
  and e.user_a_id is not null 
order by e.user_a_id


--2nd 
WITH private_events AS (
SELECT user_id, event_id
FROM event_rsvp
WHERE attendance_status IN ('going', 'maybe')
  AND event_type = 'private'
)

SELECT 
  friends.user_a_id, 
  friends.user_b_id
FROM private_events AS events_1
INNER JOIN private_events AS events_2
  ON events_1.user_id != events_2.user_id
  AND events_1.event_id = events_2.event_id
INNER JOIN friendship_status AS friends
  ON events_1.user_id = friends.user_a_id
  AND events_2.user_id = friends.user_b_id
WHERE friends.status = 'not_friends'
GROUP BY friends.user_a_id, friends.user_b_id
HAVING COUNT(*) >= 2


/* 
The Airbnb Booking Recommendations team is trying to understand the "substitutability" of two rentals and whether one rental is a good substitute 
for another. They want you to write a query to find the unique combination of two Airbnb rentals with the same exact amenities offered.
Output the count of the unique combination of Airbnb rentals.

Assumptions:
-If property 1 has a kitchen and pool, and property 2 has a kitchen and pool too, it is a good substitute and represents a unique matching rental.
-If property 3 has a kitchen, pool and fireplace, and property 4 only has a pool and fireplace, then it is not a good substitute.

rental_amenities Table:
Column Name	Type
rental_id	integer
amenity	string
*/ 

--combine to string amentities for each rental_id 
with combines_amenities as(
select 
  rental_id,
  string_agg(amenity, ', ' order by amenity) as amenities
from rental_amenities
group by rental_id
order by rental_id)

select 
  count(*) as matching_airbnb
from combines_amenities as a1 
join combines_amenities as a2 
  on a1.amenities=a2.amenities 
where a1.rental_id > a2.rental_id --to exclude duplicate matches 


/* Weekly Churn Rates
Facebook is analyzing its user signup data for June 2022. Write a query to generate the churn rate by week in June 2022. Output the week number (1, 2, 3, 4, ...) 
and the corresponding churn rate rounded to 2 decimal places.
For example, week number 1 represents the dates from 30 May to 5 Jun, and week 2 is from 6 Jun to 12 Jun.

Assumptions:
-If the last_login date is within 28 days of the signup_date, the user can be considered churned.
-If the last_login is more than 28 days after the signup date, the user didn't churn.

users Table:
Column Name	Type
user_id	integer
signup_date	datetime
last_login	datetime
*/

with churned_users as(
select 
  user_id,
  case 
    when signup_date between '2022-05-30' and '2022-06-05' then 1
    when signup_date between '2022-06-06' and '2022-06-12' then 2
    when signup_date between '2022-06-13' and '2022-06-19' then 3
    else 4 
  end as signup_week,
  case 
    WHEN last_login - signup_date <= INTERVAL '28 days' THEN 1
    ELSE 0 
  END AS churned_users
from users
where signup_date between '2022-05-30' and '2022-06-26')

select 
  signup_week,
  --count(user_id) as total_users,
  --sum(churned_users) as churned_users,
  round(100.0*sum(churned_users)/count(user_id),2) as churn_rate
from churned_users
group by signup_week
order by signup_week



/*
As a trading analyst at Bloomberg, your task is to identify specific months when a majority of the FAANG stocks (Facebook, Amazon, Apple,
 Netflix, Google) experienced a gain in value compared to the previous month, while one stock lagged behind its peers by recording a
 decrease in value. This analysis involves comparing opening prices from the previous month.

In essence, you're seeking months where 5 out of 6 FAANG stocks demonstrated an increase in value with one stock experiencing a decline.

Write a query to display the month and year in 'Mon-YYYY' format along with the ticker symbol of the stock that underperformed 
relative to its peers, ordered by month and year (in 'Mon-YYYY' format).

stock_prices Schema:
Column Name	Type	Description
date	datetime	The specified date (mm/dd/yyyy) of the stock data.
ticker	varchar	The stock ticker symbol (e.g., AAPL) for the corresponding company.
open	decimal	The opening price of the stock at the start of the trading day.
high	decimal	The highest price reached by the stock during the trading day.
low	decimal	The lowest price reached by the stock during the trading day.
close	decimal	The closing price of the stock at the end of the trading day.
*/

--find prev month open price, and assign 1 to prices which increased compared to prev month 
with mom_stocks as(
SELECT
  date::date,
  ticker,
  open as curr_mnt_open,
  lag(open) over(partition by ticker order by date) as prev_mnt_open,
  case 
    when (open > lag(open) over(partition by ticker order by date)) then 1
    else 0
  end as increrased_mom
from stock_prices
order by date, ticker)
--find months where were 5 increased prices, and 1 not increased
,searched_months as(
select 
  date::date
from mom_stocks
group by date
having sum(increrased_mom)=5)
--inner join on both tables 
select 
  to_char(s.date, 'Mon-YYYY') as mth_yr,
  s.ticker as underperforming_stock
from mom_stocks as s
inner join searched_months as m on s.date=m.date
where increrased_mom=0
order by 1

/* frequently purchased pairs

Given the Walmart transaction and product tables, write a query to determine the count of unique product combinations that are purchased 
together in the same transaction, considering that there must be a minimum of two products in the transaction. Display the output in 
ascending order of the product combinations.

For instance, if there are two transactions where apples and bananas are bought together, and another transaction where bananas and 
soy milk are bought together, the total count of unique combinations would be 2.

Psst, you may or may not need to use the products table.

Effective April 17th, 2023, the problem statement, assumptions, and solution were modified to align with the question.

transactions Table:
Column Name	Type
transaction_id	integer
product_id	integer
user_id	integer
transaction_date	datetime

products Table:
Column Name	Type
product_id	integer
product_name	string
*/ 

--1st 
WITH array_table AS (
SELECT 
  transaction_id, 
  ARRAY_AGG(CAST(product_id AS TEXT) ORDER BY product_id) as combination
FROM transactions
GROUP BY transaction_id
)

SELECT DISTINCT combination
FROM array_table
WHERE ARRAY_LENGTH(combination, 1) > 1
ORDER BY combination

--2nd 
SELECT COUNT(*) AS combo_num
FROM transactions AS p1
INNER JOIN transactions AS p2
  ON p1.transaction_id = p2.transaction_id
  AND p1.product_id > p2.product_id
 
 
 /* Compressed Median 
You are trying to find the median number of items bought per order on Alibaba, rounded to 1 decimal point.
However, instead of doing analytics on all Alibaba orders, you have access to a summary table, which describes how many items 
were in an order, and the number of orders that had that many items.

items_per_order Table:
Column Name	Type
item_count	integer
order_occurrences	integer
 */
 
 --1st 
 with agg as(
select 
  *,
  sum(order_occurrences) over(order by item_count) as running_sum,
  sum(order_occurrences) over() / 2 as median_item_order
from items_per_order)

select 
  item_count as median
from agg 
where median_item_order<=running_sum 
order by item_count 
limit 1

--2nd 
WITH running_orders AS (
SELECT
  *,
  SUM(order_occurrences) OVER (
    ORDER BY item_count ASC) as running_sum,
  SUM(order_occurrences) OVER () AS total_sum
FROM items_per_order
)

SELECT ROUND(AVG(item_count)::DECIMAL,1) AS median
FROM running_orders
WHERE total_sum <= 2 * running_sum
  AND total_sum >= 2 * (running_sum - order_occurrences);
  
  
 
 
 /* Retention rate 
 */ 
with orders_monthly as(
 select 
	date_trunc('month', order_date) as order_month,
	customer_id, 
	count(order_id) as count_orders
 from orders
 group by 1, 2
 order by 1, 2)
 
, prev_month_orders as (
select 
	customer_id,
	order_month,
	case 
		when order_month - lag(order_month) over(partition by customer_id order by order_month) = INTERVAL '1 month' then 1 
		else 0 
		end as repeat_customer
from orders_monthly)

select 
	order_month,
	count(distinct customer_id) as total_customers,
	sum(repeat_customer) as repeat_customers,
	100.0 * sum(repeat_customer) / count(distinct customer_id) as retention_rate 
from prev_month_orders
group by order_month
order by order_month



/*  Find the customers who made transactions in consecutive months (2 months in a row) 

table: transactions
transaction_id, customer_id, amount, transaction_date 
*/

with cte_transactions_month as(
select 
	distinct customer_id,
	date_trunc('month' from transaction_date) as transaction_month
from transactions)

, prev_month_transactions as(
select 
	customer_id,
	transaction_month,
	lag(transaction_month) over(partition by customer_id order by transaction_month) as prev_month
from cte_transactions_month)

SELECT
    customer_id,
    transaction_month,
    prev_month
FROM prev_month_transactions
WHERE
    prev_month IS NOT NULL
    AND transaction_month = prev_month + INTERVAL '1 month' --or this: date_diff(transaction_month,prev_month)=1
	
	
/* Classic "top N + others" question 

Find top 9 teams by wins in 2024, and for 10th row for "all others" 

teams
team_id, name

matches 
match_id, winner_id, loser_id, season 

wins_2024
team_id, win_count
*/ 

-- 1st solution: with UNION ALL and OFFSET 
-- top 9 teams
select 
	t.team_name,
	w.win_count
from wins_2024 as w 
join teams as t on w.team_id = t.team_id 
order by win_count desc
limit 9 

union all 
-- "all others" teams
select 
	'All others' as team_name,
	sum(w.win_count) as win_count 
from (select w.win_count 
	  from wins_2024 w 
	  order by win_count desc 
	  offset 9) as w 


-- 2nd solution: with CTE, ROW_NUMBER() and UNION ALL 
with ranked_teams as(
select 
	t.team_name,
	wc.win_count,
	row_number() over(order by wc.win_count desc) as rnk
from win_counts wc 
join teams t on wc.team_id = t.team_id)

, top_9 as (
select 
	team_name,
	win_count 
from ranked_teams 
where rnk <= 9 )

, all_others as(
select
	team_name, 
	sum(win_count) as win_count 
from ranked_teams
where rnk > 9)

select * from top_9
union all 
select * from all_others 

/* 
A repeat customer is someone who makes 2+ purchases, but 2 or more on the same day doesn’t count. 
Find count of repeat customers. 

purchases
purchase_id, customer_id, date 
*/ 

with repeat_customers as(
select 
	customer_id
from purchases 
group by customer_id
having count(distinct date) >= 2)

select 
	count(*) as repeat_customers
from repeat_customers


/* 
Count customers whose every purchase was organic (on a last-touch attribution basis).

purchases
purchase_id, customer_id, purchase_time  

marketing_touches 
customer_id, touch_time, channel

To properly apply last-touch attribution, you need to:
- Look at each purchase and find the latest touch before that specific purchase.
- Classify whether that purchase was organic.
- Identify customers who had only organic purchases (i.e., no purchases attributed to non-organic channels).
*/ 

-- find all marketing touches that happened before each purchase 
with all_previous_touches as(
select 
	p.customer_id,
	p.purchase_id, 
	p.purchase_time,
	mt.channel, 
	row_number() over(partition by p.purchase_id order by mt.touch_time desc) as rn --get ranking for each purchase
from purchases p
left join marketing_touches mt --if not touch exists before purchase then left join still keeps the purchase with NULL channel 
	on p.customer_id = mt.customer_id
	and mt.touch_time < p.purchase_time) 
	
--find only last touches and attribute organic to null channels 
, last_touch as(
select 
	customer_id,
	purchase_id,
	purchase_time, 
	coalesce(channel, 'organic') as last_touch_channel -- if no touch then it's organic 
from all_previous_touches 
where rn = 1) 

--find not organic customers 
, non_organic_customers as(
select 
	distinct customer_id
from last_touch
where last_touch_channel <> 'organic')

--find only organic by removing not organic customers 
select 
	count(distinct customer_id) as organic_only_customers
from last_touch 
where customer_id NOT IN(select customer_id from non_organic_customers)


-- Classic types of sql problems 

/* Top 3 products by sales + "others"
Write a query that shows total sales per product, but only keep the top 3 product by total sales as-is. All other product
should be combined into a signle row with product name "Other". 

sales: 
product_name, amount 
*/ 

with ranked_sales as( 
	select 
		product_name, 
		sum(amount) as total_sales,
		rank() over(oder by sum(amount) desc) as rnk 
	from sales 
	group by product_type
)

select 
	product_name,
	total_sales 
from sales 
where rnk <= 3

union all 

select 
	'Other' as product_name, 
	sum(amount) as total_sales 
from sales 
where rnk > 3


/* Top 5 cities by population + "Others" 
Return a list showing the total population for the top 5 most popular citites, and group the remaining citites 
into a single row labeled "Other". 

table: city_population
city_name, population 
*/ 

with ranked_cities as (
	select 
		city_name, 
		population,
		row_number() over(order by population desc) as rnk 
	from city_population 
)

select 
	city_name,
	population 
from ranked_cities 
where rnk <= 5

union all 

select 
	'Other' as city_name, 
	sum(population) as population
from ranked_cities 
where rnk > 5

/* Daily running total of sales 
Write a query to calculate the cumulative (running) total of sales by date. Order the result by sale_date 

daily_sales 
sale_date, amount 
*/ 

select 
	sale_date, 
	amount,
	sum(amount) over(order by sale_date) as running_total
from daily_sales 
order by sale_date 

/* Cumulative spending per user 
For each user, calculate the running total of their spending ordered by purchase date. 
Return user_id, purchase_date, amount, and the running_total 

user_purchases 
user_id, purchase_date, amount 
*/

select 
	user_id,
	purchase_date,
	amount,
	sum(amount) over(partition by user_id order by purchase_date) as running_total 
from user_purchases 
order by user_id, purchase_date 

/* Rolling 7-day sales (sliding windows) 
For each date, calculate the total sales in the last 7 days, including the current day. 

daily_sales 
sale_date, amount
*/ 

select 
	sale_date,
	sum(amount) over(
		order by sale_date 
		rows between 6 preceding and current row) as rolling_7_day_sales 
from daily_sales
order by sale_date 

/* Running average spending per user 
For each user, compute their average spending over time, ordered by purchase_date

user_purchases
user_id, purchase_date, amount 
*/

select 
	user_id,
	purchase_date,
	amount,
	avg(amount) over(partition by user_id order by purchase_date) as avg_running
from user_purchases
order by user_id, purchase_date 

/* Percentile rank of sales by day 
For each day, show the percentile rank of its sales amount compared to all other days. 

daily_sales
sale_date, amount 
*/ 

select 
	sale_date,
	amount,
	percent_rank() over(order by amount) as percentile_rank  --percent_rank() does not take any arguments 
from daily_sales
order by sale_date, amount 

/* Difference from previous value (daily change) 
For each date, calculate the change in sales compared to the previous day. 

daily_sales
sale_date, amount 
*/ 

select 
	sale_date,
	amount, 
	amount - lag(amount) over(order by sale_date) as change_vs_previous_day 
from daily_sales 
order by sale_date 

-- type pf problem: Nth Highest / Lowest 

/* 3rd highest product sale 
Find the product_id and amount of the 3rd highest sale overall. If there are ties, return the 3rd distinct amount. 

sales 
product_id, sale_date, amount 
*/ 

with ranked_sales as(
	select 
		product_id,
		amount,
		rank() over(order by amount desc) as rnk --use of RANK() for distinct logic 
	from sales 
)
select 
	product_id, 
	amount
from ranked_sales 
where rnk = 3

/* 2nd lowest salary per department 
For each department, find the employee(s) who have the 2nd lowest distinct salary 

employee_salaries 
employee_id, department_id, salary
*/ 

with ranked_salaries as(
	select 
		department_id,
		employee_id,
		salary,
		rank() over(partition by department_id order by salary) as rnk --use of RANK() for distinct logic, so if 2 employees have the lowest salary their unique salary is ranked 2
	from employee_salaries 
)

select 
	department_id,
	employee_id
from ranked_salaries 
where rnk = 2

/* Nth most recent login
Find the 3rd most recent login time for each user 

user_logins 
user_id, login_time
*/ 

with ranked_logins as (
	select 
		user_id,
		login_time,
		row_number() over(partition by user_id order by login_time desc) as rnk 
	from user_logins 
)
select 
	user_id,
	login_time 
from ranked_logins 
where rnk = 3

-- Common type: Percent of total (ratio to total) 
-- CType: Calculating how each value compares to a grand/group total.

/* Product sales as % of total sales 
For each product, calculate what percentage of total sales it contributes. 

sales 
product_id, amount 
*/ 
select 
	product_id, 
	--amount, 
	--sum(amount) over() as total_sales ,
	100.0 * amount / (sum(amount) over()) as percentage_of_total_sales
from sales 
order by percentage_of_total_sales desc 


/* Department headcount share (!)
Compute what percentage of the company's total employees are in each department. 

employees
employee_id, department_id 
*/ 

WITH dept_counts AS (
    SELECT 
        department_id,
        COUNT(DISTINCT employee_id) AS dept_employee_count
    FROM employees
    GROUP BY department_id
),
total_count AS (
    SELECT COUNT(DISTINCT employee_id) AS total_employee_count
    FROM employees
)

SELECT 
    d.department_id,
    ROUND(100.0 * d.dept_employee_count / t.total_employee_count, 2) AS percentage_of_total_employees
FROM dept_counts d
CROSS JOIN total_count t --attaching total_count value to every department row, because it has only one column 


/* Country share of global population 
Show each country's percentage of total world population, rounded to 2 decimal points 

countries 
country_name, population 
*/ 

select 
	country_name, 
	round(100.0 * (population / (sum(population) over())),2) as percentage_of_total_population 
from countries 





/* Alfa Bank sample questions 

3 tables: 

t1: users
user_id, created_at, country, registration_platform, channel_by_trail

t2: users_actions
user_id, action_id 

t3: actions
action_id, action_type (view_offer, apply_loan,... ), success_type, seconds_from_last_action, created_at 
*/ 

--Problem 1: посчитать кол-во всех действий каждого пользователя и кол-во успешных

-- using FILTER 
select 
	ua.user_id,
	count(distinct ua.action_id) as actions_count,
	count(a.action_id) filter(where a.success_type = true) as successful_actions
from user_actions ua
left join actions a on ua.action_id = a.action_id 
group by ua.user_id 
order by ua.user_id 

--using CASE WHEN
select 
	ua.user_id,
	count(distinct ua.action_id) as actions_count,
	count(case when a.success_type = true then 1 end) as successful_actions
from user_actions ua
left join actions a on ua.action_id = a.action_id 
group by ua.user_id 
order by ua.user_id 

-- Problem 2: для каждого пользователя необходимо посчитать целое среднее время (в секундах) между действиями, 
-- если таких действий больше одного 

user_id, avg_time_in_seconds_between_actions (if count actions>1) 

select
	ua.user_id, 
	round(avg(a.seconds_from_last_action)) as avg_seconds_from_last_action
from users_actions ua 
join actions a on ua.action_id = a.action_id 
where a.seconds_from_last_action is not null --фильтруем только где значения не null
group by ua.user_id 
order by ua.user_id 

-- Problem 3 (!): найти пользователей, которые успешно подали заявку (apply_loan), 
-- но у них не было успешного просмотра оффера (view_offer) до этого

-- using NOT EXISTS 
SELECT DISTINCT ua.user_id 
FROM user_actions ua 
JOIN actions a ON ua.action_id = a.action_id 
WHERE a.action_type = 'apply_loan' 
  AND a.success_type = TRUE 
  AND NOT EXISTS ( --check that no earlier successful view_offers existed 
    SELECT 1
    FROM user_actions ua2
    JOIN actions a2 ON ua2.action_id = a2.action_id 
    WHERE ua2.user_id = ua.user_id 
      AND a2.action_type = 'view_offer'
      AND a2.success_type = TRUE 
      AND a2.created_at < a.created_at
)

-- using CTE 
WITH successful_applies AS (
    SELECT 
		ua.user_id, 
		a.created_at AS apply_time
    FROM user_actions ua
    JOIN actions a ON ua.action_id = a.action_id
    WHERE a.action_type = 'apply_loan' AND a.success_type = TRUE
),
successful_views AS (
    SELECT 
		ua.user_id, 
		a.created_at AS view_time
    FROM user_actions ua
    JOIN actions a ON ua.action_id = a.action_id
    WHERE a.action_type = 'view_offer' AND a.success_type = TRUE
)
SELECT 
	sa.user_id
FROM successful_applies sa
LEFT JOIN successful_views sv
  ON sa.user_id = sv.user_id 
  AND sv.view_time < sa.apply_time --only earlier views, if no earlier views exist this will return null on sv side 
WHERE sv.user_id IS NULL --no matching earlier views were found 

-- Problem 4 (!): для каждого канала привлечения (channel_by_trail) нужно посчитать кол-во пользователей, 
-- сделавших хотя бы одно успешное действие и среднее кол-во успешных действий на пользователя

with successful_actions_by_user as(
	select 
		ua.user_id,
		count(a.action_id) as success_actions_count 
	from users_actions ua 
	join actions a on ua.action_id = a.action_id 
	where a.success_type is true
	group by ua.user_id 
)

select 
	u.channel_by_trail,
	count(distinct sau.user_id) as users_with_success_count,
	round(avg(sau.success_actions_count),2) as avg_success_per_user 
from successful_actions_by_user sau
join users u on u.user_id = sau.user_id
group by u.channel_by_trail


-- Common type: Last Value / Previous Row
-- CType: LAG() / LEAD() / Accessing neighboring rows for comparisons

/* Problem 1: Previous order amount 

Table: orders (order_id, customer_id, order_date, amount)

Task:
For each order, show the previous order amount placed by the same customer (if any).
*/ 

select 
	order_id,
	amount,
	lag(amount) over(partition by customer_id order by order_date) as previous_amount
from orders 

/* Problem 2: Time Between Logins
Table: user_logins (user_id, login_time)

Task:
For each login, calculate the time difference (in days) between the current login and the previous login by the same user.
*/ 

select 
	user_id,
	login_time,
	login_time - lag(login_time) over(partition by user_id order by login_time) as diff_between_current_and_previous_login --if supports datetime substractions (like in postgrwesql)
	-- if not supports datetime substractions: 
	-- DATEDIFF(login_time, LAG(login_time) OVER(PARTITION BY user_id ORDER BY login_time)) AS days_since_last_login
from user_logins 


/* Problem 3: Price Change Compared to Next Product
Table: products (product_id, price)

Task:
For each product, show the price difference compared to the next product in ascending price order.
*/

select 
	product_id,
	price - lead(price) over(order by price asc) as price_diff_with_next_product
from products


-- Common type: Time Between Events
-- CType: DATEDIFF, LAG/LEAD, timestamp subtraction — typically using window functions to compute time elapsed 
-- between events like logins, purchases, or status changes.

/* Problem 1: Days Between Purchases
Table: purchases (user_id, purchase_date)

For each purchase, calculate the number of days since the previous purchase by the same user.
*/ 

select 
	user_id,
	purchase_date, 
	purchase_date - lag(purchase_date) over(partition by user_id order by purchase_date) as days_from_previous_purchase
	-- or: DATEDIFF(purchase_date, lag(purchase_date) over(partition by user_id order by purchase_date)) as days_from_previous_purchase
from purchases 

/* Problem 2: Time Since Last Status Change
Table: account_status_log (user_id, status, change_time)
For each status change, compute the duration in hours since the previous status change for that user.
*/

-- for PostgreSQL 
select 
	user_id,
	status,
	change_time,
	extract(epoch from (change_time - lag(change_time) over(partition by user_id order by change_time))) 
		/ 60 / 60  as prev_change_time 
from account_status_log

-- for SQL Server using DATEDIFF
SELECT 
    user_id,
    status,
    change_time,
    DATEDIFF(
        HOUR, 
        LAG(change_time) OVER(PARTITION BY user_id ORDER BY change_time), 
        change_time
    ) AS prev_change_time
FROM account_status_log;


/* Problem 3: Time Until Next Game Session
Table: game_sessions (player_id, session_start)
For each session, compute the gap in minutes between the current session start and the next session for the same player.
*/ 

-- for PostgreSQL 
select 
	player_id,
	session_start,
	extract(epoch from lead(session_start) over(partition by player_id order by session_start) - session_start) / 60 as gap 
from game_sessions 

-- for SQL Server
select 
	player_id,
	session_start,
	DATEDIFF(
		MINUTE, 
		session_start,
		lead(session_start) over(partition by player_id order by session_start)		
	) as gap
from game_sessions 


-- Common type: 7. Self Join / Find Pairs
-- CType: Self join or pairs matching, to compare rows within the same table — often for finding related pairs, events, or gaps.

/* (!) Problem 1: Users Who Refer Each Other
Table: user_referrals (user_id, referred_user_id)

Find all pairs of users who referred each other.
*/

select 
	u1.user_id as user_a,
	u1.user_id as user_b 
from user_referrals u1 
join user_referrals u2 
	on u1.user_id = u2.referred_user_id
	and u1.referred_user_id = u2.user_id 
where u1.user_id < u1.referred_user_id  -- to avoid duplicate (A,B) and (B,A) 


/* (!) Problem 2: Overlapping Events
Table: events (event_id, start_time, end_time)

Find all pairs of events that overlap in time. (i.e., their time ranges intersect)
*/

select 
	e1.event_id as event_1,
	e2.event_id as event_2
from events e1 
join events e2 --join the events table to itself to compare each event with every other event.
	on e1.event_id < e2.event_id -- to avoid self-pairs and duplicates 
	and e1.start_time <= e2.end_time -- ensures that event_1 starts before event_2 ends
	and e2.start_time <= e1.end_time -- ensures that event_2 starts before event_1 ends 


-- Common type: 8. Group-Wise Maximum / Latest per Group
-- CType: Pick the row with the maximum or most recent value per group (e.g., latest order per user).

/* Problem 1: Latest Order Per Customer
Table: orders (order_id, customer_id, order_date, total_amount)

Task:
Find the most recent order for each customer.
*/ 

with ranked_orders as(
	select 
		order_id,
		customer_id,
		order_date,
		amount,
		row_number() over(partition by customer_id order by order_date desc) as rnk 
	from orders
)

select order_id, customer_id
from ranked_orders
where rnk = 1

/* Problem 4: Highest Salary Per Department
Table: employees (employee_id, department_id, salary)

Return the employee with the highest salary in each department.
*/ 

with ranked_employees as(
	select *, 
		rank() over(partition by department_id order by salary desc) as rnk 
	from employees
)

select employee_id
from ranked_employees
where rnk = 1


-- Self join 

/* Q1. 
Find services (i.e., bus numbers and companies) that go directly from Craiglockhart to London Road, 
without requiring a change of service.

2 tables: 
stops(id, name)
route(num, company, pos, stop) — this tells us which services go to which stops, and in what position (order) on the route.
*/ 

SELECT 
   a.company,
   a.num,
   stops_a.name,
   stops_b.name
from route as a 
join route as b on a.num=b.num and a.company=b.company
join stops as stops_a on a.stop=stops_a.id
join stops as stops_b on b.stop=stops_b.id
where stops_a.name='Craiglockhart' 
   and stops_b.name='London Road'
order by a.num

/* Q2. 
Give list of services which connects stops 115 and 137 

2 tables: 
stops(id, name)
route(num, company, pos, stop) — this tells us which services go to which stops, and in what position (order) on the route.
*/ 

select distinct
   a.company,
   a.num
from route as a
join route as b on a.num=b.num and a.company=b.company
where a.stop=115 and b.stop=137

/* Q3. 
Give list of services which connects stops 'Craiglockhart' and 'Tollcross'
*/ 

select 
   a.company,
   a.num
from route as a 
join route as b on a.num=b.num and a.company=b.company
join stops as stopa on stopa.id=a.stop
join stops as stopb on stopb.id=b.stop
where stopa.name='Craiglockhart'
and stopb.name='Tollcross'

/* Q4 
Give a distinct list of the stops which may be reached from 'Craiglockhart' by taking one bus, including 'Craiglockhart' itself, 
offered by the LRT company. Include the company and bus no. of the relevant services.
*/ 

select distinct
   stopb.name,
   a.company,
   a.num
from route as a
join route as b on a.num=b.num and a.company=b.company
join stops as stopa on stopa.id=a.stop
join stops as stopb on stopb.id=b.stop
where a.company='LRT'
and stopa.name='Craiglockhart'

/* (!) Q5. 
Find two-bus routes that go from Craiglockhart to Lochend, where:
- The first bus takes you from Craiglockhart to a transfer stop.
- The second bus takes you from the same transfer stop to Lochend.

We need to return:
- First bus number and company
- Name of the transfer stop
- Second bus number and company
*/ 

SELECT DISTINCT
  r1.num AS num,
  r1.company AS company,
  s_transfer.name AS name,
  r2.num AS num,
  r2.company AS company
FROM route r1
JOIN route r1_dest ON r1.num = r1_dest.num AND r1.company = r1_dest.company
JOIN stops s1 ON r1.stop = s1.id
JOIN stops s_transfer ON r1_dest.stop = s_transfer.id

JOIN route r2 ON s_transfer.id = r2.stop
JOIN route r2_dest ON r2.num = r2_dest.num AND r2.company = r2_dest.company
JOIN stops s2_dest ON r2_dest.stop = s2_dest.id

WHERE s1.name = 'Craiglockhart'
  AND s2_dest.name = 'Lochend'
  AND r1_dest.stop = r2.stop