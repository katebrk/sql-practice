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