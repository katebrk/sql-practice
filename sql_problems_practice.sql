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

