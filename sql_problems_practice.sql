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

