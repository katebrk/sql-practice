/* SaaS User Engagement and Revenue Analysis

The company wants a better understanding of:
- How users engage with the product over time
- Which user behaviors correlate with retention and expansion revenue
- Monthly recurring revenue (MRR) broken down by cohorts and plans

Tables:

users:
user_id	STRING
created_at	TIMESTAMP
email	STRING
company_id	STRING
is_active	BOOLEAN

companies:
company_id	STRING
company_name	STRING
industry	STRING
plan	STRING
is_churned	BOOLEAN
created_at	TIMESTAMP

events:
event_id	STRING
user_id	STRING
event_type	STRING
occurred_at	TIMESTAMP

subscriptions:
company_id	STRING
month	DATE (YYYY-MM)
mrr	NUMERIC(10, 2)
is_new	BOOLEAN
is_expansion	BOOLEAN
is_churn	BOOLEAN

Task: 
Build a SQL data model or set of queries to answer the following business questions
*/ 

/* PART 1: Behavioral Cohorts
Create monthly user cohorts based on the users.created_at. 
For each cohort, calculate retention by counting how many users from the cohort were active (had any event) in each subsequent month after signup.

Output schema: cohort_month, activity_month, users_active
*/ 

-- assign each user to their cohort month 
with user_cohorts as (
select 
	user_id,
	DATE_TRUNC(month, created_at) as cohort_month
	-- or: TO-CHAR(created_at, 'YYYY-MM') 
from users
), 
-- get distinct months when each user was active 
user_activity as (
select 
	user_id,
	DATE_TRUNC(month, occured_at) as activity_month
from events as e 
)
-- join both, filter to only include activity after signup, and count active users per cohort per activity month
select 
	uc.cohort_month,
	ua.activity_month, 
	count(distinct uc.user_id) as users_active 
from user_cohorts as uc 
join user_activity as ua on uc.user_id=ua.user_id 
where ua.activity_month >= uc.cohort_month 
group by uc.cohort_month, ua.activity_month
order by uc.cohort_month, ua.activity_month


/* PART 2: MRR Movement Report
Create a table showing monthly MRR movements grouped by type (new, expansion, churn, and existing).

Output schema: month, new_mrr, expansion_mrr, churned_mrr, existing_mrr

Existing MRR = Total MRR – New – Expansion + Churn
*/

with classification_mrr as (
select
	month,
	case when is_new then mrr else 0 end as new_mrr,
	case when is_expansion then mrr else 0 end as expansion_mrr,
	case when is_churn then mrr else 0 end as churned_mrr,
	mrr 
from subscriptions 
),
monthly_aggregates as (
select 
	month,
	sum(new_mrr) as new_mrr,
	sum(expansion_mrr) as expansion_mrr,
	sum(churned_mrr) as churned_mrr,
	sum(mrr) as total_mrr 
from classification_mrr
group by month 
)
select 
	month, 
	new_mrr,
	expansion_mrr,
	churned_mrr,
	(total_mrr - new_mrr - expansion_mrr + churned_mrr) as existing_mrr
from monthly_aggregates
order by month 

/* PART 3: Power User Analysis

Definition:
A "Power User" is a user who, within a single calendar month, satisfies all of the following:
- Logs in at least 15 times (event_type = 'login')
- Creates at least 10 docs (event_type = 'create_doc')
- Invites at least 2 users (event_type = 'invite_user')

For each month, output:
- The user_id
- Their company_id
- Their plan
- The month in which they qualified as a power user

Output schema:
month        user_id   company_id   plan
YYYY-MM      U123      C001         Pro


events:
event_id	STRING
user_id	STRING
event_type	STRING
occurred_at	TIMESTAMP
*/

-- all in different CTEs
with login_users as (
select 
	user_id, 
	date_trunc(month, occured_at) as month, 
	count(distinct event_id) as login_count 
from events 
where event_type = 'login'
group by 1, 2
having count(distinct event_id) >= 15
),
create_doc_users as (
select 
	user_id, 
	date_trunc(month, occured_at) as month, 
	count(distinct event_id) as login_count 
from events 
where event_type = 'create_doc'
group by 1, 2
having count(distinct event_id) >= 10
),
invite_user_users as (
select 
	user_id, 
	date_trunc(month, occured_at) as month, 
	count(distinct event_id) as login_count 
from events 
where event_type = 'invite_user'
group by 1, 2
having count(distinct event_id) >= 2
)
select 
	l.user_id,
	l.month,
	c.company_id,
	c.plan 
from login_users as l 
inner join create_doc_users cd on cd.user_id=l.user_id and cd.month=l.month 
inner join invite_user_users iu on iu.user_id=l.user_id and iu.month=l.month 
left join companies as c on c.company_id=l.company_id 

-- all in one CTE 
with monthly_activity as (
select 
	user_id,
	date_trunc(month, occured_at) as month, 
	count(distinct case when event_type='login' then event_id end) as login_count,
	count(distinct case when event_type='create_doc' then event_id end) as doc_count,
	count(distinct case when event_type='invite_user' then event_id end) as invite_count,
from events 
where event_type IN ('login', 'create_doc', 'invite_user') 
group by user_id, date_trunc(month, occured_at)
),

power_users as (
select *
from monthly_activity
where login_count>=15 and doc_count>=10 and invite_count>=2
)

SELECT
	pu.month,
  pu.user_id,
  u.company_id,
  c.plan
FROM power_users pu
LEFT JOIN users u ON pu.user_id = u.user_id
LEFT JOIN companies c ON u.company_id = c.company_id
ORDER BY pu.month, pu.user_id

