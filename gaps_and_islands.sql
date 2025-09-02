-- Gaps & islands 


-- Type 1. Contiguous sequences of identical values (e.g., consecutive wins).
-- Trick: group by a running counter that changes only when value changes.

/* Example 1. Find consecitve wins

matches
+-------------+------+
| Column Name | Type |
+-------------+------+
| player_id   | int  |
| match_day   | date |
| result      | enum |
+-------------+------+
*/

with grouped_wins as (
select 
    player_id,
    match_day,
    result,
    sum(case when result != 'Win' then 1 else 0 end) 
        over(partition by player_id order by match_day) as streak_group
from matches)
-- each (player_id, streak_group) represents one island of wins (streak) 
select
    player_id,
    streak_group,
    count(*) as streak_len
from grouped_wins
where result = 'Win'
group by 1, 2


-- Type 2. Consecutive ranges of numbers/dates (e.g., find continuous days without gaps).
-- Trick: compare row number with the actual value (e.g., date - row_number() is constant within an island).

/* Example 1. Find consecutive days 

| player\_id | match\_day | result |
| ---------- | ---------- | ------ |
| 1          | 2025-01-01 | Win    |
| 1          | 2025-01-02 | Win    |
| 1          | 2025-01-03 | Lose   |
| 1          | 2025-01-05 | Win    |
| 1          | 2025-01-06 | Win    |
| 1          | 2025-01-08 | Win    |

Find continuous blocks of days when the player played, even if the result wasn’t always a win.

Output:
| player\_id | start\_day | end\_day   | length |
| ---------- | ---------- | ---------- | ------ |
| 1          | 2025-01-01 | 2025-01-03 | 3      |
| 1          | 2025-01-05 | 2025-01-06 | 2      |
| 1          | 2025-01-08 | 2025-01-08 | 1      |

*/

with cte as (
select 
    player_id, 
    match_day, 
    row_number() over(partition by player_id order by match_day) as rn 
from matches
)
, islands as (
select 
    player_id, 
    match_day, 
    rn,
    (match_day - rn * INTERVAL '1 day')::date as group_streak
from cte) 

select
    player_id,
    min(match_day) as start_day,
    max(match_day) as end_day,
    count(*) as streak_days
from islands  
group by player_id, group_streak
order by player_id, start_day




-- LEETCODE 

/* 1225. Report contiguos dates

Table: Failed

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| fail_date    | date    |
+--------------+---------+
fail_date is the primary key (column with unique values) for this table.
This table contains the days of failed tasks.
 

Table: Succeeded

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| success_date | date    |
+--------------+---------+
success_date is the primary key (column with unique values) for this table.
This table contains the days of succeeded tasks.
 

A system is running one task every day. Every task is independent of the previous tasks. The tasks can fail or succeed.

Write a solution to report the period_state for each continuous interval of days in the period from 2019-01-01 to 2019-12-31.

period_state is 'failed' if tasks in this interval failed or 'succeeded' if tasks in this interval succeeded. Interval of days are retrieved as start_date and end_date.

Return the result table ordered by start_date.

The result format is in the following example.
| period_state | start_date | end_date   |
| ------------ | ---------- | ---------- |
| succeeded    | 2019-01-01 | 2019-01-03 |
| failed       | 2019-01-04 | 2019-01-05 |
| succeeded    | 2019-01-06 | 2019-01-06 |
*/ 


WITH combined AS (
    SELECT fail_date AS task_date, 'failed' AS period_state
    FROM failed
    WHERE fail_date >= '2019-01-01' AND fail_date <= '2019-12-31'
    UNION ALL
    SELECT success_date AS task_date, 'succeeded' AS period_state
    FROM succeeded
    WHERE success_date >= '2019-01-01' AND success_date <= '2019-12-31'
),
ranked AS (
    SELECT 
        task_date,
        period_state,
        ROW_NUMBER() OVER (ORDER BY task_date) AS rn_global,
        ROW_NUMBER() OVER (PARTITION BY period_state ORDER BY task_date) AS rn_state
    FROM combined
)
SELECT 
    period_state,
    MIN(task_date) AS start_date,
    MAX(task_date) AS end_date
FROM ranked
GROUP BY period_state, rn_global - rn_state
ORDER BY start_date