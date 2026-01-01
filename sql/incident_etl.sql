USE sql_etlproject;
SELECT COUNT(*) FROM incident_event_log_dataset;

SELECT COUNT(DISTINCT number) AS distinct_incidents
FROM incident_event_log_dataset;

-- find the last date when number was last updated
SELECT 
  number,
  MAX(STR_TO_DATE(sys_updated_at, '%d-%m-%Y %H:%i')) AS last_updated
FROM incident_event_log_dataset
GROUP BY number
ORDER BY number
LIMIT 10;

SELECT VERSION();

-- step 2 Find the latest timestamp per ticket, then pull the full row for that timestamp.
WITH latest AS (
  SELECT
    number,
    MAX(STR_TO_DATE(sys_updated_at, '%d-%m-%Y %H:%i')) AS last_updated
  FROM incident_event_log_dataset
  GROUP BY number
)
SELECT t.*
FROM incident_event_log_dataset as t
JOIN latest as l
  ON t.number = l.number
 AND STR_TO_DATE(t.sys_updated_at, '%d-%m-%Y %H:%i') = l.last_updated;
 
-- Using Windows Function
SELECT *
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (PARTITION BY number
	ORDER BY STR_TO_DATE(sys_updated_at, '%d-%m-%Y %H:%i') DESC
    ) AS rn
  FROM incident_event_log_dataset as t
) as ranked
WHERE rn = 1;

-- Step 3 now we look onto the load part so we create a  new table which has tickets with all other columns & their latest timestamp status
CREATE TABLE ticket_snapshot_raw AS
SELECT *
FROM (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY number
      ORDER BY STR_TO_DATE(sys_updated_at, '%d-%m-%Y %H:%i') DESC
    ) AS rn
  FROM incident_event_log_dataset t
) as ranked
WHERE rn = 1;


-- this is for quality checks
SELECT COUNT(*) AS raw_rows,
       COUNT(DISTINCT number) AS raw_distinct_tickets
FROM incident_event_log_dataset;

SELECT COUNT(*) AS snapshot_rows,
       COUNT(DISTINCT number) AS snapshot_distinct_tickets
FROM ticket_snapshot_raw;

SELECT opened_at, resolved_at, closed_at
FROM ticket_snapshot_raw
WHERE opened_at IS NOT NULL AND opened_at != ''
LIMIT 5;

-- Step 4 for each ticket_snapshot_raw we create opened_dt,resolved_dt, closed_dt etc
-- also create mins_to_resolve , minutes_to_close
-- below we just check if there are any negative & wrong values eg if date is opened but resolve date is before opening date
SELECT
  number,
  STR_TO_DATE(opened_at,  '%d-%m-%Y %H:%i') AS opened_dt,
  STR_TO_DATE(resolved_at,'%d-%m-%Y %H:%i') AS resolved_dt,
  STR_TO_DATE(closed_at,  '%d-%m-%Y %H:%i') AS closed_dt,
  TIMESTAMPDIFF(
    MINUTE,
    STR_TO_DATE(opened_at, '%d-%m-%Y %H:%i'),
    STR_TO_DATE(resolved_at, '%d-%m-%Y %H:%i')
  ) AS minutes_to_resolve,
  TIMESTAMPDIFF(
    MINUTE,
    STR_TO_DATE(opened_at, '%d-%m-%Y %H:%i'),
    STR_TO_DATE(closed_at, '%d-%m-%Y %H:%i')
  ) AS minutes_to_close
FROM ticket_snapshot_raw
WHERE opened_at IS NOT NULL AND opened_at <> ''
  AND resolved_at IS NOT NULL AND resolved_at <> ''
  AND closed_at IS NOT NULL AND closed_at <> ''
LIMIT 20;

CREATE TABLE ticket_snapshot_clean AS
SELECT
  *,
  CASE
    WHEN resolved_at IS NOT NULL
     AND resolved_at <> ''
	 AND resolved_at <> '?'
     AND STR_TO_DATE(resolved_at,'%d-%m-%Y %H:%i')
         >= STR_TO_DATE(opened_at,'%d-%m-%Y %H:%i')
    THEN TIMESTAMPDIFF(
           MINUTE,
           STR_TO_DATE(opened_at,'%d-%m-%Y %H:%i'),
           STR_TO_DATE(resolved_at,'%d-%m-%Y %H:%i')
         )
    ELSE NULL
  END AS minutes_to_resolve_clean,
  CASE
    WHEN closed_at IS NOT NULL
     AND closed_at <> ''
	 AND closed_at <> '?'
     AND STR_TO_DATE(closed_at,'%d-%m-%Y %H:%i')
         >= STR_TO_DATE(opened_at,'%d-%m-%Y %H:%i')
    THEN TIMESTAMPDIFF(
           MINUTE,
           STR_TO_DATE(opened_at,'%d-%m-%Y %H:%i'),
           STR_TO_DATE(closed_at,'%d-%m-%Y %H:%i')
         )
    ELSE NULL
  END AS minutes_to_close_clean
FROM ticket_snapshot_raw;

SELECT COUNT(*) FROM ticket_snapshot_clean;

-- check the tables so you understand what you have done
SELECT * FROM ticket_snapshot_clean
LIMIT 100;

-- First step of Analysis KPI - Average resolution time by priority
SELECT
  priority,
  COUNT(*) AS ticket_count,
  AVG(minutes_to_resolve_clean) AS avg_minutes_to_resolve
FROM ticket_snapshot_clean
WHERE minutes_to_resolve_clean IS NOT NULL
GROUP BY priority
ORDER BY priority;

-- This KPI alone suggests potential issues critical ticket takes longer than high to resolve, such as:
-- Critical tickets are more complex, not just urgent
-- Critical tickets may require:
 -- more teams
-- approvals
-- vendor involvement

-- Second KPI - Resolution time by Assignment Group
SELECT
  assignment_group,
  COUNT(*) AS ticket_count,
  AVG(minutes_to_resolve_clean) AS avg_minutes_to_resolve
FROM ticket_snapshot_clean
WHERE minutes_to_resolve_clean IS NOT NULL
  AND assignment_group <> '?'
GROUP BY assignment_group
HAVING COUNT(*) >= 50
ORDER BY avg_minutes_to_resolve DESC;
 
 -- Query for little more depth  to know which group takes how my avg time for queries to solve based on priority
SELECT
  assignment_group,
  priority,
  COUNT(*) AS ticket_count,
  AVG(minutes_to_resolve_clean) AS avg_minutes_to_resolve
FROM ticket_snapshot_clean
WHERE minutes_to_resolve_clean IS NOT NULL
  AND assignment_group <> '?'
GROUP BY assignment_group, priority
HAVING COUNT(*) >= 50
ORDER BY
  CAST(SUBSTRING_INDEX(assignment_group, ' ', -1) AS UNSIGNED),
  CAST(SUBSTRING_INDEX(priority, ' ', 1) AS UNSIGNED);
  
  -- Next KPI: How many tickets are taking too long to resolve
  -- It calculates, by priority, how many tickets breached the defined SLA thresholds 
  -- and what percentage of tickets breached SLA.
  
SELECT
  priority,
  COUNT(*) AS total_tickets,
  SUM(sla_breached) AS breached_tickets,
  ROUND(100 * SUM(sla_breached) / COUNT(*), 2) AS breach_percentage
FROM (
  SELECT
    priority,
    CASE
      WHEN priority LIKE '1%' AND minutes_to_resolve_clean > 14400 THEN 1
      WHEN priority LIKE '2%' AND minutes_to_resolve_clean > 28800 THEN 1
      WHEN priority LIKE '3%' AND minutes_to_resolve_clean > 43200 THEN 1
      WHEN priority LIKE '4%' AND minutes_to_resolve_clean > 57600 THEN 1
      ELSE 0
    END AS sla_breached
  FROM ticket_snapshot_clean
  WHERE minutes_to_resolve_clean IS NOT NULL
) as sla_flagged_alias_table
GROUP BY priority
ORDER BY CAST(SUBSTRING_INDEX(priority, ' ', 1) AS UNSIGNED);

-- View result table & export it to csv for Tableau Visualization
SELECT * 
FROM ticket_snapshot_clean LIMIT 100000;

