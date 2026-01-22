-- Daily active users (DAU)
SELECT
  event_date,
  COUNT(DISTINCT user_id) AS dau
FROM fact_events
WHERE event = 'page_view'
GROUP BY event_date
ORDER BY event_date;

-- Revenue by day
SELECT
  event_date,
  ROUND(SUM(amount), 2) AS revenue
FROM fact_events
WHERE event = 'purchase'
GROUP BY event_date
ORDER BY event_date;