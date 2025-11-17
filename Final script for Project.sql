with monthly_revenue AS (
  SELECT
    DATE(DATE_TRUNC('month', payment_date)) AS payment_month,
    user_id,
    SUM(revenue_amount_usd) AS total_revenue
  FROM project.games_payments gp
  GROUP BY 1, 2
),
revenue_lag_lead_months AS (
  SELECT
    mr.*,
    DATE(mr.payment_month - INTERVAL '1' MONTH) AS previous_calendar_month,
    DATE(mr.payment_month + INTERVAL '1' MONTH) AS next_calendar_month,
    LAG(mr.total_revenue) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS previous_paid_month_revenue,
    LAG(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS previous_paid_month,
    LEAD(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS next_paid_month
  FROM monthly_revenue mr
),
revenue_metrics AS (
  SELECT
    payment_month,
    user_id,
    total_revenue,
 CASE WHEN previous_paid_month IS NULL THEN total_revenue END AS new_mrr,
CASE WHEN previous_paid_month = previous_calendar_month
                     AND total_revenue > previous_paid_month_revenue
                   THEN total_revenue - previous_paid_month_revenue
               END AS expansion_revenue,
  CASE WHEN previous_paid_month = previous_calendar_month
                     AND total_revenue < previous_paid_month_revenue
                   THEN total_revenue - previous_paid_month_revenue
               END AS contraction_revenue,
  CASE WHEN previous_paid_month IS NOT NULL
                     AND previous_paid_month != previous_calendar_month
                   THEN total_revenue
               END AS back_from_churn_revenue,
  CASE WHEN next_paid_month IS NULL
                     OR next_paid_month != next_calendar_month
                   THEN total_revenue
               END AS churned_revenue,
  CASE WHEN next_paid_month IS NULL
                OR next_paid_month != next_calendar_month
             THEN next_calendar_month
        END AS churn_month
  
  FROM revenue_lag_lead_months
)
SELECT
  rm.*,
  gpu.game_name,
  gpu.language,
  gpu.has_older_device_model,
  gpu.age
FROM revenue_metrics rm
LEFT JOIN project.games_paid_users gpu USING(user_id);