{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key=['date','paymentPeriod', 'evaluationChannel', 'category', 'currencyType', 'gender']
    )
}}

WITH loan_transactions_w_drivers AS (
    SELECT * FROM {{ ref('loan_transactions_w_drivers') }}
)
SELECT
    date,
    paymentPeriod,
    evaluationChannel,
    category,
    currencyType,
    gender,
    Sum(loanAmount) as sum_loanAmount,
    Avg(loanAmount) as avg_loanAmount,
    Sum(currentDebt) as sum_currentDebt,
    Avg(currentDebt) as avg_currentDebt,
    Max(interestRate) as max_interestRate,
    Min(interestRate) as min_interestRate,
    Avg(interestRate) as avg_interestRate,
    Avg(healthScore) as health_score,
    Avg(monthlySalary) as monthly_salary
FROM loan_transactions_w_drivers
WHERE date = '{{ var("target_date") }}'
GROUP BY 
    date,
    paymentPeriod,
    evaluationChannel,
    category,
    currencyType,
    gender