{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key=['date','customerId', 'paymentPeriod']
    )
}}

WITH 
customer_information AS (
    SELECT * FROM {{ ref('customer_information') }}
),
daily_customer_drivers AS (
    SELECT * FROM {{ ref('daily_customer_drivers') }}
),
loan_transactions AS (
    SELECT * FROM {{ ref('loan_transactions') }}
)
SELECT
    l.date,
    l.customerId,
    l.paymentPeriod,
    l.loanAmount,
    l.currencyType,
    l.evaluationChannel,
    l.interestRate,
    c.gender,
    cd.monthlySalary,
    cd.healthScore,
    cd.currentDebt,
    cd.category,
    cd.isRiskyCustomer
FROM loan_transactions l
INNER JOIN daily_customer_drivers cd ON l.date = cd.date and l.customerId = cd.customerId
LEFT JOIN customer_information c ON l.customerId = c.customerId
WHERE l.date = '{{ var("target_date") }}'