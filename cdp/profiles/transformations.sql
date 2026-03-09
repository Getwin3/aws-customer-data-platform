SELECT
customer_id,
MAX(transaction_date) as last_purchase
FROM transactions
GROUP BY customer_id