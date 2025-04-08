-- 1 Number of transactions on  2022/01/14
SELECT COUNT(*) AS transaction_count
FROM transactions
WHERE transaction_date = '2022-01-14';

-- 2 Total amount of all sell transactions (including tax)

SELECT SUM(amount_inc_tax) AS total_sell_amount
FROM transactions
WHERE category = 'SELL';

-- 3 Balance of (SELL - BUY) transaction by date for the product 'Amazon Echo Do'

SELECT
    DATE(transaction_date) AS date,
    SUM(CASE WHEN category = 'SELL' THEN amount_inc_tax ELSE 0 END) -
    SUM(CASE WHEN category = 'BUY' THEN amount_inc_tax ELSE 0 END) AS balance
FROM transactions
WHERE name = 'Amazon Echo Dot'
GROUP BY DATE(transaction_date)
ORDER BY date;

-- 4 Cumulated Balance of (SELL - BUY) transaction by date for the product 'Amazon Echo Do'

WITH daily_balance AS (
  SELECT
    transaction_date,
    SUM(CASE WHEN category = 'SELL' THEN amount_inc_tax ELSE 0 END) -
    SUM(CASE WHEN category = 'BUY' THEN amount_inc_tax ELSE 0 END) AS balance
  FROM transactions
  WHERE name = 'Amazon Echo Dot'
  GROUP BY transaction_date
)
SELECT
  transaction_date,
  SUM(balance) OVER (ORDER BY transaction_date) AS cumulated_balance
FROM daily_balance;