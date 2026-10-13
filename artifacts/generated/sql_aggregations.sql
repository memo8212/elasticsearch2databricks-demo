-- Kibana dashboard aggregations -> Spark SQL

-- Total base amount by department
SELECT dept, SUM(base_amount) AS total_base, COUNT(doc_id) AS txn_count FROM dept_txn GROUP BY dept ORDER BY 2 DESC;

-- Daily transaction count
SELECT date_trunc('day', txn_date) AS day, AVG(amount) AS avg_amount FROM dept_txn GROUP BY 1 ORDER BY 1;
