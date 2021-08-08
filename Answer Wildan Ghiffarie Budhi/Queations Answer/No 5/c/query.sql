WITH order_data AS (
	SELECT 
		oli.ORDER_KEY,
		oli.ORDER_DATE_KEY,
		oli.ORDER_TOTAL_PRICE
	FROM
		presentation.order_line_items oli 
	GROUP BY
		1, 2, 3
)

SELECT 
	d.`YEAR`,
	d.`MONTH`,
	SUM(o.ORDER_TOTAL_PRICE) AS REVENUE
FROM 
	order_data o
LEFT JOIN
	presentation.dates d 
	ON o.ORDER_DATE_KEY = d.DATE_KEY 
GROUP BY 
	d.`YEAR`, d.`MONTH` 
ORDER BY 
	3 DESC 
LIMIT 
	3