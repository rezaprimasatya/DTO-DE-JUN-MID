WITH order_data AS (
	SELECT 
		oli.ORDER_KEY,
		oli.ORDER_TOTAL_PRICE,
		oli.ORDER_DATE_KEY
	FROM
		presentation.order_line_items oli 
	GROUP BY
		1, 2, 3
)

SELECT 
	d.FISCAL_YEAR,
	SUM(o.ORDER_TOTAL_PRICE) AS REVENUE
FROM 
	order_data o
LEFT JOIN
	presentation.dates d 
	ON o.ORDER_DATE_KEY = d.DATE_KEY 
GROUP BY 
	1
ORDER BY 
	1 ASC