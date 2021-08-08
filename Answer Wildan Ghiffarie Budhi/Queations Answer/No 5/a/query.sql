WITH order_data AS (
	SELECT 
		oli.ORDER_KEY,
		oli.CUSTOMER_NATION_KEY,
		oli.ORDER_TOTAL_PRICE
	FROM
		presentation.order_line_items oli 
	GROUP BY
		1, 2, 3
),

top_nation AS (
	SELECT 
		o.CUSTOMER_NATION_KEY,
		SUM(o.ORDER_TOTAL_PRICE) AS REVENUE
	FROM 
		order_data o
	GROUP BY
		o.CUSTOMER_NATION_KEY
	ORDER BY
		2 DESC
	LIMIT 
		5
)

SELECT
	n.NATION_NAME,
	t.REVENUE
FROM 
	top_nation t
LEFT JOIN
	presentation.nations n 
	ON t.CUSTOMER_NATION_KEY = n.NATION_KEY 