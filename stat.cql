//STATISTICS
//EXPLAIN
//MATCH (n) DETACH DELETE n
//WITH 'https://api.best-novostroy.ru/api/v1/dombook/available-building-ids' AS url
//CALL apoc.load.jsonParams(url,{Authorization:'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'}, null)
//YIELD value as buildings

//UNWIND buildings.result AS id
//WITH buildings.result[1800..1999] AS sub
//UNWIND sub AS id
WITH 'https://api.best-novostroy.ru/api/v1/dombook/building-by-id/13?include=lots' AS url
CALL apoc.load.jsonParams(url,{Authorization:'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'}, null)
YIELD value as building

WITH building, building.analytics AS analytics, ['price', 'square'] AS keys, ['st', 'sp', '1', '2', '3', '4'] AS rooms, ['_min_', '_max_'] AS minmax

UNWIND rooms AS room
WITH {rooms: room, is_studio: CASE WHEN room = 'st' THEN true ELSE false END, is_open_plan: CASE WHEN room = 'sp' THEN true ELSE false END,
count: analytics['count_' + room], price: { min: analytics['price_min_' + room], max: analytics['price_max_' + room] }, price_square: { min: analytics['price_square_min_' + room], max: analytics['price_square_max_' + room] }, square: { min: analytics['square_min_' + room], max: analytics['square_max_' + room] } } AS stat WHERE analytics['count_' + room] IS NOT NULL
WITH collect(stat) AS statistics
RETURN *