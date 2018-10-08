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
///////////////////////////////
WITH 'https://api.best-novostroy.ru/api/v1/dombook/building-by-id/403?include=lots' AS url
CALL apoc.load.jsonParams(url,{Authorization:'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'}, null)
YIELD value as building
//RETURN SIZE(building.lots);

UNWIND CASE WHEN SIZE(building.lots) > 0 THEN [] ELSE [0] END AS m
WITH building, building.analytics AS analytics, ['price', 'square'] AS keys, ['st', 'sp', '1', '2', '3', '4'] AS rooms, ['_min_', '_max_'] AS minmax

UNWIND rooms AS room
WITH {rooms: room, 
	is_studio: CASE WHEN room = 'st' THEN true ELSE false END, 
	is_open_plan: CASE WHEN room = 'sp' THEN true ELSE false END,
	count: analytics['count_' + room], 
	price: { min: analytics['price_min_' + room], max: analytics['price_max_' + room] }, 
	price_square: { min: analytics['price_square_min_' + room], max: analytics['price_square_max_' + room] }, 
	square: { min: analytics['square_min_' + room], max: analytics['square_max_' + room] },
    finishing: building.finishings[0]
} AS stat
WHERE analytics['count_' + room] IS NOT NULL
WITH collect(stat) AS statistics
UNWIND statistics AS stat
UNWIND ['min', 'max'] AS minmax
WITH statistics, collect(DISTINCT stat {
	.rooms, .is_studio, .is_open_plan, 
    price: stat.price[minmax], 
    price_square: stat.price_square[minmax], 
    square: stat.square[minmax],
    lot_finishing_type: stat.finishing
}) AS lot
RETURN *
//////////////////////////////////////////
CALL spatial.addPointLayer('geom');
CALL spatial.layers();

MATCH (n:`Адрес`) WHERE EXISTS(n.latitude) AND EXISTS(n.longitude)
WITH n
CALL spatial.addNode('geom',n) YIELD node
RETURN node;

CALL spatial.bbox('geom',{lon: 37.06399542968747, lat: 56.06757655398861}, {lon: 38.173614570312466, lat: 55.432467441048146});

POLYGON((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861))

CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') 
YIELD node AS address
MATCH (address)--(b :Корпус) RETURN b

//EXPLAIN
CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') YIELD node AS address
MATCH (address)<-[:расположен]-(b :Корпус)
WITH DISTINCT b AS bc
//UNWIND bc AS b
MATCH (bc)<-[:`в составе`]-(l :Лот) 
WHERE (bc)<-[:`в составе`]-(l) AND l.price > 5000000 AND l.price < 10000000
RETURN bc

//EXPLAIN
CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') YIELD node AS address
MATCH (address)<-[:расположен]-(b :Корпус)<-[:`в составе`]-(l :Лот)-[:тип]->(nt:`Тип недвижимости`)
WHERE l.price > 5000000 AND l.price < 10000000 AND nt.name = 'Апартаменты'
//WITH DISTINCT b, nt, l
MATCH (d:Девелопер)<-[:проектируется]-(b)-[:строится]->(z:Застройщик)
RETURN DISTINCT b.name, COLLECT(DISTINCT d.name) AS d, COLLECT(DISTINCT z.name) AS z, {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square) } } AS lots

//EXPLAIN
CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') YIELD node AS address
MATCH (address)<-[:расположен]-(b :Корпус)<-[:`в составе`]-(l :Студия:`Свободная планировка`)-[:тип]->(nt:`Тип недвижимости`)
//WHERE l.price > 5000000 AND l.price < 10000000 AND nt.name = 'Апартаменты'
//WITH DISTINCT b, nt, l
MATCH (d:Девелопер)<-[:проектируется]-(b)-[:строится]->(z:Застройщик)
RETURN DISTINCT b.name, COLLECT(DISTINCT d.name) AS d, COLLECT(DISTINCT z.name) AS z, {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square) } } AS lots
//MULTIPOLYGON( ( (1 1,5 1,5 5,1 5,1 1) ,(2 2, 3 2, 3 3, 2 3,2 2) ), ( (3 3,6 2,6 4,3 3) ) )

CALL spatial.intersects('geom', 'POLYGON((37.43770224731442 55.676943531025884, 37.55855185668943 55.676943531025884, 37.55855185668943 55.63715317690114, 37.43770224731442 55.63715317690114, 37.43770224731442 55.676943531025884))') YIELD node AS address

"[
  [
    [
      [
        55.676943531025884,
        37.43770224731442
      ],
      [
        55.676943531025884,
        37.55855185668943
      ],
      [
        55.63715317690114,
        37.55855185668943
      ],
      [
        55.63715317690114,
        37.43770224731442
      ],
      [
        55.676943531025884,
        37.43770224731442
      ]
    ]
  ]
]"
 [
        56.06757655398861,
        37.06399542968747
      ],
      [
        56.06757655398861,
        38.173614570312466
      ],
      [
        55.432467441048146,
        38.173614570312466
      ],
      [
        55.432467441048146,
        37.06399542968747
      ],
      [
        56.06757655398861,
        37.06399542968747
      ]

      CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') YIELD node AS address
MATCH (address)<-[:расположен]-(b :Корпус)
WHERE b.parking_available = false
WITH DISTINCT b AS b SKIP 50 LIMIT 10
MATCH (b)<-[:`в составе`]-(l :Лот)-[:тип]->(nt:`Тип недвижимости`)
//WHERE l.price > 5000000 AND l.price < 10000000 //AND nt.name = 'Апартаменты'
WITH b, {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square) } } AS lots
MATCH (d:Девелопер)<-[:проектируется]-(b)-[:строится]->(z:Застройщик)
RETURN b, d, CASE WHEN z.name = 'Не указано' THEN d ELSE z END, COLLECT(lots)