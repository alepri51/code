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
//
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