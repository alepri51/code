docker run --publish 7474:7474 --publish 7687:7687 --volume /c/nodejs/dombook.neo/data:/data -
-volume /c/nodejs/dombook.neo/plugins:/plugins --volume /c/nodejs/dombook.neo/conf:/conf neo4j:latest
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

"[
  [
    [
      [
        55.68555873688577,
        37.31348938258666
      ],
      [
        55.68555873688577,
        37.555188601336646
      ],
      [
        55.60595500350018,
        37.555188601336646
      ],
      [
        55.60595500350018,
        37.31348938258666
      ],
      [
        55.68555873688577,
        37.31348938258666
      ]
    ]
  ]
]"

"[
  [
    [
      [
        55.68138843117449,
        37.58952087672729
      ],
      [
        55.679642589202665,
        37.58042282374877
      ],
      [
        55.67702367950475,
        37.55295700343626
      ],
      [
        55.665964119321565,
        37.50317520411985
      ],
      [
        55.66373242393355,
        37.495278780780005
      ],
      [
        55.65664837024431,
        37.4777693203308
      ],
      [
        55.65373674200055,
        37.47382110866088
      ],
      [
        55.649465960525944,
        37.47279114039915
      ],
      [
        55.6473303943531,
        37.47605270656126
      ],
      [
        55.64616549077932,
        37.4829191616394
      ],
      [
        55.64548594762426,
        37.50969833644407
      ],
      [
        55.64412682578956,
        37.5272077968933
      ],
      [
        55.64383557923393,
        37.557248537860104
      ],
      [
        55.646359643791534,
        37.573384707293684
      ],
      [
        55.64995130016287,
        37.58814758571167
      ],
      [
        55.65198966066868,
        37.59484237941283
      ],
      [
        55.655289637459745,
        37.59982055934446
      ],
      [
        55.66101540487093,
        37.60531372340698
      ],
      [
        55.66635222721247,
        37.60737365993041
      ],
      [
        55.67430758715353,
        37.60806030543822
      ],
      [
        55.676247672443445,
        37.60651535304563
      ],
      [
        55.67818766111011,
        37.60256714137571
      ],
      [
        55.679642589202665,
        37.59621567042846
      ],
      [
        55.68138843117449,
        37.58952087672729
      ]
    ]
  ]
]"

CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.58952087672729 55.68138843117449,37.58042282374877 55.679642589202665,37.55295700343626 55.67702367950475,37.50317520411985 55.665964119321565,37.495278780780005 55.66373242393355,37.4777693203308 55.65664837024431,37.47382110866088 55.65373674200055,37.47279114039915 55.649465960525944,37.47605270656126 55.6473303943531,37.4829191616394 55.64616549077932,37.50969833644407 55.64548594762426,37.5272077968933 55.64412682578956,37.557248537860104 55.64383557923393,37.573384707293684 55.646359643791534,37.58814758571167 55.64995130016287,37.59484237941283 55.65198966066868,37.59982055934446 55.655289637459745,37.60531372340698 55.66101540487093,37.60737365993041 55.66635222721247,37.60806030543822 55.67430758715353,37.60651535304563 55.676247672443445,37.60256714137571 55.67818766111011,37.59621567042846 55.679642589202665,37.58952087672729 55.68138843117449)),((37.58952087672729 55.68138843117449,37.58042282374877 55.679642589202665,37.55295700343626 55.67702367950475,37.50317520411985 55.665964119321565,37.495278780780005 55.66373242393355,37.4777693203308 55.65664837024431,37.47382110866088 55.65373674200055,37.47279114039915 55.649465960525944,37.47605270656126 55.6473303943531,37.4829191616394 55.64616549077932,37.50969833644407 55.64548594762426,37.5272077968933 55.64412682578956,37.557248537860104 55.64383557923393,37.573384707293684 55.646359643791534,37.58814758571167 55.64995130016287,37.59484237941283 55.65198966066868,37.59982055934446 55.655289637459745,37.60531372340698 55.66101540487093,37.60737365993041 55.66635222721247,37.60806030543822 55.67430758715353,37.60651535304563 55.676247672443445,37.60256714137571 55.67818766111011,37.59621567042846 55.679642589202665,37.58952087672729 55.68138843117449)))') YIELD node AS area
MATCH (area)<-[:расположен]-(b :Корпус)
WITH COLLECT(ID(b)) AS area
CALL spatial.intersects('geom', 'POLYGON((37.31348938258666 55.68555873688577,37.555188601336646 55.68555873688577,37.555188601336646 55.60595500350018,37.31348938258666 55.60595500350018,37.31348938258666 55.68555873688577))') YIELD node AS viewport
MATCH (viewport)<-[:расположен]-(b :Корпус)
WITH area, COLLECT(ID(b)) AS viewport
WITH [x IN viewport WHERE x IN area | x] AS intersect
MATCH (b :Корпус) 
WHERE ID(b) IN intersect //AND b.parking_available = false
WITH DISTINCT b AS b, size(intersect) AS cnt SKIP 10 LIMIT 10
MATCH (b)<-[:`в составе`]-(l :Лот)-[:тип]->(nt:`Тип недвижимости`)
//WHERE l.price > 5000000 AND l.price < 10000000 //AND nt.name = 'Апартаменты'
WITH cnt, b, {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square) } } AS lots
MATCH (d:Девелопер)<-[:проектирует]-(b)-[:строит]->(z:Застройщик)
RETURN cnt, b, d, CASE WHEN z.name = 'Не указано' THEN d ELSE z END, COLLECT(lots)

CALL apoc.custom.asProcedure('polygon5',
  'CALL spatial.intersects(\'geom\', $coordinates) YIELD node AS adresses MATCH (adresses)<-[:расположен]-(b :Корпус) RETURN b AS buildings','WRITE',
  [['buildings','NODE']],[['coordinates','STRING']]);

CALL custom.polygon5('POLYGON((37.31348938258666 55.68555873688577,37.555188601336646 55.68555873688577,37.555188601336646 55.60595500350018,37.31348938258666 55.60595500350018,37.31348938258666 55.68555873688577))') YIELD buildings
RETURN buildings;

WITH [] AS area, [
'POLYGON((37.68935782592771 55.82908115271371,37.838359901123034 55.82908115271371,37.838359901123034 55.74977093199822,37.68935782592771 55.74977093199822,37.68935782592771 55.82908115271371))'
] AS poly
UNWIND poly AS p
CALL custom.polygon5(p) YIELD buildings
WITH area + COLLECT(DISTINCT ID(buildings)) AS area
CALL custom.polygon5('POLYGON((37.68935782592771 55.82908115271371,37.838359901123034 55.82908115271371,37.838359901123034 55.74977093199822,37.68935782592771 55.74977093199822,37.68935782592771 55.82908115271371))') YIELD buildings
WITH area, COLLECT(DISTINCT ID(buildings)) AS viewport
WITH [x IN viewport WHERE x IN area | x] AS intersect
MATCH (b :Корпус) 
WHERE ID(b) IN intersect //AND b.parking_available = false
WITH DISTINCT b AS b, size(intersect) AS cnt 
MATCH (b)<-[:`в составе`]-(l :Лот)-[:тип]->(nt:`Тип недвижимости`)
WHERE l.price > 5000000 AND l.price < 10000000 //AND nt.name = 'Апартаменты'
MATCH (d:Девелопер)<-[:проектирует]-(b)-[:строит]->(z:Застройщик)
WITH b {.*, lots: {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square)} } } SKIP 40 LIMIT 20
//WITH size(cnt) AS count, b, d, CASE WHEN z.name = 'Не указано' THEN d ELSE z END AS z, COLLECT(lots) AS lots SKIP 40 LIMIT 20
RETURN *

WITH [] AS area, [
'POLYGON((37.68935782592771 55.82908115271371,37.838359901123034 55.82908115271371,37.838359901123034 55.74977093199822,37.68935782592771 55.74977093199822,37.68935782592771 55.82908115271371))'
] AS poly
UNWIND poly AS p
CALL custom.polygon5(p) YIELD buildings
WITH area + COLLECT(DISTINCT ID(buildings)) AS area
CALL custom.polygon5('POLYGON((37.68935782592771 55.82908115271371,37.838359901123034 55.82908115271371,37.838359901123034 55.74977093199822,37.68935782592771 55.74977093199822,37.68935782592771 55.82908115271371))') YIELD buildings
WITH area, COLLECT(DISTINCT ID(buildings)) AS viewport
WITH [x IN viewport WHERE x IN area | x] AS intersect
MATCH (b :Корпус) 
WHERE ID(b) IN intersect //AND b.parking_available = false
WITH b, size(intersect) AS cnt 
MATCH (d:Девелопер)<-[:проектирует]-(b :Корпус)-[:строит]->(z:Застройщик)
WITH DISTINCT b
MATCH (b)<-[:`в составе`]-(l :Лот)-[:тип]->(nt:`Тип недвижимости`)
WHERE l.price > 5000000 AND l.price < 10000000 //AND nt.name = 'Апартаменты'
WITH DISTINCT b, COLLECT(l {.*, nt: nt.name}) AS lots
UNWIND lots AS l
WITH b {.*, lots: COLLECT({type: l.nt, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square)} }) }

//WITH cnt, b {.*, lots: {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square)} } } //SKIP 80 LIMIT 20
//WITH size(cnt) AS count, b, d, CASE WHEN z.name = 'Не указано' THEN d ELSE z END AS z, COLLECT(lots) AS lots SKIP 40 LIMIT 20
RETURN b //SKIP 0 LIMIT 20