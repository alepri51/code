const neo4j = require('neo4j-driver').v1;
const axios = require('axios');

let vieport = [
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
];

for(let i = 0; i < vieport.length; i++) {
    let [lon, lat] = vieport[i];
    vieport[i] = [lat, lon];
}

vieport = JSON.stringify(vieport).replace(/\[/g, '(').replace(/\]/g, ')').replace(/\,/g, ' ').replace(/\) \(/g, ',');;

let area = [
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
];

for(let i = 0; i < area.length; i++) {
    let [lon, lat] = area[i];
    area[i] = [lat, lon];
}

area = JSON.stringify(area).replace(/\[/g, '(').replace(/\]/g, ')').replace(/\,/g, ' ').replace(/\) \(/g, ',');

let mp = `POLYGON${vieport},POLYGON${area}`;
//let mp = `MULTIPOLYGON(${vieport},${area})`;
console.log(mp);

console.time('time');

const driver = neo4j.driver('bolt://localhost:7687', neo4j.auth.basic('neo4j', '123'), {
    maxConnectionLifetime: 3 * 60 * 60 * 1000, // 3 hours
    maxConnectionPoolSize: 2500,
    connectionAcquisitionTimeout: 2 * 60 * 1000 // 120 seconds
});

const session = driver.session();


let schema = [
    'CREATE INDEX ON :Адрес(name)',
    'CREATE INDEX ON :Девелопер(name)',
    'CREATE INDEX ON :Застройщик(name)',
    'CREATE INDEX ON :Класс(name)',
    'CREATE INDEX ON :Конструктив(name)',
    'CREATE INDEX ON :Корпус(id)',
    'CREATE INDEX ON :Лот(id)',
    'CREATE INDEX ON :Лот(price)',
    'CREATE INDEX ON :Отделка(name)',
    'CREATE INDEX ON :Проект(name)',
    'CREATE INDEX ON :Стадия(name)',
    'CREATE INDEX ON :`Тип парковки`(name)',
    'CREATE INDEX ON :`Тип недвижимости`(name)',
    'CREATE INDEX ON :`Тип фото`(name)',
    'CREATE INDEX ON :Фото(url)',
    //'CALL spatial.removeLayer("geom")',
    'CALL spatial.addPointLayer("geom")'
]

let after = [
    `MATCH (n:\`Адрес\`) WHERE EXISTS(n.latitude) AND EXISTS(n.longitude)
        WITH n
        CALL spatial.addNode('geom',n) YIELD node
    RETURN node;`,
    `CALL spatial.intersects('geom', 'MULTIPOLYGON(((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)), ((37.06399542968747 56.06757655398861, 38.173614570312466 56.06757655398861, 38.173614570312466 55.432467441048146, 37.06399542968747 55.432467441048146, 37.06399542968747 56.06757655398861)))') YIELD node AS address
    MATCH (address)<-[:расположен]-(b :Корпус)<-[:\`в составе\`]-(l :Лот)-[:тип]->(nt:\`Тип недвижимости\`)
    WHERE l.price > 5000000 AND l.price < 10000000 AND nt.name = 'Апартаменты'
    WITH b, {type: nt.name, rooms: l.rooms, count: COUNT(l), price: { min: MIN(l.price), max: MAX(l.price) }, square: { min: MIN(l.square), max: MAX(l.square) } } AS lots
    WITH DISTINCT b, lots
    //UNWIND db AS b
    MATCH (d:Девелопер)<-[:проектируется]-(b)-[:строится]->(z:Застройщик)
    RETURN b, d, z, COLLECT(lots)`
]

const promise = session.run(
    `CALL apoc.load.jsonParams('https://api.best-novostroy.ru/api/v1/dombook/available-building-ids',{Authorization:'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'}, null) 
    YIELD value as buildings 
    RETURN buildings.result`
);

promise.then(async result => {
    session.close();

    for(let i = 0; i < schema.length; i++) {
        await session.run(schema[i]);
    }

    const singleRecord = result.records[0];
    let ids = singleRecord.get(0);
    ids = ids.map(id => {
        //id = neo4j.int(123);
        return id.toString();
    });

    let requests = [];
    let queries = [];

    console.log(ids);
    while (ids.length) {
        await Promise.all(requests);
        requests = [];

        await Promise.all(queries);
        queries = [];

        let part = ids.splice(0, 10);
        let i = part.length - 1;


        while(i > -1) {

            let id = part[i];
            i--;

            let url = `https://api.best-novostroy.ru/api/v1/dombook/building-by-id/${id}?include=lots`;

            let request = axios({
                url,
                headers: {
                    Authorization: 'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'
                },
                $id: id
            })
            .then(result => {
                let building = result.data;

                const session = driver.session();
                const promise = session.run(
                    `WITH {building} as building
                    
                    MERGE (b :Корпус {id: building.id}) SET b += building {.commercial_square, .total_square, .pantry_count, .created_at, .floors_num, .parking_spaces_count, .discounts, .updated_at, .parking_available, .floors_from, .pantry_available, .construction_start, .floors_to, .in_operation_date, .sales_start, .name, .living_square, .ceiling_height, .building_type}
                    
                    MERGE (cs :Стадия {name: COALESCE(building.construction_stage.name, 'Не указано')}) SET cs += COALESCE(building.construction_stage, {})
                    MERGE (pt :\`Тип парковки\` {name: COALESCE(building.parking_type.name, 'Не указано')}) SET pt += COALESCE(building.parking_type, {})
                    MERGE (bd :Организация:Застройщик {name: COALESCE(building.builder.name, 'Не указано')}) SET bd += COALESCE(building.builder, {})
                    MERGE (dv :Организация:Девелопер {name: COALESCE(building.developer.name, 'Не указано')}) SET dv += COALESCE(building.developer, {})
                    MERGE (addr :Адрес {name: COALESCE(building.address.string, 'Не указано')}) SET addr += COALESCE(building.address, {}) REMOVE addr.string// {.*, string: null}
                    MERGE (constr :Конструктив {name: COALESCE(building.constructive.name, 'Не указано')}) SET constr += COALESCE(building.constructive, {})
                    MERGE (class :Класс {name: COALESCE(building.realty_class.name, 'Не указано')}) SET class += COALESCE(building.realty_class, {})
                    MERGE (proj :Проект {name: COALESCE(building.project_name, 'Не указано')}) SET proj.description = building.project_description
                    
                    MERGE (b)-[:\`в стадии\`]-(cs)
                    MERGE (b)-[:имеет]-(pt)
                    MERGE (b)-[:строится]-(bd)
                    MERGE (b)-[:проектируется]-(dv)
                    MERGE (b)-[:расположен]-(addr)
                    MERGE (b)-[:имеет]-(constr)
                    MERGE (b)-[:соответствует]-(class)
                    MERGE (b)-[:\`в составе\`]-(proj)
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                });
    

                const promise1 = session.run(
                    `WITH {building} as building
                    
                    MATCH (b :Корпус {id: building.id})

                    UNWIND building.finishings AS finishing
                        MERGE (n :Отделка {name: COALESCE(finishing.name, 'Не указано')}) SET n += COALESCE(finishing, {})
                        MERGE (b)-[:имеет]-(n)
                    
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                });

                const promise2 = session.run(
                    `WITH {building} as building
                    
                    MATCH (b :Корпус {id: building.id})

                    UNWIND building.building_photos AS building_photo

                        MERGE (pt :\`Тип фото\` {name: building_photo.photo_type.name})

                        MERGE (n :Фото {url: COALESCE(building_photo.url, 'Не указано')}) 
                            SET n += building_photo {.*, photo_type: null}
                            REMOVE n.photo_type

                        MERGE (n)-[:есть]->(pt)
                        MERGE (b)-[:имеет]-(n)
                    
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                });

                const promise3 = session.run(
                    `WITH {building} as building
                    
                    UNWIND CASE WHEN SIZE(building.lots) > 0 THEN [] ELSE [0] END AS m
                        WITH building, building.analytics AS analytics, ['st', 'sp', '1', '2', '3', '4'] AS rooms

                        UNWIND rooms AS room
                            WITH {
                                name: toString(building.id) + '_' + room,
                                building_id: building.id,
                                rooms: room, 
                                is_studio: CASE WHEN room = 'st' THEN true ELSE false END, 
                                is_open_plan: CASE WHEN room = 'sp' THEN true ELSE false END,
                                count: analytics['count_' + room], 
                                price_min: analytics['price_min_' + room], 
                                price_max: analytics['price_max_' + room], 
                                price_square_min: analytics['price_square_min_' + room], 
                                price_square_max: analytics['price_square_max_' + room], 
                                square_min: analytics['square_min_' + room], 
                                square_max: analytics['square_max_' + room],
                                finishing: building.finishings[0]
                            } AS stat
                            WHERE analytics['count_' + room] IS NOT NULL

                            WITH stat, {building} as building
                            MATCH (b :Корпус {id: building.id})
                            MERGE (s :Статистика {name: stat.name}) 
                                SET s += stat {.*, finishing: null}

                            MERGE (s)<-[:имеет]-(b)
                            WITH s AS stat

                            UNWIND ['_min', '_max'] AS minmax
                                WITH collect(DISTINCT {
                                    is_fake: true,
                                    rooms: stat.rooms, 
                                    is_studio: stat.is_studio, 
                                    is_open_plan: stat.is_open_plan, 
                                    price: stat['price' + minmax], 
                                    price_square: stat['price_square' + minmax], 
                                    square: stat['square' + minmax],
                                    lot_finishing_type: stat.finishing,
                                    id: stat.name + minmax,
                                    stat_name: stat.name
                                }) AS lots
                    

                    UNWIND CASE WHEN SIZE(lots) = 0 THEN {building}.lots ELSE lots END AS lot
                        WITH lot, {building} AS building
                        MATCH (b :Корпус {id: building.id})

                        MERGE (l :Лот {id: lot.id})
                            SET l += lot {.identifier, .rooms, .created_at, .section, .price_square, .is_open_plan, .finishing, .number, .square, 
                                .is_studio, .updated_at, .price, .price_square, .floor, .ceiling_height, building_id: b.id}
                            
                        MERGE (l)-[:\`в составе\`]-(b)

                        FOREACH(label IN CASE WHEN lot.is_fake = true THEN [0] ELSE [] END |
                            SET l:Фэйк
                        )
                        FOREACH(label IN CASE WHEN lot.is_studio = true THEN [0] ELSE [] END |
                            SET l:Студия
                        )
                        FOREACH(label IN CASE WHEN lot.is_open_plan = true THEN [0] ELSE [] END |
                            SET l:\`Свободная планировка\`
                        )
                        
                        MERGE (ot :Отделка {name: COALESCE(lot.lot_finishing_type.name, 'Не указано')}) SET ot += COALESCE(lot.lot_finishing_type, {})
                        MERGE (l)-[:имеет]-(ot)
                        
                        MERGE (rt :\`Тип недвижимости\` {name: COALESCE(lot.lot_type.name, building.lot_type.name, 'Не указано')}) 
                            SET rt += COALESCE(lot.lot_type, building.lot_type, {})
                        MERGE (l)-[:тип]-(rt)
                        
                        WITH l, b, lot
                        UNWIND lot.planing_photos AS photo
                            MERGE (ph :Фото {url: photo.url}) 
                                SET ph += photo
                            MERGE (l)-[:имеет]-(ph)
                        
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                });

                /* const promise4 = session.run(
                    `
                    
                    MATCH (n:Адрес) WHERE EXISTS(n.latitude) AND EXISTS(n.longitude)
                    WITH n
                    CALL spatial.addNode('geom',n) YIELD node
                    RETURN node;
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                }); */


                queries.push(promise);
                queries.push(promise1);
                queries.push(promise2);
                queries.push(promise3);
                //queries.push(promise4);

                
            })
            .catch(err => {
                console.log(err);
                ids.push(err.config.$id);
            });

            requests.push(request);
        }
    }

    // on application exit:
    //driver.close();
    console.log('err');

    for(let i = 0; i < after.length; i++) {
        console.time('find');
        let res = await session.run(after[i]);
        console.timeEnd('find');
        console.log(res);
    }

    console.timeEnd('time');
});




/* MERGE (cs :Стадия {name: COALESCE(building.construction_stage.name, 'Не указано')}) SET cs += COALESCE(building.construction_stage, {})
                    MERGE (pt :\`Тип парковки\` {name: COALESCE(building.parking_type.name, 'Не указано')}) SET pt += COALESCE(building.parking_type, {})
                    MERGE (bd :Организация:Застройщик {name: COALESCE(building.builder.name, 'Не указано')}) SET bd += COALESCE(building.builder, {})
                    MERGE (dv :Организация:Девелопер {name: COALESCE(building.developer.name, 'Не указано')}) SET dv += COALESCE(building.developer, {})
                    MERGE (addr :Адрес {name: COALESCE(building.address.string, 'Не указано')}) SET addr += COALESCE(building.address, {}) REMOVE addr.string// {.*, string: null}
                    MERGE (constr :Конструктив {name: COALESCE(building.constructive.name, 'Не указано')}) SET constr += COALESCE(building.constructive, {})
                    MERGE (class :Класс {name: COALESCE(building.realty_class.name, 'Не указано')}) SET class += COALESCE(building.realty_class, {})
                    MERGE (proj :Проект {name: COALESCE(building.project_name, 'Не указано')}) SET proj.description = building.project_description
                   
                    MERGE (b)-[:\`в стадии\`]-(cs)
                    MERGE (b)-[:имеет]-(pt)
                    MERGE (b)-[:строится]-(bd)//сделать проверку когда застройщик это девелопер
                    MERGE (b)-[:проектируется]-(dv)
                    MERGE (b)-[:расположен]-(addr)
                    MERGE (b)-[:имеет]-(constr)
                    MERGE (b)-[:соответствует]-(class)
                    MERGE (b)-[:\`в составе\`]-(proj) */

/* FOREACH(finishing IN building.finishings |
    MERGE (n :Отделка {name: COALESCE(finishing.name, 'Не указано')}) SET n += COALESCE(finishing, {})
    MERGE (b)-[:имеет]-(n)
)
FOREACH(building_photo IN building.building_photos |
    MERGE (pt :\`Тип фото\` {name: building_photo.photo_type.name})
    MERGE (n :Фото {url: COALESCE(building_photo.url, 'Не указано')}) 
        SET n += building_photo {.*, photo_type: null}
        REMOVE n.photo_type
    MERGE (n)-[:есть]->(pt)
    MERGE (b)-[:имеет]-(n)
)
FOREACH(lot IN building.lots |
    MERGE (l :Лот {id: lot.id})
        SET l += lot {.identifier, .rooms, .created_at, .section, .price_square, .is_open_plan, .finishing, .number, .square, 
            .is_studio, .updated_at, .price, .floor, .ceiling_height}
        
    FOREACH(label IN CASE WHEN lot.is_studio = true THEN [0] ELSE [] END |
        SET l:Студия
    )
    FOREACH(label IN CASE WHEN lot.is_open_plan = true THEN [0] ELSE [] END |
        SET l:\`Свободная планировка\`
    )
    
    FOREACH(photo IN lot.planing_photos |
        MERGE (ph :Фото {url: photo.url}) 
            SET ph += photo
        MERGE (l)-[:имеет]-(ph)
    )
    MERGE (ot :Отделка {name: COALESCE(lot.lot_finishing_type.name, 'Не указано')}) SET ot += COALESCE(lot.lot_finishing_type, {})
    MERGE (l)-[:имеет]-(ot)
    
    MERGE (rt :\`Тип недвижимости\` {name: COALESCE(lot.lot_type.name, building.lot_type.name, 'Не указано')}) 
        SET rt += COALESCE(lot.lot_type, building.lot_type, {})
    MERGE (l)-[:тип]-(rt)
    
    MERGE (l)-[:\`в составе\`]-(b)
) */