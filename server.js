const neo4j = require('neo4j-driver').v1;
const axios = require('axios');

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
    'CREATE INDEX ON :Отделка(name)',
    'CREATE INDEX ON :Проект(name)',
    'CREATE INDEX ON :Стадия(name)',
    'CREATE INDEX ON :`Тип парковки`(name)',
    'CREATE INDEX ON :`Тип недвижимости`(name)',
    'CREATE INDEX ON :`Тип фото`(name)',
    'CREATE INDEX ON :Фото(name)'
]

const promise = session.run(
    `CALL apoc.load.jsonParams('https://api.best-novostroy.ru/api/v1/dombook/available-building-ids',{Authorization:'Basic ZG9tYm9vazozZDUxMjVjMTYwNDYwNjgxZGEzMDc2MWNjMmVjNDc2NDZmOTllMTNjMDc3ZWQ4MjdhMTM1ZDQyMDczZDE0Yjk4'}, null) 
    YIELD value as buildings 
    RETURN buildings.result`
);

promise.then(async result => {
    session.close();

    for(let i = 0; i < schema.length - 1; i++) {
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

        let part = ids.splice(0, 100);
        let i = part.length - 1;


        while(i > -1) {

            let id = parseInt(part[i]);
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
                    
                    MATCH (b :Корпус {id: building.id})

                    UNWIND building.lots AS lot

                        MERGE (l :Лот {id: lot.id})
                            SET l += lot {.identifier, .rooms, .created_at, .section, .price_square, .is_open_plan, .finishing, .number, .square, 
                                .is_studio, .updated_at, .price, .floor, .ceiling_height}
                            
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
                        
                            MERGE (l)-[:\`в составе\`]-(b)
                    
                    `,
                    { building }
                ).catch(err => {
                    console.log(err);
                    ids.push(building.id);
                });; 


                queries.push(promise);
                queries.push(promise1);
                queries.push(promise2);
                queries.push(promise3);

                
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