-- Crea un esquema externo asociado a una base de datos en AWS Glue Data Catalog
create external schema environment_db 
from data catalog 
database 'environment_db' 
iam_role 'arn:aws:iam::302788521885:role/LabRole';

-- Crea una base de datos externa si no existe
create external database if not exists;

-- Elimina la tabla 'food_emissions' si ya existe
DROP TABLE IF EXISTS environment_db.food_emissions;

-- Crea una tabla externa para almacenar datos de emisiones de alimentos, con el esquema definido
CREATE EXTERNAL TABLE environment_db.food_emissions (
    food_product VARCHAR(255),
    land_use_change DECIMAL(10,2),
    animal_feed DECIMAL(10,2),
    farm DECIMAL(10,2),
    processing DECIMAL(10,2),
    transport DECIMAL(10,2),
    packaging DECIMAL(10,2),
    retail DECIMAL(10,2),
    total_emissions DECIMAL(10,2),
    eutrophying_emissions_per_1000kcal DECIMAL(10,2),
    eutrophying_emissions_per_kilogram DECIMAL(10,2),
    eutrophying_emissions_per_100g_protein DECIMAL(10,2),
    freshwater_withdrawals_per_1000kcal DECIMAL(10,2),
    freshwater_withdrawals_per_100g_protein DECIMAL(10,2),
    freshwater_withdrawals_per_kilogram DECIMAL(10,2),
    greenhouse_gas_emissions_per_1000kcal DECIMAL(10,2),
    greenhouse_gas_emissions_per_100g_protein DECIMAL(10,2),
    land_use_per_1000kcal DECIMAL(10,2),
    land_use_per_kilogram DECIMAL(10,2),
    land_use_per_100g_protein DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://trabajo-1-almac-recup-info/raw/food/'
TABLE PROPERTIES (
    'numRows'='43',
    'skip.header.line.count'='1'
);

-- Elimina la tabla 'co2_emissions' si ya existe
DROP TABLE IF EXISTS environment_db.co2_emissions;

-- Crea una tabla externa para almacenar datos de emisiones de CO2 por país
CREATE EXTERNAL TABLE environment_db.co2_emissions (
    country VARCHAR(255),
    code VARCHAR(10),
    calling_code BIGINT,
    year BIGINT,  -- Año de emisión
    co2_emission_tons BIGINT, 
    population_2022 BIGINT,
    area BIGINT, 
    percent_of_world VARCHAR(10), 
    density_km2 VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://trabajo-1-almac-recup-info/raw/country/'
TABLE PROPERTIES (
    'numRows'='59620',
    'skip.header.line.count'='1'
);

-- Cuenta el número de países únicos en la tabla 'country'
SELECT COUNT(DISTINCT "country") FROM "labone"."country";

-- Selecciona el país, las emisiones de CO2 y el año de la última entrada por país
SELECT t1."country", t1."co2 emission (tons)", t1."year" AS "ultima_entrada"
FROM "labone"."country" t1
INNER JOIN (
    SELECT "country", MAX("year") AS "ultima_entrada"
    FROM "labone"."country"
    GROUP BY "country"
) t2 ON t1."country" = t2."country" AND t1."year" = t2."ultima_entrada";

-- Selecciona las emisiones de CO2 por país y año, ordenado por país y año
SELECT "country", "year", "co2 emission (tons)"
FROM "labone"."country"
WHERE "co2 emission (tons)" IS NOT NULL
ORDER BY "country", "year";

-- Selecciona el país con la mayor emisión de CO2 en el año 2020
SELECT "country", MAX("co2 emission (tons)") AS "Highest_CO2"
FROM "labone"."country"
WHERE "year" = 2020
GROUP BY "country"
ORDER BY "Highest_CO2" DESC;

-- Suma total de emisiones de CO2 por país, ordenado de mayor a menor
SELECT "country", SUM("co2 emission (tons)") AS "Total_CO2"
FROM "labone"."country" 
GROUP BY "country"
ORDER BY "Total_CO2" DESC;

-- Selecciona todas las entradas para Estados Unidos donde las emisiones de CO2 sean nulas
select * 
from "labone"."country" 
where "country" = 'United States'
and "co2 emission (tons)" is null
ORDER BY "year" DESC;

-- Calcula la suma de las emisiones totales de cada categoría de alimentos, agrupado por producto
SELECT "food product", 
       SUM("land use change" + "animal feed" + "farm" + "processing" + "transport" + "packaging" + "retail") AS "total emissions"
FROM "labone"."food"
GROUP BY "food product"
ORDER BY "total emissions" DESC;

-- Selecciona las primeras 10 filas de la tabla 'food'
SELECT * FROM "labone"."food" LIMIT 10;

-- Selecciona los productos con mayor uso de tierra por kilogramo, limitando a 5 resultados
SELECT food_product, land_use_per_kilogram
FROM environment_db.food_emissions
ORDER BY land_use_per_kilogram DESC
LIMIT 5;

-- Calcula los promedios de diferentes tipos de emisiones y recursos utilizados en la producción de alimentos
SELECT AVG(land_use_change) AS avg_land_use_change, 
       AVG(animal_feed) AS avg_animal_feed,
       AVG(farm) AS avg_farm,
       AVG(processing) AS avg_processing,
       AVG(transport) AS avg_transport,
       AVG(packaging) AS avg_packaging,
       AVG(retail) AS avg_retail
FROM environment_db.food_emissions;
