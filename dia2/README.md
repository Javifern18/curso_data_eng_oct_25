# Transformación (Funciones y Procedimientos)

Mientras tu ingestabas los datos de prueba para que negocio pudiera tener una primera visión rápida de los mismos, uno de tus compañeros ha realizado el desarrollo para ingestar estos datos directamente desde la aplicación web, creando finalmente un repositorio centralizado en Snowflake, en la base de datos CURSO_DATA_ENGINEERING_2025, esquema BRONZE tenemos ya todas las tablas cargadas.

Ahora, necesitamos trabajar y pulir el dato para sacar datos relevantes a partir de él, además intentaremos que esta transformación se lance de forma sencilla, este será tu nuevo cometido.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 1: CLONING

Para asegurarnos de que todos partimos de la misma situación vamos a clonar el esquema BRONZE de CURSO_DATA_ENGINEERING_2025 a nuestra propia base de datos, reescribiendo nuestro esquema BRONZE para que haya menos lío. Hemos visto el cloning en teoría y la sintaxis es muy sencilla. Aqui está la documentación de crear un objeto como clon de otro, https://docs.snowflake.com/en/sql-reference/sql/create-clone. Ya sabeís que en la documentación la sintaxis viene con muchas pamplinas al final el comando que solemos lanzar es mucho más corto y sencillo. 

Pista : Siempre que tengáis un objeto ya creado y lo querais recrear sobreescribiendo el antiguo podéis usar la claúsula OR REPLACE en el comando CREATE.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 2: CAPA SILVER

Ahora mismo todos tenemos en nuestro BRONZE nuestras tablas con el dato RAW, osea sin tratar. El siguiente paso será pasar al schema SILVER y aprovecharemos este paso para hacer algunos cambios y añadir algún campo que enriquezca nuestros datos.

En primer lugar tenemos que crear el esquema SILVER y las tablas de SILVER, os dejo aqui los CREATES que vamos a usar (cambiad base de datos y esquema al vuestro).

```

-- SILVER

--  ORDERS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ORDERS (
    ORDER_ID VARCHAR(50),
    SHIPPING_SERVICE VARCHAR(20),
    SHIPPING_COST FLOAT,
    ADDRESS_ID VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    Promo_Name VARCHAR(50),
    ESTIMATED_DELIVERY_AT TIMESTAMP_NTZ,
    ORDER_COST FLOAT,
    USER_ID VARCHAR(50),
    ORDER_TOTAL FLOAT,
    DELIVERED_AT TIMESTAMP_NTZ,
    TRACKING_ID VARCHAR(50),
    STATUS VARCHAR(20),
    Delivery_time_hours INT
);

-- EVENTS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.EVENTS (
    EVENT_ID VARCHAR(50),
    PAGE_URL VARCHAR(200),
    EVENT_TYPE VARCHAR(50),
    USER_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    SESSION_ID VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    ORDER_ID VARCHAR(50),
    Hit_number INT
);

-- PRODUCTS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.PRODUCTS (
    PRODUCT_ID VARCHAR(50),
    PRICE FLOAT,
    NAME VARCHAR(100),
    INVENTORY NUMBER(38,0)
);

-- ADDRESSES
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ADDRESSES (
    ADDRESS_ID VARCHAR(50),
    ZIPCODE NUMBER(38,0),
    COUNTRY VARCHAR(50),
    ADDRESS VARCHAR(150),
    STATE VARCHAR(50)
);

-- USERS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.USERS (
    USER_ID VARCHAR(50),
    UPDATED_AT TIMESTAMP_NTZ,
    ADDRESS_ID VARCHAR(50),
    LAST_NAME VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ,
    PHONE_NUMBER VARCHAR(20),
    FIRST_NAME VARCHAR(50),
    EMAIL VARCHAR(100)
);

-- ORDER_ITEMS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.ORDER_ITEMS (
    ORDER_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    QUANTITY NUMBER(38,0)
);

-- PROMOS
CREATE OR REPLACE TABLE MY_DB.MY_SILVER_SCHEMA.PROMOS (
    PROMO_ID VARCHAR(50),
    DISCOUNT FLOAT,
    STATUS VARCHAR(50)
);
```

Tenemos que ejecutar todos esos creates, una vez hecho vamos a echarle un ojo más de cerca a las diferencias entre estás tablas y las de BRONZE. 

En primer lugar podemos ver que ya no todos los tipos de dato son VARCHAR(256), ahora tenemos los tipos de datos ajustados al dato que nos viene, además hemos añadido algunos campos, HIT_NUMBER en EVENTS y DELIVERY_TIME_HOURS en ORDERS. Para rellenar estas tablas tendremos que tener en cuenta estas diferencias.

El primer paso es preparar un comando INSERT INTO que cargará el dato desde BRONZE a SILVER aplicando las transformaciones necesarias, os dejo aqui el ejemplo de PROMOS para que veáis que no tiene mucha ciencia.
```
-- PROMOS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.PROMOS 
SELECT 
    PROMO_ID::varchar(50),
    DISCOUNT::float,
    STATUS::varchar(50)
FROM my_db.my_bronze_schema.promos;
```
Tened cuidado de sustituir las bases de datos y esquemas por los vuestros. 

Además, podéis ver que en este caso la única transformación que tenemos que hacer es cambiar el tipo de dato (CASTEOS), snowflake tiene una manera sencilla de aplicarlos que es con ::tipo_de_dato, esto habrá que hacerlo para todas las tablas. Os recomendamos empezar por las que no son EVENTS ni ORDERS ya que para estas habrá que añadir un campo calculado que tiene un poco más de dificultad.

Una vez hechos los INSERT INTO para las tablas sencillas podéis ejecutarlos y comprobar que efectivamente se rellenan las tablas de SILVER.

**Tablas "complicadas":**

Vamos a por ORDERS, el nuevo campo se llama DELIVERY_TIME_HOURS y os dicen desde negocio que este campo es la diferencia en horas desde que se crea un pedido hasta que se entrega. Tendreís que buscar una función de Snowflake que consiga hacer esto, tenéis el timestamp de creación en CREATED_AT y el de entrega en DELIVERED_AT.

Con EVENTS ocurre lo siguiente, a un data science flipado se le ha ocurrido la brillante idea de hacer un análisis de las sesiones en la web y nos ha pedido que le añadamos un campo que numere los distintos eventos dentro de una sesión. Osea que si dentro de una sesión lo primero que hace un usuario es visitar un producto, que ese registro lleve HIT_NUMBER = 1 , luego visita otro HIT_NUMBER = 2, luego va al carrito y revisa HIT_NUMBER = 3 y luego compra HIT_NUMBER = 4.
Esto se hace con una window function. Queremos que para todos los registros que tienen el mismo SESSION_ID se base en el timestamp CREATED_AT para añardir un campo HIT_NUMBER ordenado. 

Funciona con varias window functions pero yo por ejemplo he usado row_number, la cual podéis encontrar en la documentación de Snowflake.

Os dejamos intentarlo y pasaremos la solución por mattermost si se complica.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 3: PROCEDURES EN CAPA SILVER

Una vez tengamos todos los inserts listos vamos a meterlos en procedures para luego poder ejecutar la transformación de manera más sencilla, vamos a crear un procedure en el que llamaremos a todos los inserts, os dejo uno de ejemplo con un solo insert metido y solo tendréis que añadir los demás. Si os fijáis lo primero que se hace es truncar la tabla y luego insertar los datos. Esto lo hacemos para poder lanzarlo todas las veces que queramos sin que se nos acumulen datos repetidos, en la práctica normalmente las tablas serán históricas y cada día se insertaran días nuevos por lo que el truncate sobraría.
```
CREATE OR REPLACE PROCEDURE INSERT_PROCEDURE()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
BEGIN
    -- Truncado de la tabla
    TRUNCATE TABLE MY_DB.MY_SILVER_SCHEMA.PROMOS;

    -- Insert de la tabla
    INSERT INTO MY_DB.MY_SILVER_SCHEMA.PROMOS 
    SELECT 
        PROMO_ID::varchar(50),
        DISCOUNT::float,
        STATUS::varchar(50)
    FROM my_db.my_bronze_schema.promos;
    -- Añadir aqui resto de tablas
    -- ...
    -- Devolvemos un mensaje diciendo que ha ido todo bien
    RETURN 'Insertado con éxito máquina';
END;
```

### PASO 4: CAPA GOLD

Ya tenemos nuestra SILVER que da gusto verla, nuestros campos están con los formatos adecuados y hemos añadido un par de campos que aportan una información muy interesante. Ahora vamos a movernos a GOLD, en esta capa vamos a montar dos casos de uso. Estos casos de uso son dos ejemplos sencillos de como podríamos agregar los datos que nos dan para obtener información relevante para el negocio.


#### Caso de uso 1: SESSION DETAILS

Queremos saber más sobre las sesiones web y vamos a crear una tabla agrupada por sesión que nos de información valiosa a la que puedan mirar los usuarios de negocio y entender mejor como actuan nuestros clientes en la web. Os dejo primero el CREATE de la tabla, recordad cambiar base de datos y esquema a los vuestros y crear el esquema GOLD si es que aún no lo habéis hecho.
```
-- SESSION DETAILS
CREATE OR REPLACE TABLE MY_DB.MY_GOLD_SCHEMA.session_details (
    session_id VARCHAR(50),
    Finish_with_Order BOOLEAN,
    Num_Eventos INT,
    Session_duration_minutes INT
);
```
Para rellenar esta tabla tendremos que construir un INSERT INTO, pero podemos centrarnos en la select y luego simplemente meterla en un INSERT INTO.

Algunos detalles y explicaciones:

Agruparemos por sesión por lo que cada registro corresponderá a una sesión y tendrá su session_id correspondiente (viene de tabla EVENTS), además queremos tener un indicador de si la sesión acabó en compra, la pista que os doy aqui es que en la tabla events si una sesión termina en compra uno de los registros asociado a esa sesión llevará un order_id no nulo. El tercer campo es el número de eventos que han tenido lugar en esa sessión, hemos calculado un campo en SILVER que puede simplificar este cálculo mucho. Para finalizar queremos saber la duración de una sesión en minutos, os diré que para esto se usa el campo CREATED_AT de la tabla EVENTS y una función que ya usamos en silver para calcular la diferencia en horas desde que se creaba un pedido hasta que era entregado.

PD: Sólo necesitamos la tabla EVENTS para calcular todo esto, entendemos que son cosas cada vez más complejas, no hay problema si no las sacáis, preguntad todo y al final de la clase se compartirán todas las soluciones. Lo importante es que entendáis los casos de uso y que sepáis explicar lo que se quiere conseguir.


#### Caso de uso 2: STATE ANALISIS

Vamos a hacer una agrupación por estado (los datos son de Estados Unidos) 
Os dejamos el create por aquí, recordad cambiar base de datos y esquema.
```
-- STATE ANALYSIS
CREATE OR REPLACE TABLE MY_DB.MY_GOLD_SCHEMA.GENERAL_STATE_ANALYSIS (
    state VARCHAR(50),
    TOTAL_ORDER_COSTS FLOAT,
    NUMBER_OF_ORDERS INT,
    NUMBER_OF_USERS INT,
    SHIPPING_COST_PERCENTAGE FLOAT,
    Main_shipping_service VARCHAR(50)
);
```
Igual que antes  podemos centrarnos en la select y luego simplemente meterla en un INSERT INTO.
Como véis necesitamos datos de varias tablas y aplicar ciertos cálculos a algunos de ellos. Entiendo que esto puede ser complejo por favor preguntad todas las dudas y no os preocupéis si no conseguís terminarlo, nos hemos pasado y el ejercicio es demasiado largo.

Algunos detalles y pistas:

Vamos a tener que construir una select con un join para poder coger datos de dos tablas y agruparlos por estado. 
Las tablas que necesitamos son ORDERS y ADDRESSES, con ellas vamos a poder obtener todo lo que se nos pide, sabemos que estás tablas se unen por el campo ADDRESS_ID que las dos contienen.

En TOTAL_ORDER_COSTS se esperan encontrar los gastos totales de todos los pedidos que se han hecho en un estado.
NUMBER_OF_ORDERS cuenta el numero de pedidos que ha habido en un estado y NUMBER_OF_USERS el numero de usuarios distintos que han pedido algo en un estado.
SHIPPING_COST_PERCENTAGE es un ratio que nos indica cuanto ha supuesto los gastos de envío en relación con es coste total de los pedidos para cada estado.
Finalmente MAIN_SHIPPING_SERVICE corresponde a el servicio de paquetería más usado en cada estado.


### PASO 5: PROCEDURES EN CAPA GOLD

Felicidades ya has terminado realmente, tu INSERT INTO en gold va fino y funciona de lujo y ya solo tienes que crear un procedure con la misma forma que el de silver que contenga el insert into para cada caso de uso. Puedes copiar y pegar el de SILVER y sustituir las queries que tira por el insert into de GOLD correspondiente. Con esto la transformación está completamente desarrollada y preparada para lanzarse cómodamente. Te has ganado una cerveza compañer@.

PD: Espero que alguien llegue a este punto


