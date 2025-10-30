# Tablas dinámicas

Las tablas dinámicas son una función relativamente nueva de Snowflake diseñada para **actualizar automáticamente los conjuntos de datos derivados a medida que cambian los datos subyacentes.** Estas tablas están pensadas para reducir la sobrecarga de mantenimiento de los conjuntos de datos que necesitan mantenerse actualizados con los datos base de los que derivan.

¿Dónde habíamos visto esto? Cuando estuvimos viendo los Task, también hacíamos actualizaciones de los datos de las tablas. Entonces, ¿cuándo utilizo una tarea y cuando una tabla dinámica?:

- Las Task son más adecuadas para escenarios en los que se necesita un control sobre cuándo y cómo se ejecutan los procesos de datos, especialmente para operaciones son continuas basadas en intervalos.
- Las tablas dinámicas son perfectas para cuando necesitamos que un conjunto de datos siempre se encuentre en su forma más actualizada, sin necesidad de activar manualmente dichas actualizaciones.


# 🔔 Nueva tarea

¡Sorpresa! Aunque todo parecía estar listo y en funcionamiento, siempre hay cambios por hacer. Esto refleja una realidad muy importante en el manejo de datos: los flujos de datos **NUNCA SE TERMINAN**.

Vamos a ver las tareas que se necesitan realizar utilizando Snowflake y las tablas dinámicas:

Tu equipo ha oído hablar de las tablas dinámicas y espera que utilices estas para gestionar todo el flujo de datos, desde la captura inicial (Bronze) hasta las transformaciones intermedias (Silver) y finalmente la refinación hacia los datos de alta calidad (Gold).

## 1. ¿Cómo está montado actualmente nuestro flujo de datos?

### a) Capa Silver

Ayer, ingestábamos en nuestra capa Silver utilizando un STREAM y dos TASKS:

- El stream captura los cambios en la tabla ORDERS_HIST de BRONZE (CURSO_DATA_ENGINEERING_2025) y desencadena la TAREA PADRE.
- La TAREA PADRE mergea la información del stream con la tabla de ORDERS en Silver de nuestra BBDD.
- La TAREA HIJA, se ejecuta después de la PADRE y recrea una tabla agregada en Gold cuyo origen de información es la tabla ORDERS de Silver que acaba de ser mergeada.

## 2. Tu tarea de crear tablas dinámicas con el flujo de datos antiguo

### a) Capa Silver

En esta etapa, debes crear 1 tabla dinámica SILVER.DT_ORDERS para procesar de manera automática e incremental los datos que llegan a la tabla ORDERS_HIST de bronze de la base de datos centralizada (**curso_snowflake_de_2025**). Utilizarás el warehouse WH_CURSO_DATA_ENGINEERING y deberás configurar un **LAG** de tal manera que DT_ORDERS se actualice cuando las tablas dinámicas que crearemos en la capa **GOLD** lo necesiten (ya que dependen entre sí).

```sql
SELECT
        ORDER_ID::varchar() AS ORDER_ID,
        SHIPPING_SERVICE::varchar(20) AS SHIPPING_SERVICE,
        (replace(SHIPPING_COST, ',', '.'))::decimal AS SHIPPING_COST,
        ADDRESS_ID::varchar(50) AS ADDRESS_ID,
        CREATED_AT::timestamp_ntz AS CREATED_AT,
        IFNULL(promo_id, 'N/A') AS PROMO_NAME,
        ESTIMATED_DELIVERY_AT::timestamp_ntz AS ESTIMATED_DELIVERY_AT,
        (replace(ORDER_COST, ',', '.'))::decimal AS ORDER_COST,
        USER_ID::varchar(50) AS USER_ID,
        (replace(ORDER_TOTAL, ',', '.'))::decimal AS ORDER_TOTAL,
        DELIVERED_AT::timestamp_ntz AS DELIVERED_AT,
        TRACKING_ID::varchar(50) AS TRACKING_ID,
        STATUS::varchar(20) AS STATUS,
        TIMESTAMPDIFF(HOUR, created_at, delivered_at) AS DELIVERY_TIME_HOURS
    FROM curso_data_engineering_2025.bronze.orders_hist
```

**Si nos fijamos, en esta última SELECT no estamos teniendo en cuenta el MERGE que hicimos ayer... Sería bueno tener en cuenta eso y quedarnos solo con los ORDER_ID más recientes.**

Os dejamos un atajo a la documentación https://docs.snowflake.com/en/user-guide/dynamic-tables-tasks-create

### b) Capa Gold

Finalmente, la etapa Gold también necesita de una tabla dinámica. Estas tabla se actualizará cada minuto. 

```sql
        SELECT 
            TO_DATE(CREATED_AT),
            STATUS,
            COUNT(DISTINCT ORDER_ID)
        FROM 
            SILVER.DT_ORDERS
        GROUP BY    
            TO_DATE(CREATED_AT),
            STATUS;
```

## 3. Tarea extra

¡Tu equipo no te deja que descanses 🥵! El negocio tiene claro el dato que le gustaría consultar en un cuadro de mando, y para el que tú deberías de preparar dos agrupados que ya precalculen esta información. Cada uno de ellos también serán una tabla dinámica.

1. Suma de Shipping_Cost agrupada por Código Postal: Deberás crear una tabla dinámica que realice este cálculo.
2. Suma de Order_Cost por Nombre de Producto dónde el estado de la orden sea "shipped”: Igualmente, deberás crear una tabla dinámica para este cálculo.

</br>
</br>

**SUSPENDED TODAS LAS TABLAS DINÁMICAS UNA VEZ SE HAYA TERMINADO EL EJERCICIO:**
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()) WHERE DATABASE_NAME = '' order by DATA_TIMESTAMP desc; 
ALTER DYNAMIC TABLE SILVER.DT_ORDERS SUSPEND;
ALTER DYNAMIC TABLE GOLD.DT_ORDERS_STATUS SUSPEND;
ALTER DYNAMIC TABLE GOLD.DT_AGGR_SHIP_COST_CP SUSPEND;
ALTER DYNAMIC TABLE GOLD.DT_AGGR_ORDER_COST_PRO SUSPEND;
```
# Snowpark

Snowpark proporciona una biblioteca intuitiva para consultar y procesar datos a escala en Snowflake. Mediante una biblioteca, puedes crear aplicaciones que procesen datos en Snowflake **sin mover los datos al sistema donde se ejecuta el código de su aplicación**.

Snowflake proporciona actualmente bibliotecas Snowpark para tres lenguajes: **Java, Python y Scala**.

En este caso, vamos a poner un ejemplo con Python, y ¿por qué con Python? Porque Snowflake nos permite utilizar un Worksheet con Python directamente, sin tener que instalar nada y desde la propia interfaz de Snowflake:

![Untitled (4)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/d1ffc4b4-a805-422a-a23c-584492db42e1)


# **1. Configuración inicial**

Vamos a hacer un clone de nuestra tabla de orders para poder modificarla sin problema:

```sql
CREATE OR REPLACE TABLE BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS_COPY 
  CLONE BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS;
```

# **2. Creación y visualización de un dataframe**

Primero, crearemos un dataframe simple desde una tabla existente en tu base de datos y veremos cómo mostrar los resultados.

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session): 
    # Seleccionamos la tabla desde tu esquema y base de datos
    df = session.table("BASE_DE_DATOS_ALUMNO.BRONZE.ORDERS_COPY")
    return df
```

Para mostrar los resultados, podemos verlos también desde la terminal. Esto se hará seleccionando que la salida sea un String y usando la función print de python:

![Untitled (3)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/8d92bd53-3489-4117-99b9-c8cbff135b11)


```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    # Creamos un dataframe
    df = session.table("ORDERS_COPY")
    print("df.show(10) resultado")
    print(df.show(10))
    print('')
    print('___________________________')
    print('___________________________')
    print("df.count() resultado")
    print(df.count())
    return df
```

Para que nos vuelva a salir como tabla en la pestaña de “Results” tenemos que volver a selecciona en Settings>Return type>`Table()` :

![Untitled (2)](https://github.com/javipo84/Curso_Snowflake/assets/166698078/18976eb7-97df-45cb-98a1-c75cbd3e5144)


# 3. Transformación de los datos

Con Python podemos escoger las columnas que queremos consultar dentro del dataframe creado:

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Seleccionamos algunas columnas de interés
    df = df.select("ORDER_ID", "CUSTOMER_ID", "ORDER_STATUS", "ORDER_DATE").limit(10)
    return df
```

## a) Filtrado de filas

Podemos filtrar por filas:

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Filtramos las órdenes con estado 4
    df = df.filter(f.col("ORDER_STATUS") == 4).limit(10)
    return df

```

## **b) Inserción de nuevas columnas**

También podemos agregar una columna que indique el número de días que tardó el envío desde la fecha de pedido hasta la fecha de envío.

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Calculamos los días entre el pedido y el envío
    df = df.with_column("SHIPPING_DELAY", f.datediff("SHIPPED_DATE", "ORDER_DATE"))
    return df

```

## c) Agregación de datos

Nuestros datos también pueden ser agrupados como si estuviésemos haciendo un `GROUP BY` en SQL pero en este caso con Python:

```sql
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Agrupamos por STORE_ID y realizamos varias agregaciones en ORDER_TOTAL
    grouped_data = df.groupBy("STORE_ID").agg(
        f.sum("ORDER_TOTAL").as_("TOTAL_SALES"),
        f.avg("ORDER_TOTAL").as_("AVERAGE_SALES"),
        f.min("ORDER_TOTAL").as_("MIN_SALES"),
        f.max("ORDER_TOTAL").as_("MAX_SALES"),
        f.count("*").as_("COUNT_ORDERS")
    )

    return grouped_data
```

## d) **Uniones entre tablas**

Supongamos que queremos unir la tabla **`orders`** con **`customers`** donde queremos mantener todos los registros de **`orders`** y solo los datos coincidentes de **`customers`**.

```sql
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.use_database('BASE_DE_DATOS_ALUMNO')
    session.use_schema('BRONZE')

    df_orders = session.table("ORDERS_COPY")
    df_customers = session.table("customers")
    # Realizamos un left join
    left_join_data = df_orders.join(df_customers, df_orders["CUSTOMER_ID"] == df_customers["CUSTOMER_ID"], "left")
    
    return left_join_data
```

## **e) Ejercicio: creación de una columna de correo electrónico para el cliente**

Generemos una dirección de correo electrónico ficticia para el cliente utilizando su **`CUSTOMER_ID`**.

- Solución:

```sql 
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as f
from snowflake.snowpark.types import StringType

    def main(session: snowpark.Session):
        session.use_database('BASE_DE_DATOS_ALUMNO')
        session.use_schema('BRONZE')

    df = session.table("ORDERS_COPY")
    # Creamos una dirección de email ficticia para el cliente
    df = df.with_column('CUSTOMER_EMAIL', f.concat(f.cast(f.col("CUSTOMER_ID"), StringType()), f.lit('@example.com')))
    return df 
```
