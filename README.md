# PSet 2 – NY Taxi 2025

**Pipeline ETL reproducible con Docker Compose, Mage, PostgreSQL y pgAdmin**

## 1\. Objetivo del proyecto

Este proyecto construye un flujo ETL reproducible para cargar y transformar los datos de **NY Taxi Yellow Tripdata 2025**.

El objetivo es implementar dos capas de datos:

* **`raw`**: capa de datos crudos cargados desde archivos `.parquet`, conservando una estructura cercana a la fuente.
* **`clean`**: capa transformada y validada, con tratamiento de nulos, normalización de tipos, eliminación de duplicados, validaciones temporales y un **modelo dimensional** con tabla de hechos y dimensiones.

\---

## 2\. Arquitectura del proyecto

El proyecto se ejecuta localmente con **Docker Compose** y utiliza tres servicios principales:

* **Mage**: orquestación de pipelines ETL
* **PostgreSQL**: almacenamiento de las capas `raw` y `clean`
* **pgAdmin**: consulta y validación visual de resultados

### Flujo general

1. Se descargan **12 archivos `.parquet`** de NY Taxi 2025, uno por mes.
2. Mage carga esos archivos al esquema **`raw`** de PostgreSQL.
3. Mage transforma la información de `raw` hacia el esquema **`clean`**.
4. En `clean` se construye:

   * una tabla staging (`stg\\\_trip\\\_2025`)
   * dimensiones (`dim\\\_vendor`, `dim\\\_payment\\\_type`, `dim\\\_pickup\\\_location`, `dim\\\_dropoff\\\_location`)
   * una tabla de hechos (`fact\\\_trip`)
5. Los resultados se validan en **pgAdmin** y con consultas SQL.

\---

## 3\. Estructura del repositorio

```text
pset-2/
├── data-orquestador/
│   └── orquestador/              # Proyecto Mage
│       ├── data/
│       ├── data\\\_exporters/
│       ├── data\\\_loaders/
│       ├── pipelines/
│       ├── transformers/
│       ├── io\\\_config.yaml
│       └── ...
├── docs/                         # Documentación y entregables
├── notebooks/                    # EDA
├── screenshots/                  # Evidencia visual
├── docker-compose.yaml
└── README.md
```

\---

## 4\. Requisitos previos

Antes de levantar el proyecto, la persona que lo reproduzca debe tener instalado:

* **Docker Desktop**
* **Git**
* Navegador web
* Puerto libre para:

  * **6789** → Mage
  * **5432** → PostgreSQL
  * **9000** → pgAdmin

\---

## 5\. Pasos para levantar el entorno local

### 5.1. Clonar el repositorio

```bash
git clone <URL\\\_DEL\\\_REPOSITORIO>
cd pset-2
```

### 5.2. Levantar los servicios con Docker Compose

Desde la raíz del proyecto, ejecutar:

```bash
docker compose up -d
```

### 5.3. Verificar que los contenedores estén activos

```bash
docker compose ps
```

También se puede verificar desde **Docker Desktop**.

Se deben observar activos los servicios equivalentes a:

* Mage
* PostgreSQL
* pgAdmin

\---

## 6\. Acceso a las herramientas

### 6.1. Mage

Abrir en el navegador:

```text
http://localhost:6789
```

### 6.2. pgAdmin

Abrir en el navegador:

```text
http://localhost:9000
```

### 6.3. PostgreSQL

Si se desea conectar desde un cliente SQL externo:

* **Host**: `localhost`
* **Puerto**: `5432`

> Nota: dentro del entorno Docker, Mage se conecta a PostgreSQL usando el host interno configurado en el proyecto.

\---

## 7\. Configuración manual obligatoria en Mage

Este proyecto **requiere crear secrets manualmente en Mage** para la conexión a PostgreSQL.

### Importante

Si estos secrets no se crean, otra persona podrá levantar los contenedores, pero **no podrá ejecutar correctamente los pipelines**.

### 7.1. Ir a Secrets en Mage

En Mage:

1. abrir el proyecto
2. ir a **Secrets**
3. crear los siguientes secrets con **`+ New`**

### 7.2. Secrets que deben crearse

Crear exactamente estos nombres:

* `pg\\\_user`
* `pg\\\_password`
* `pg\\\_db`
* `pg\\\_port`
* `pg\\\_host`

### 7.3. Valores esperados

Usar los valores que correspondan a la configuración del `docker-compose.yaml`.

Ejemplo de referencia para este proyecto:

```text
pg\\\_host = data-warehouse-1
pg\\\_port = 5432
pg\\\_db = warehouse
pg\\\_user = <usuario\\\_postgres>
pg\\\_password = <password\\\_postgres>
```

> Si el `docker-compose.yaml` del repositorio tiene otros nombres o credenciales, deben respetarse esos valores.

### 7.4. Uso de los secrets

El archivo `io\\\_config.yaml` usa esos secrets para construir la conexión a PostgreSQL.  
Por eso no se deben escribir credenciales reales dentro del código fuente.

\---

## 8\. Cómo ejecutar los pipelines

El proyecto tiene dos pipelines principales:

* **`pipe\\\_raw`**
* **`pipe\\\_clean`**

### 8.1. Pipeline `pipe\\\_raw`

#### Propósito

Cargar los 12 meses de NY Taxi 2025 en la capa `raw`.

#### Bloques

* `extract\\\_data\\\_raw`
* `load\\\_data\\\_raw`
* `trigger\\\_pipe\\\_clean\\\_after\\\_last\\\_raw`

#### Lógica de ejecución

La carga de `raw` se divide en tres bloques de meses, usando tres triggers:

* `raw\\\_2025\\\_m01\\\_m04` → meses 1 a 4
* `raw\\\_2025\\\_m05\\\_m08` → meses 5 a 8
* `raw\\\_2025\\\_m09\\\_m12` → meses 9 a 12

#### Recomendación de ejecución

Ejecutar los triggers en este orden:

1. `raw\\\_2025\\\_m01\\\_m04`
2. `raw\\\_2025\\\_m05\\\_m08`
3. `raw\\\_2025\\\_m09\\\_m12`

Esperar a que cada trigger termine antes de ejecutar el siguiente.

#### Resultado esperado

Al finalizar, el esquema `raw` debe contener las 12 tablas:

* `yellow\\\_tripdata\\\_2025\\\_01`
* `yellow\\\_tripdata\\\_2025\\\_02`
* `yellow\\\_tripdata\\\_2025\\\_03`
* `yellow\\\_tripdata\\\_2025\\\_04`
* `yellow\\\_tripdata\\\_2025\\\_05`
* `yellow\\\_tripdata\\\_2025\\\_06`
* `yellow\\\_tripdata\\\_2025\\\_07`
* `yellow\\\_tripdata\\\_2025\\\_08`
* `yellow\\\_tripdata\\\_2025\\\_09`
* `yellow\\\_tripdata\\\_2025\\\_10`
* `yellow\\\_tripdata\\\_2025\\\_11`
* `yellow\\\_tripdata\\\_2025\\\_12`

### 8.2. Pipeline `pipe\\\_clean`

#### Propósito

Transformar los datos de `raw` hacia una capa analítica `clean`.

#### Bloques

* `load\\\_clean\\\_params`
* `create\\\_stg\\\_trip\\\_2025`
* `create\\\_dimensions`
* `create\\\_fact\\\_trip`

#### Forma de ejecución

Este pipeline debe ejcutarse asi:

**Proceso Automático:**  
Después de ejecutar el último trigger raw (`raw\\\_2025\\\_m09\\\_m12`), el bloque `trigger\\\_pipe\\\_clean\\\_after\\\_last\\\_raw` dispara el trigger:

* `clean\\\_after\\\_raw\\\_2025`

El triger anterior dispara el pipe\_clean

\---

## 9\. Cómo acceder a pgAdmin y validar resultados

Ingresar a:

```text
http://localhost:9000
```

Luego conectarse al servidor PostgreSQL del proyecto y abrir la base de datos:

* **Base**: `warehouse`

Dentro de esta base deben existir los esquemas:

* `raw`
* `clean`

\---

## 10\. Descripción de schemas, tablas y relaciones

### 10.1. Esquema `raw`

Contiene las tablas mensuales crudas cargadas desde la fuente.

#### Tablas

* `yellow\\\_tripdata\\\_2025\\\_01`
* `yellow\\\_tripdata\\\_2025\\\_02`
* `yellow\\\_tripdata\\\_2025\\\_03`
* `yellow\\\_tripdata\\\_2025\\\_04`
* `yellow\\\_tripdata\\\_2025\\\_05`
* `yellow\\\_tripdata\\\_2025\\\_06`
* `yellow\\\_tripdata\\\_2025\\\_07`
* `yellow\\\_tripdata\\\_2025\\\_08`
* `yellow\\\_tripdata\\\_2025\\\_09`
* `yellow\\\_tripdata\\\_2025\\\_10`
* `yellow\\\_tripdata\\\_2025\\\_11`
* `yellow\\\_tripdata\\\_2025\\\_12`

#### Características

* conserva nombres y estructura cercanos a la fuente
* agrega campos técnicos de trazabilidad como año, mes, URL fuente e ingestión

### 10.2. Esquema `clean`

Contiene los datos validados y modelados para análisis.

#### Tablas

* `stg\\\_trip\\\_2025`
* `dim\\\_vendor`
* `dim\\\_payment\\\_type`
* `dim\\\_pickup\\\_location`
* `dim\\\_dropoff\\\_location`
* `fact\\\_trip`

#### Función de cada tabla

**`stg\\\_trip\\\_2025`**  
Tabla staging intermedia para:

* unir la data del año completo
* limpiar duplicados
* estandarizar nombres de columnas
* tipificar campos
* calcular métricas derivadas
* aplicar validaciones mínimas

**`dim\\\_vendor`**  
Dimensión de proveedor del servicio.

**`dim\\\_payment\\\_type`**  
Dimensión de tipo de pago.

**`dim\\\_pickup\\\_location`**  
Dimensión de ubicación de recogida.

**`dim\\\_dropoff\\\_location`**  
Dimensión de ubicación de destino.

**`fact\\\_trip`**  
Tabla de hechos principal del modelo dimensional.

#### Granularidad de `fact\\\_trip`

**1 fila en `fact\\\_trip` = 1 viaje limpio de taxi del año 2025**

### 10.3. Relaciones del modelo

La tabla `fact\\\_trip` se relaciona con las dimensiones mediante:

* `vendor\\\_key` → `dim\\\_vendor.vendor\\\_key`
* `payment\\\_type\\\_key` → `dim\\\_payment\\\_type.payment\\\_type\\\_key`
* `pickup\\\_location\\\_key` → `dim\\\_pickup\\\_location.pickup\\\_location\\\_key`
* `dropoff\\\_location\\\_key` → `dim\\\_dropoff\\\_location.dropoff\\\_location\\\_key`

\---

## 11\. Decisiones de diseño

Las decisiones principales del proyecto fueron las siguientes:

### 11.1. Cargar solo el año 2025

El taller fue desarrollado únicamente para el año 2025, en lugar de 10 años, para reducir consumo de memoria y facilitar la ejecución local.

### 11.2. Dividir la carga raw por bloques de meses

La carga se dividió en tres triggers de 4 meses para evitar sobrecarga de memoria y mejorar el control operativo.

### 11.3. Mantener `raw` cercano a la fuente

La capa `raw` no transforma el negocio; solo organiza la llegada mensual de archivos en PostgreSQL.

### 11.4. Construir una staging limpia antes del modelo dimensional

Se creó `stg\\\_trip\\\_2025` para concentrar:

* limpieza
* validaciones
* deduplicación
* tipificación
* cálculo de duración y fecha

### 11.5. Modelo dimensional simple y claro

Se implementó un esquema tipo estrella con:

* una tabla de hechos: `fact\\\_trip`
* cuatro dimensiones principales

### 11.6. Manejo explícito de nulos en tipo de pago

Los registros sin tipo de pago válido fueron mapeados a la categoría `unknown`, evitando `NULL` en la clave de pago de la tabla de hechos.

\---

## 12\. Validaciones aplicadas en la capa clean

El pipeline `create\\\_stg\\\_trip\\\_2025` demuestra criterio técnico y evita que la capa `clean` sea una copia directa de `raw`.

Entre las validaciones aplicadas están:

* manejo de nulos en campos críticos
* tratamiento de duplicados con `trip\\\_hash`
* normalización de tipos de datos
* estandarización de nombres de columnas
* validación de pickup y dropoff
* filtrado de registros imposibles o erróneos
* cálculo de `trip\\\_duration\\\_minutes`
* conservación de categoría `unknown` para tipos de pago faltantes o inválidos

\---

## 13\. Consultas SQL para validar resultados en PostgreSQL

### 13.1. Validar existencia de schemas

```sql
SELECT schema\\\_name
FROM information\\\_schema.schemata
WHERE schema\\\_name IN ('raw', 'clean')
ORDER BY schema\\\_name;
```

### 13.2. Validar tablas del esquema raw

```sql
SELECT table\\\_schema, table\\\_name
FROM information\\\_schema.tables
WHERE table\\\_schema = 'raw'
ORDER BY table\\\_name;
```

### 13.3. Confirmar total de tablas raw

```sql
SELECT COUNT(\\\*) AS total\\\_tablas\\\_raw
FROM information\\\_schema.tables
WHERE table\\\_schema = 'raw';
```

### 13.4. Validar tablas del esquema clean

```sql
SELECT table\\\_schema, table\\\_name
FROM information\\\_schema.tables
WHERE table\\\_schema = 'clean'
ORDER BY table\\\_name;
```

### 13.5. Confirmar total de tablas clean

```sql
SELECT COUNT(\\\*) AS total\\\_tablas\\\_clean
FROM information\\\_schema.tables
WHERE table\\\_schema = 'clean';
```

### 13.6. Validar staging sin nulos en `payment\\\_type\\\_id`

```sql
SELECT COUNT(\\\*) AS filas\\\_staging\\\_con\\\_payment\\\_type\\\_id\\\_null
FROM clean.stg\\\_trip\\\_2025
WHERE payment\\\_type\\\_id IS NULL;
```

### 13.7. Validar fact sin nulos en `payment\\\_type\\\_key`

```sql
SELECT COUNT(\\\*) AS filas\\\_con\\\_payment\\\_type\\\_key\\\_null
FROM clean.fact\\\_trip
WHERE payment\\\_type\\\_key IS NULL;
```

### 13.8. Conteo de registros en staging

```sql
SELECT COUNT(\\\*) AS total\\\_staging
FROM clean.stg\\\_trip\\\_2025;
```

### 13.9. Conteo de registros en fact

```sql
SELECT COUNT(\\\*) AS total\\\_fact\\\_trip
FROM clean.fact\\\_trip;
```

### 13.10. Conteo por dimensión

```sql
SELECT 'dim\\\_vendor' AS tabla, COUNT(\\\*) AS total FROM clean.dim\\\_vendor
UNION ALL
SELECT 'dim\\\_payment\\\_type', COUNT(\\\*) FROM clean.dim\\\_payment\\\_type
UNION ALL
SELECT 'dim\\\_pickup\\\_location', COUNT(\\\*) FROM clean.dim\\\_pickup\\\_location
UNION ALL
SELECT 'dim\\\_dropoff\\\_location', COUNT(\\\*) FROM clean.dim\\\_dropoff\\\_location;
```

### 13.11. Muestra de datos en la tabla de hechos

```sql
SELECT
    trip\\\_key,
    trip\\\_date,
    pickup\\\_datetime,
    dropoff\\\_datetime,
    vendor\\\_key,
    payment\\\_type\\\_key,
    pickup\\\_location\\\_key,
    dropoff\\\_location\\\_key,
    trip\\\_count,
    trip\\\_distance,
    total\\\_amount
FROM clean.fact\\\_trip
LIMIT 20;
```

### 13.12. Validar granularidad de la fact table

```sql
SELECT
    trip\\\_key,
    trip\\\_date,
    pickup\\\_datetime,
    dropoff\\\_datetime,
    trip\\\_count,
    trip\\\_distance,
    total\\\_amount,
    trip\\\_duration\\\_minutes
FROM clean.fact\\\_trip
LIMIT 20;
```

### 13.13. Consulta analítica con dimensiones

```sql
SELECT
    f.trip\\\_date,
    dv.vendor\\\_name,
    dpt.payment\\\_type\\\_name,
    f.trip\\\_distance,
    f.fare\\\_amount,
    f.tip\\\_amount,
    f.tolls\\\_amount,
    f.total\\\_amount,
    f.trip\\\_duration\\\_minutes
FROM clean.fact\\\_trip f
LEFT JOIN clean.dim\\\_vendor dv
    ON f.vendor\\\_key = dv.vendor\\\_key
LEFT JOIN clean.dim\\\_payment\\\_type dpt
    ON f.payment\\\_type\\\_key = dpt.payment\\\_type\\\_key
LIMIT 20;
```

### 13.14. Resumen por proveedor

```sql
SELECT
    dv.vendor\\\_name,
    COUNT(\\\*) AS total\\\_viajes,
    ROUND(AVG(f.trip\\\_distance), 2) AS distancia\\\_promedio,
    ROUND(AVG(f.total\\\_amount), 2) AS monto\\\_promedio
FROM clean.fact\\\_trip f
LEFT JOIN clean.dim\\\_vendor dv
    ON f.vendor\\\_key = dv.vendor\\\_key
GROUP BY dv.vendor\\\_name
ORDER BY total\\\_viajes DESC;
```

### 13.15. Resumen por tipo de pago

```sql
SELECT
    dpt.payment\\\_type\\\_name,
    COUNT(\\\*) AS total\\\_viajes,
    ROUND(AVG(f.total\\\_amount), 2) AS promedio\\\_total
FROM clean.fact\\\_trip f
LEFT JOIN clean.dim\\\_payment\\\_type dpt
    ON f.payment\\\_type\\\_key = dpt.payment\\\_type\\\_key
GROUP BY dpt.payment\\\_type\\\_name
ORDER BY total\\\_viajes DESC;
```

### 13.16. Agregados generales de la fact table

```sql
SELECT
    COUNT(\\\*) AS total\\\_viajes,
    SUM(trip\\\_count) AS suma\\\_trip\\\_count,
    ROUND(AVG(trip\\\_distance), 2) AS distancia\\\_promedio,
    ROUND(AVG(total\\\_amount), 2) AS monto\\\_promedio,
    ROUND(AVG(trip\\\_duration\\\_minutes), 2) AS duracion\\\_promedio\\\_min
FROM clean.fact\\\_trip;
```

\---

## 14\. Cómo detener el entorno

Para apagar los servicios:

```bash
docker compose down
```

Si se desea reiniciar desde cero:

```bash
docker compose down -v
docker compose up -d
```

> Advertencia: usar `-v` elimina volúmenes y puede borrar la persistencia local.

\---

## 15\. Resultado esperado al reproducir el proyecto

Si otra persona sigue este README correctamente, debe poder:

* levantar Mage, PostgreSQL y pgAdmin localmente
* crear los secrets manuales en Mage
* ejecutar `pipe\\\_raw`
* ejecutar `pipe\\\_clean` o dejar que se dispare al finalizar `raw`
* validar en PostgreSQL la existencia de:

  * 12 tablas en `raw`
  * 6 tablas en `clean`
* comprobar que:

  * `stg\\\_trip\\\_2025` no tiene nulos en `payment\\\_type\\\_id`
  * `fact\\\_trip` no tiene nulos en `payment\\\_type\\\_key`
  * el modelo dimensional funciona correctamente

\---

## 16\. Entregables incluidos en `docs`

La carpeta `docs` contiene los entregables del proyecto, por ejemplo:

* diagrama de arquitectura
* modelo dimensional documentado
* evidencia visual
* otras piezas de soporte del taller

\---

## 17\. Notas finales de solución de problemas

Si al reproducir el proyecto aparecen errores, revisar en este orden:

1. que Docker Desktop esté corriendo
2. que `docker compose up -d` haya levantado los servicios
3. que Mage abra en `http://localhost:6789`
4. que pgAdmin abra en `http://localhost:9000`
5. que los secrets de PostgreSQL hayan sido creados correctamente en Mage
6. que los triggers se ejecuten en el orden correcto
7. que el `io\\\_config.yaml` esté apuntando a los mismos nombres de secret



