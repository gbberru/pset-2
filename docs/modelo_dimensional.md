# Modelo dimensional documentado

## 1. Descripción general

La capa `clean` fue modelada con un **esquema estrella** para analizar viajes de **NY Taxi 2025**. El modelo toma como punto de partida la tabla staging `clean.stg_trip_2025`, donde los 12 meses de la capa `raw` ya han sido unificados, limpiados y estandarizados. A partir de esa staging se construyen las dimensiones descriptivas y la tabla de hechos principal.

El diseño separa dos tipos de información:

- **atributos descriptivos**, almacenados en dimensiones
- **métricas del viaje**, almacenadas en la tabla de hechos

Esta organización facilita consultas analíticas, agregaciones y cruces por proveedor, forma de pago y ubicaciones.

---

## 2. Tabla de hechos

### Nombre de la tabla de hechos
- `clean.fact_trip`

### Propósito
Registrar cada viaje limpio como un hecho analítico individual.

### Granularidad de la fact table
- **1 fila en `clean.fact_trip` = 1 viaje limpio de taxi del año 2025**

### Columnas principales
- `trip_key`
- `trip_date`
- `pickup_datetime`
- `dropoff_datetime`
- `vendor_key`
- `payment_type_key`
- `pickup_location_key`
- `dropoff_location_key`
- `trip_count`
- `trip_distance`
- `fare_amount`
- `tip_amount`
- `tolls_amount`
- `total_amount`
- `trip_duration_minutes`

### Métricas principales
- `trip_count`
- `trip_distance`
- `fare_amount`
- `tip_amount`
- `tolls_amount`
- `total_amount`
- `trip_duration_minutes`

---

## 3. Dimensiones del modelo

### Dimensiones construidas
- `clean.dim_vendor`
- `clean.dim_payment_type`
- `clean.dim_pickup_location`
- `clean.dim_dropoff_location`

### Descripción resumida

| Dimensión | Clave primaria | Clave de negocio | Atributo descriptivo | Propósito |
|---|---|---|---|---|
| `clean.dim_vendor` | `vendor_key` | `vendor_id` | `vendor_name` | Describe el proveedor del viaje. |
| `clean.dim_payment_type` | `payment_type_key` | `payment_type_id` | `payment_type_name` | Describe la forma de pago. |
| `clean.dim_pickup_location` | `pickup_location_key` | `pickup_location_id` | `pickup_location_name` | Describe la ubicación de recogida. |
| `clean.dim_dropoff_location` | `dropoff_location_key` | `dropoff_location_id` | `dropoff_location_name` | Describe la ubicación de destino. |

---

## 4. Tabla staging de apoyo

### Nombre
- `clean.stg_trip_2025`

### Función
La tabla staging no es una dimensión ni una tabla de hechos final. Su función es:

- unificar los 12 meses provenientes de `raw`
- limpiar registros inválidos o inconsistentes
- estandarizar nombres y tipos de datos
- preparar los campos requeridos por dimensiones y hechos

### Campos relevantes
- `trip_hash`
- `vendor_id`
- `pickup_datetime`
- `dropoff_datetime`
- `pickup_location_id`
- `dropoff_location_id`
- `payment_type_id`
- `trip_distance`
- `fare_amount`
- `tip_amount`
- `tolls_amount`
- `total_amount`
- `trip_date`
- `trip_duration_minutes`

---

## 5. Claves primarias y foráneas

### Claves primarias
- `fact_trip.trip_key`
- `dim_vendor.vendor_key`
- `dim_payment_type.payment_type_key`
- `dim_pickup_location.pickup_location_key`
- `dim_dropoff_location.dropoff_location_key`

### Claves foráneas lógicas del modelo
- `fact_trip.vendor_key` → `dim_vendor.vendor_key`
- `fact_trip.payment_type_key` → `dim_payment_type.payment_type_key`
- `fact_trip.pickup_location_key` → `dim_pickup_location.pickup_location_key`
- `fact_trip.dropoff_location_key` → `dim_dropoff_location.dropoff_location_key`

> Estas relaciones se documentan como parte del **modelo dimensional y de la lógica analítica** observada en la capa `clean`.

---

## 6. Breve explicación del diseño

Se eligió una **estructura tipo estrella** porque simplifica la consulta analítica, separa claramente descriptores y métricas, mejora la legibilidad del modelo y facilita agregaciones por proveedor, forma de pago y ubicación.

La tabla `fact_trip` concentra las medidas cuantitativas del viaje. Las dimensiones almacenan atributos descriptivos reutilizables. La tabla `stg_trip_2025` funciona como etapa intermedia de depuración y normalización antes de poblar el modelo dimensional final.

---

## 7. Resumen ejecutivo del modelo

### Tabla de hechos
- `clean.fact_trip`

### Dimensiones
- `clean.dim_vendor`
- `clean.dim_payment_type`
- `clean.dim_pickup_location`
- `clean.dim_dropoff_location`

### Tabla staging
- `clean.stg_trip_2025`

### Granularidad
- 1 fila = 1 viaje limpio de taxi en 2025

### Claves foráneas lógicas
- `vendor_key`
- `payment_type_key`
- `pickup_location_key`
- `dropoff_location_key`
