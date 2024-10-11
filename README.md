# PIDS
# Documentación del despliegue de aplicación con Docker, PostgreSQL, Airflow y Superset

Este documento describe los pasos necesarios para poner en funcionamiento tu entorno completo de análisis de datos de viajes en taxi, utilizando Docker Compose, PostgreSQL, Apache Airflow y Apache Superset.

## **Requisitos previos**
- Tener Docker y Docker Compose instalados en el sistema.
- Haber comprobado los permisos en las carpetas necesarias.

## **Pasos para el Despliegue**

### 1. **Levantar los contenedores con Docker Compose**
   Ejecuta el siguiente comando para iniciar todos los contenedores definidos en el archivo `docker-compose.yml`:

   ```bash
   docker compose up -d
   ```

   Esto levantará los contenedores de PostgreSQL, Airflow (webserver y scheduler) y Superset en modo "detached".

### 2. **Verificar la Base de Datos en PostgreSQL**
   Conéctate al contenedor de PostgreSQL para verificar que la base de datos y las tablas se hayan creado correctamente:

   ```bash
   docker exec -ti pids-postgres-1 bash
   ```

   Dentro de la consola del contenedor, conéctate a la base de datos `taxi_data`:

   ```bash
   psql -U airflow -d taxi_data
   ```

   Asegúrate de que las tablas `raw_data` y `analysis_results` se hayan creado correctamente. Estas tablas se generan al iniciar el contenedor, gracias al script `init.sql`.

   ```sql
   \d
   ```

### 3. **Verificar que las tablas estén vacías**
   Podemos realizar una consulta simple para verificar que la tabla `raw_data` está vacía (esto es lo esperado, ya que aún no se han cargado datos):

   ```sql
   SELECT * FROM raw_data;
   ```

   La consulta debería devolver un resultado vacío.

### 4. **Acceder a Apache Superset**
   Abre un navegador web y ve a `http://localhost:8088`. Esto te llevará a la interfaz de Apache Superset.

   - **Usuario**: `admin`
   - **Contraseña**: `admin`

   Una vez dentro de Superset, navega al dashboard que fue importado automáticamente durante el inicio de los contenedores. En este punto, el dashboard no tendrá datos, ya que la base de datos aún está vacía.

### 5. **Acceder a Apache Airflow**
   Abre un navegador web y ve a `http://localhost:8080`. Esto te llevará a la interfaz de Apache Airflow.
   
   **Importante:** Para que lo siguiente funcione debe haber algún csv en el directorio `data`. En este caso, hay generada una carpeta `csv_por_fecha` en el que están una serie de archivos csv que simulan los datos de cada día durante un mes. Simplemente se puede copiar (uno o varios) a la carpeta `data` antes de ejecutar el DAG.

   - **Usuario**: `admin`
   - **Contraseña**: `admin`

   Dentro de Airflow:
   - Ve a la pestaña de "DAGs".
   - Busca el DAG llamado `load_csv_to_postgres` (importado automáticamente gracias al volumen compartido).
   - Ejecuta el DAG manualmente (trigger). En un escenario real se podría modificar el DAG para ejecutarse diariamente.

   Si el DAG se ejecuta correctamente, los siguientes pasos ocurrirán automáticamente:
   - Los archivos CSV en la carpeta `data` se cargan en la tabla `raw_data` de PostgreSQL.
   - Cada archivo CSV se procesa, y los resultados del análisis se almacenan en la tabla `analysis_results`.
   - Los archivos CSV procesados se eliminan de la carpeta `data`.

### 6. **Verificar los Datos en Apache Superset**
   Después de que los datos hayan sido cargados por Airflow, regresa a Superset (`http://localhost:8088`):
   - Navega al dashboard importado.
   - Verifica que los gráficos ahora muestran datos, ya que las tablas `raw_data` y `analysis_results` contienen información procesada.

En este punto, tu entorno completo está funcionando correctamente, y los dashboards de Superset deberían reflejar los datos analizados y cargados por Airflow. ¡Disfruta de tu entorno de análisis de datos en tiempo real! 

### 7. **Acceder al chatbot**
El fichero ya cuenta con una carpeta `models` en la que se encuentra el modelo ya entrenado y al levantar el docker ya se ejecuta auntomáticamente. Para hacer uso del mismo solo bastará con acceder a la dirección `http://localhost:8088`:

---

## **Notas Adicionales**
- Si encuentras algún error durante el proceso, revisa los logs de cada contenedor con `docker logs <nombre_del_contenedor>`.
- El usuario `admin` y la contraseña `admin` se establecen de forma predeterminada durante el despliegue, pero es recomendable cambiarlos para entornos de producción.
- La carpeta `data` debería contener archivos CSV con los datos que deseas cargar y analizar.

Si sigues estos pasos, podrás poner en funcionamiento tu entorno de análisis de datos de viajes en taxi sin problemas.


