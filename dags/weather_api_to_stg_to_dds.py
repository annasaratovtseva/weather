# Описание API: https://openweathermap.org/current
# Пример вызова: https://api.openweathermap.org/data/2.5/weather?q=Saratov&units=metric&APPID={API key}


# 1. section imports
import requests
import logging as _log
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

#2. section global vars
URL = "https://api.openweathermap.org/data/2.5/weather"

APPID_VARIABLE_NAME = "openweathermap_appid"

PG_CONN_ID = "pg_weather"

LOAD_STG_SQL = '''
truncate table stg_weather;

insert into stg_weather (
    coord_lon, coord_lat, weather_id, weather_main, weather_description, weather_icon, main_temp, main_feels_like, main_temp_min, main_temp_max, main_pressure, main_humidity,
    main_sea_level, main_grnd_level, visibility, wind_speed, wind_deg, wind_gust, clouds_all, rain_1h, snow_1h, dt, sys_country, sys_sunrise, sys_sunset, timezone, city_id, city_name
)
values
    {x};
'''

LOAD_DDS_SQL = '''
call load_dds_cities();
call load_dds_weather_conditions();
call load_dds_weather();
'''

CITIES = [
    "Kaliningrad",               # UTC+2  (MSK-1)
    "Moscow",                    # UTC+3  (MSK)
    "Saratov", "Arkadak", "Balakovo", "Balashov", "Vol'sk", "Yershov", "Kalininsk", "Krasnoarmeysk", "Krasnyy Kut", # Города Саратовской области: UTC+4  (MSK+1)
    "Marks", "Novouzensk", "Petrovsk", "Pugachev", "Rtishchevo", "Khvalynsk", "Shikhany", "Engels", "Atkarsk",
    "Ekaterinburg",              # UTC+5  (MSK+2)
    "Omsk",                      # UTC+6  (MSK+3)
    "Krasnoyarsk",               # UTC+7  (MSK+4)
    "Irkutsk",                   # UTC+8  (MSK+5)
    "Yakutsk",                   # UTC+9  (MSK+6)
    "Vladivostok",               # UTC+10 (MSK+7)
    "Magadan",                   # UTC+11 (MSK+8)
    "Petropavlovsk-Kamchatskiy"  # UTC+12 (MSK+9)
]

# 3. section def
def request_and_parse_data(**context):
    """
    Получение данных из API и их парсинг
    """
    appid = Variable.get(APPID_VARIABLE_NAME)

    all_data = []

    for city in CITIES:
        params = {"appid": appid, "q": city, "units": "metric"}
        response = requests.get(URL, params=params)
        response_json = response.json()

        _log.info(response_json)

        coord_json = response_json["coord"]
        weather_json = response_json["weather"]
        main_json = response_json["main"]
        visibility = response_json["visibility"]
        wind_json = response_json["wind"]
        wind_gust = wind_json.get("gust", 0)
        clouds_json = response_json["clouds"]
        rain_json = response_json.get("rain", 0)
        match rain_json:
            case 0:
                rain_1h = rain_json
            case _:
                rain_1h = rain_json["1h"]
        snow_json = response_json.get("snow", 0)
        match snow_json:
            case 0:
                snow_1h = snow_json
            case _:
                snow_1h = snow_json["1h"]
        dt = response_json["dt"]
        sys_json = response_json["sys"]
        timezone = response_json["timezone"]
        id = response_json["id"]
        name = response_json["name"].replace("'", "''")

        data = (
            coord_json["lon"], coord_json["lat"],
            weather_json[0]["id"], weather_json[0]["main"], weather_json[0]["description"], weather_json[0]["icon"],
            main_json["temp"], main_json["feels_like"], main_json["temp_min"], main_json["temp_max"], main_json["pressure"], main_json["humidity"], main_json["sea_level"], main_json["grnd_level"],
            visibility,
            wind_json["speed"], wind_json["deg"], wind_gust,
            clouds_json["all"],
            rain_1h,
            snow_1h,            
            dt,
            sys_json["country"], sys_json["sunrise"], sys_json["sunset"],
            timezone,
            id,
            name
        )

        all_data.append(data)
    
    all_data = str(all_data)[1:-1].replace('"', "'")

    context["task_instance"].xcom_push(key="all_data", value=all_data)

def load_data_to_stg(**context):
    """
    Запись распарсенных данных в базу данных в слой STG
    """
    all_data = context["task_instance"].xcom_pull(
        task_ids="request_and_parse_data_task", key="all_data")

    _log.info(all_data)

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(LOAD_STG_SQL.format(x=all_data))

    conn.commit()
    conn.close()

def load_data_to_dds(**context):
    """
    Запись данных в базу данных в слой DDS
    """
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(LOAD_DDS_SQL)

    conn.commit()
    conn.close()

# 4. section dag, task descr
args = {
    "owner": "airflow",
    "catchup": "False",
}

with DAG(
    dag_id="weather_api_to_stg_to_dds",
    default_args=args,
    description="Loading data from OpenWeatherMap API to STG and DDS",
    start_date=datetime(2025, 5, 28, 23, 30, 0),
    schedule_interval="30 * * * *", # Каждый час
    tags=["pg", "API"],
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    request_and_parse_data_task = PythonOperator(
        task_id="request_and_parse_data_task",
        python_callable=request_and_parse_data,
        provide_context=True,
        dag=dag
    )
    load_data_to_stg_task = PythonOperator(
        task_id="load_data_to_stg_task",
        python_callable=load_data_to_stg,
        provide_context=True,
        dag=dag
    )
    load_data_to_dds_task = PythonOperator(
        task_id="load_data_to_dds_task",
        python_callable=load_data_to_dds,
        provide_context=True,
        dag=dag
    )    

    request_and_parse_data_task >> load_data_to_stg_task >> load_data_to_dds_task