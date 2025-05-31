drop procedure if exists load_cdm_fact_weather_daily;
drop procedure if exists load_cdm_fact_weather;
drop procedure if exists load_cdm_dim_city;
drop procedure if exists load_cdm_dim_weather_condition;
drop procedure if exists load_dds_weather;
drop procedure if exists load_dds_cities;
drop procedure if exists load_dds_weather_conditions;

drop table if exists cdm_fact_weather_daily;
drop table if exists cdm_fact_weather;
drop table if exists cdm_dim_city;
drop table if exists cdm_dim_weather_condition;
drop table if exists dds_weather;
drop table if exists dds_cities;
drop table if exists dds_weather_conditions;
drop table if exists stg_weather;


----------------------------------------------------------
-- STG TABLES
----------------------------------------------------------
create table stg_weather
(
	coord_lon               decimal(10, 4),
	coord_lat               decimal(10, 4),
	weather_id              int,
	weather_main            varchar(100),
	weather_description     varchar(500),
	weather_icon            char(3),
	main_temp               decimal(10, 4),
	main_feels_like         decimal(10, 4),
	main_temp_min           decimal(10, 4),
	main_temp_max           decimal(10, 4),
	main_pressure           decimal(10, 4),
	main_humidity           decimal(10, 4),
	main_sea_level          decimal(10, 4),
	main_grnd_level         decimal(10, 4),
	visibility              decimal(10, 4),
	wind_speed              decimal(10, 4),
	wind_deg                decimal(10, 4),
	wind_gust               decimal(10, 4),
	clouds_all              decimal(10, 4),
	rain_1h                 decimal(10, 4),
	snow_1h                 decimal(10, 4),
	dt                      int,
	sys_country             char(2),
	sys_sunrise             int,
	sys_sunset              int,
	timezone                int,
	city_id                 int,
	city_name               varchar(100)
);

----------------------------------------------------------
-- DDS TABLES
----------------------------------------------------------
create table dds_cities
(
	city_id         int,
	city_name       varchar(100),
	country_code    varchar(5),
	longitude       decimal(10, 4),
	latitude        decimal(10, 4),
	timezone        int,
	constraint pk_dds_cities primary key (city_id)
);

create table dds_weather_conditions
(
	weather_condition_id            int,
	weather_condition_name          varchar(500),
	main_weather_condition_name     varchar(100),
	constraint pk_dds_weather_conditions primary key (weather_condition_id)
);

create table dds_weather
(
	weather_id              bigint             generated always as identity,
	city_id                 int,
	weather_condition_id    int,
	datetime_utc            timestamp,
	datetime_unix           int,
	temperature             decimal(10, 4),
	temperature_feels_like  decimal(10, 4),
	temperature_min         decimal(10, 4),
	temperature_max         decimal(10, 4),
	pressure                decimal(10, 4),
	pressure_sea_level      decimal(10, 4),
	pressure_ground_level   decimal(10, 4),
	humidity                decimal(10, 4),
	visibility              decimal(10, 4),
	wind_speed              decimal(10, 4),
	wind_direction          decimal(10, 4),
	wind_gust               decimal(10, 4),
	cloudiness              decimal(10, 4),
	precipitation_rain      decimal(10, 4),
	precipitation_snow      decimal(10, 4),
	sunrise_datetime_utc    timestamp,
	sunrise_datetime_unix   int,
	sunset_datetime_utc     timestamp,
	sunset_datetime_unix    int,
	constraint pk_dds_weather primary key (weather_id),
	constraint fk_dds_weather_dds_cities foreign key (city_id) references dds_cities (city_id),
	constraint fk_dds_weather_dds_weather_conditions foreign key (weather_condition_id) references dds_weather_conditions (weather_condition_id)
);

----------------------------------------------------------
-- CDM TABLES
----------------------------------------------------------
create table cdm_dim_city
(
	city_id         int,
	city_name       varchar(100),
	country_code    varchar(5),
	longitude       decimal(10, 4),
	latitude        decimal(10, 4),
	utc_offset      int,
	constraint pk_cdm_dim_city primary key (city_id)
);

create table cdm_dim_weather_condition
(
	weather_condition_id            int,
	weather_condition_name          varchar(500),
	main_weather_condition_name     varchar(100),
	constraint pk_cdm_dim_weather_condition primary key (weather_condition_id)
);

create table cdm_fact_weather
(
	weather_id              bigint,
	city_id                 int,
	weather_condition_id    int,
	datetime_local          timestamp,
	temperature             decimal(10, 4),
	temperature_feels_like  decimal(10, 4),
	temperature_min         decimal(10, 4),
	temperature_max         decimal(10, 4),
	pressure                decimal(10, 4),
	pressure_sea_level      decimal(10, 4),
	pressure_ground_level   decimal(10, 4),
	humidity                decimal(10, 4),
	visibility              decimal(10, 4),
	wind_speed              decimal(10, 4),
	wind_direction          decimal(10, 4),
	wind_gust               decimal(10, 4),
	cloudiness              decimal(10, 4),
	precipitation_rain      decimal(10, 4),
	precipitation_snow      decimal(10, 4),
	sunrise_datetime_local  timestamp,
	sunset_datetime_local   timestamp,
	constraint pk_cdm_fact_weather primary key (weather_id),
	constraint fk_cdm_fact_weather_cdm_dim_city foreign key (city_id) references cdm_dim_city (city_id),
	constraint fk_cdm_fact_weather_cdm_dim_weather_condition foreign key (weather_condition_id) references cdm_dim_weather_condition (weather_condition_id)
);

create table cdm_fact_weather_daily
(
	weather_daily_id                bigint             generated always as identity,
	city_id                         int,
	date_local                      date,
	temperature_daily               decimal(10, 4),
	temperature_feels_like_daily    decimal(10, 4),
	temperature_min_daily           decimal(10, 4),
	temperature_max_daily           decimal(10, 4),
	pressure_daily                  decimal(10, 4),
	pressure_sea_level_daily        decimal(10, 4),
	pressure_ground_level_daily     decimal(10, 4),
	humidity_daily                  decimal(10, 4),
	visibility_daily                decimal(10, 4),
	wind_speed_daily                decimal(10, 4),
	wind_direction_daily            decimal(10, 4),
	wind_gust_daily                 decimal(10, 4),
	cloudiness_daily                decimal(10, 4),
	precipitation_rain_daily        decimal(10, 4),
	precipitation_snow_daily        decimal(10, 4),
	sunrise_datetime_local          timestamp,
	sunset_datetime_local           timestamp,
	constraint pk_cdm_fact_weather_daily primary key (weather_daily_id),
	constraint fk_cdm_fact_weather_daily_cdm_dim_city foreign key (city_id) references cdm_dim_city (city_id)
);


----------------------------------------------------------
-- DDS PROCEDURES
----------------------------------------------------------
create or replace procedure load_dds_cities()
language plpgsql
as $$
begin
	merge into dds_cities C
	using
	(
		select
			city_id,
			city_name,
			sys_country,
			coord_lon,
			coord_lat,
			timezone
		from stg_weather
	) STG on STG.city_id = C.city_id
	when matched then
		update set
			city_name    = STG.city_name,
			country_code = STG.sys_country,
			longitude    = STG.coord_lon,
			latitude     = STG.coord_lat,
			timezone     = STG.timezone
	when not matched then
		insert (city_id,     city_name,     country_code,    longitude,     latitude,      timezone)
		values (STG.city_id, STG.city_name, STG.sys_country, STG.coord_lon, STG.coord_lat, STG.timezone);
end;$$;

create or replace procedure load_dds_weather_conditions()
language plpgsql
as $$
begin
	merge into dds_weather_conditions WC
	using
	(
		select distinct
			weather_id,
			weather_description,
			weather_main
		from stg_weather
	) STG on STG.weather_id = WC.weather_condition_id
	when matched then
		update set
			weather_condition_name      = STG.weather_description,
			main_weather_condition_name = STG.weather_main
	when not matched then
		insert (weather_condition_id, weather_condition_name,  main_weather_condition_name)
		values (STG.weather_id,       STG.weather_description, STG.weather_main);
end;$$;

create or replace procedure load_dds_weather()
language plpgsql
as $$
begin
	insert into dds_weather
	(
		city_id,
		weather_condition_id,
		datetime_utc,
		datetime_unix,
		temperature,
		temperature_feels_like,
		temperature_min,
		temperature_max,
		pressure,
		pressure_sea_level,
		pressure_ground_level,
		humidity,
		visibility,
		wind_speed,
		wind_direction,
		wind_gust,
		cloudiness,
		precipitation_rain,
		precipitation_snow,
		sunrise_datetime_utc,
		sunrise_datetime_unix,
		sunset_datetime_utc,
		sunset_datetime_unix
	)
	select
		STG.city_id,
		STG.weather_id,
		to_timestamp(STG.dt) at time zone 'UTC',
		STG.dt,
		STG.main_temp,
		STG.main_feels_like,
		STG.main_temp_min,
		STG.main_temp_max,
		STG.main_pressure,
		STG.main_sea_level,
		STG.main_grnd_level,
		STG.main_humidity,
		STG.visibility,
		STG.wind_speed,
		STG.wind_deg,
		STG.wind_gust,
		STG.clouds_all,
		STG.rain_1h,
		STG.snow_1h,
		to_timestamp(STG.sys_sunrise) at time zone 'UTC',
		STG.sys_sunrise,
		to_timestamp(STG.sys_sunset) at time zone 'UTC',
		STG.sys_sunset
	from stg_weather      STG
	left join dds_weather W   on W.city_id = STG.city_id and W.datetime_unix = STG.dt
	where W.city_id is null;
end;$$;

----------------------------------------------------------
-- CDM PROCEDURES
----------------------------------------------------------
create or replace procedure load_cdm_dim_city()
language plpgsql
as $$
begin
	merge into cdm_dim_city C
	using
	(
		select
			city_id,
			city_name,
			country_code,
			longitude,
			latitude,
			timezone / 60 / 60 as utc_offset
		from dds_cities
	) DDS_C on DDS_C.city_id = C.city_id
	when matched then
		update set
			city_name    = DDS_C.city_name,
			country_code = DDS_C.country_code,
			longitude    = DDS_C.longitude,
			latitude     = DDS_C.latitude,
			utc_offset   = DDS_C.utc_offset
	when not matched then
		insert (city_id,       city_name,       country_code,       longitude,       latitude,       utc_offset)
		values (DDS_C.city_id, DDS_C.city_name, DDS_C.country_code, DDS_C.longitude, DDS_C.latitude, DDS_C.utc_offset);
end;$$;

create or replace procedure load_cdm_dim_weather_condition()
language plpgsql
as $$
begin
	merge into cdm_dim_weather_condition WC
	using
	(
		select
			weather_condition_id,
			weather_condition_name,
			main_weather_condition_name
		from dds_weather_conditions
	) DDS_WC on DDS_WC.weather_condition_id = WC.weather_condition_id
	when matched then
		update set
			weather_condition_name      = DDS_WC.weather_condition_name,
			main_weather_condition_name = DDS_WC.main_weather_condition_name
	when not matched then
		insert (weather_condition_id,        weather_condition_name,        main_weather_condition_name)
		values (DDS_WC.weather_condition_id, DDS_WC.weather_condition_name, DDS_WC.main_weather_condition_name);
end;$$;

create or replace procedure load_cdm_fact_weather(cdm_dag_schedule_hour_utc int)
language plpgsql
as $$
begin
	insert into cdm_fact_weather
	(
		weather_id,
		city_id,
		weather_condition_id,
		datetime_local,
		temperature,
		temperature_feels_like,
		temperature_min,
		temperature_max,
		pressure,
		pressure_sea_level,
		pressure_ground_level,
		humidity,
		visibility,
		wind_speed,
		wind_direction,
		wind_gust,
		cloudiness,
		precipitation_rain,
		precipitation_snow,
		sunrise_datetime_local,
		sunset_datetime_local
	)
	select
		DDS_W.weather_id,
		DDS_W.city_id,
		DDS_W.weather_condition_id,
		to_timestamp(DDS_W.datetime_unix + DDS_C.timezone) at time zone 'UTC',
		DDS_W.temperature,
		DDS_W.temperature_feels_like,
		DDS_W.temperature_min,
		DDS_W.temperature_max,
		DDS_W.pressure,
		DDS_W.pressure_sea_level,
		DDS_W.pressure_ground_level,
		DDS_W.humidity,
		DDS_W.visibility,
		DDS_W.wind_speed,
		DDS_W.wind_direction,
		DDS_W.wind_gust,
		DDS_W.cloudiness,
		DDS_W.precipitation_rain,
		DDS_W.precipitation_snow,
		to_timestamp(DDS_W.sunrise_datetime_unix + DDS_C.timezone) at time zone 'UTC',
		to_timestamp(DDS_W.sunset_datetime_unix + DDS_C.timezone) at time zone 'UTC'
	from dds_weather           DDS_W
	inner join dds_cities      DDS_C on DDS_C.city_id = DDS_W.city_id
	left join cdm_fact_weather W     on W.weather_id  = DDS_W.weather_id
	where   case cdm_dag_schedule_hour_utc
				when 17 then (DDS_C.timezone / 60 / 60) between 7 and 12 -- города с часовой зоной от UTC+7 до UTC+12
				when 22 then (DDS_C.timezone / 60 / 60) between 2 and 6  -- города с часовой зоной от UTC+2 до UTC+6
	        end
		and to_timestamp(DDS_W.datetime_unix + DDS_C.timezone) at time zone 'UTC' >= cast(cast(now() at time zone concat('Etc/GMT', case when (DDS_C.timezone / 60 / 60) > 0 then '-' else '+' end, abs(DDS_C.timezone / 60 / 60)) as date) as timestamp) - interval '1 day'
		and to_timestamp(DDS_W.datetime_unix + DDS_C.timezone) at time zone 'UTC' <  cast(cast(now() at time zone concat('Etc/GMT', case when (DDS_C.timezone / 60 / 60) > 0 then '-' else '+' end, abs(DDS_C.timezone / 60 / 60)) as date) as timestamp)
		and W.weather_id is null;
end;$$;

create or replace procedure load_cdm_fact_weather_daily(cdm_dag_schedule_hour_utc int)
language plpgsql
as $$
begin
	insert into cdm_fact_weather_daily
	(
		city_id,
		date_local,
		temperature_daily,
		temperature_feels_like_daily,
		temperature_min_daily,
		temperature_max_daily,
		pressure_daily,
		pressure_sea_level_daily,
		pressure_ground_level_daily,
		humidity_daily,
		visibility_daily,
		wind_speed_daily,
		wind_direction_daily,
		wind_gust_daily,
		cloudiness_daily,
		precipitation_rain_daily,
		precipitation_snow_daily,
		sunrise_datetime_local,
		sunset_datetime_local
	)
	select
		W.city_id,
		cast(W.datetime_local as date),
		avg(W.temperature),
		avg(W.temperature_feels_like),
		avg(W.temperature_min),
		avg(W.temperature_max),
		avg(W.pressure),
		avg(W.pressure_sea_level),
		avg(W.pressure_ground_level),
		avg(W.humidity),
		avg(W.visibility),
		avg(W.wind_speed),
		avg(W.wind_direction),
		avg(W.wind_gust),
		avg(W.cloudiness),
		avg(W.precipitation_rain),
		avg(W.precipitation_snow),
		min(W.sunrise_datetime_local),
		min(W.sunset_datetime_local)
	from cdm_fact_weather            W
	inner join cdm_dim_city          C  on C.city_id  = W.city_id
	left join cdm_fact_weather_daily WD on WD.city_id = W.city_id and WD.date_local = cast(W.datetime_local as date)
	where   case cdm_dag_schedule_hour_utc
				when 17 then C.utc_offset between 7 and 12 -- города с часовой зоной от UTC+7 до UTC+12
				when 22 then C.utc_offset between 2 and 6  -- города с часовой зоной от UTC+2 до UTC+6
	        end
		and W.datetime_local >= cast(cast(now() at time zone concat('Etc/GMT', case when C.utc_offset > 0 then '-' else '+' end, abs(C.utc_offset)) as date) as timestamp) - interval '1 day'
		and WD.city_id is null
	group by
		W.city_id,
		cast(W.datetime_local as date);
end;$$;