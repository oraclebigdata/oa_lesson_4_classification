
CREATE EXTERNAL TABLE IF NOT EXISTS historic_weather(ncdc string)
    PARTITIONED BY (yr string)                         
    STORED AS TEXTFILE                                 
    LOCATION '/user/oracle/weather/historic';     

ALTER TABLE historic_weather ADD IF NOT EXISTS PARTITION (yr='2010') location '/user/oracle/weather/historic/yr=2010';
ALTER TABLE historic_weather ADD IF NOT EXISTS PARTITION (yr='2011') location '/user/oracle/weather/historic/yr=2011';

#DROP TABLE clean_weather;


CREATE TABLE IF NOT EXISTS clean_weather AS SELECT w.stn, w.wban, w.weather_year, w.weather_month,
w.weather_day, w.temp, w.dewp, w.weather FROM (
  FROM historic_weather SELECT TRANSFORM(ncdc) USING '/mnt/shared/decision_tree_bootcamp/ncdc_parser.py' as stn, wban, weather_year, weather_month, weather_day, temp, dewp, weather
  ) w;

set hive.cli.print.header = true;
SELECT * FROM clean_weather TABLESAMPLE(10 PERCENT) s WHERE weather_year='2010';

#INSERT OVERWRITE DIRECTORY '/user/oracle/weather/cleaned_history/2010' 
#SELECT * FROM clean_weather WHERE weather_year='2010' and stn != 'stn';

#INSERT OVERWRITE DIRECTORY '/user/oracle/weather/cleaned_history/2011' 
#SELECT * FROM clean_weather WHERE weather_year='2011' and stn != 'stn';