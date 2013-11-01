set hive.cli.print.header = true;
SELECT * FROM clean_weather TABLESAMPLE(10 PERCENT) s WHERE weather_year='2010';
