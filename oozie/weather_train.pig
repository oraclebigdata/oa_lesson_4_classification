REGISTER 'c45_udf.py' USING jython AS c45; 
rmf /user/oracle/weather/models
weather_obs = LOAD '/user/oracle/weather/cleaned_history' 
  using PigStorage('\u0001') as                             
  (usaf:int , wban:int, year:int, month:int, day:int, temp:float, dewp:float, weather:chararray);               

stations = LOAD '/user/oracle/weather/stations' USING PigStorage() as 
(stn:int,wban:int,country:chararray,state:chararray,lat:float,lon:float);
observations = JOIN weather_obs BY usaf, stations BY stn using 'replicated';
training_data = FOREACH observations GENERATE state,lat, lon, day,temp,dewp,weather;
training_groups = GROUP training_data BY state;
models = FOREACH training_groups GENERATE c45.build_instances(group, training_data);
STORE models INTO '/user/oracle/weather/models' USING PigStorage();
