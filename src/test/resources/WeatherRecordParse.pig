rawWeather = LOAD '$input' USING TextLoader()  as (s : CHARARRAY);
cleanWeather = FOREACH rawWeather GENERATE FLATTEN(barrett.udf.SimpleWeatherParser(s));
