
sensors = load '$input' as (machine_name:chararray,record_date:chararray,channel_1:float,channel_2:float,channel_3:float);
sensor_group = GROUP sensors BY machine_name;
grouped_channel_averages = FOREACH sensor_group GENERATE group,AVG(sensors.channel_1), AVG(sensors.channel_2);
STORE grouped_channel_averages INTO '$output';



