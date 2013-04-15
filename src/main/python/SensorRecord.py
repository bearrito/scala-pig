from collections import namedtuple
import datetime
import json
import simplejson
import random

class SensorRecord(object):

    def __init__(self,machine_name,channel_1,channel_2,channel_3,mode,record_date):
        self.machine_name = machine_name
        self.channel_1 = channel_1
        self.channel_2 = channel_2
        self.channel_3 = channel_3
        self.mode = mode
        self.record_date = record_date




records = []
with open('/home/bearrito/Git/scala-pig/src/main/data/mapper_input','w+') as f:
    for i in range(10000):
        input = machine_name = "Machine-" + str((i % 3) + 1) + '\t'
        input += str(datetime.datetime.today().replace(second=0, microsecond=0)) + '\t'
        input +=  str(round(random.normalvariate((i % 3),1),4)) + '\t'
        input +=  str(round( random.expovariate(3),4) )+ '\t'
        input +=  str(round(random.lognormvariate(0,2),4)) + '\n'
        f.write(input)
