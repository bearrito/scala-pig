__author__ = 'me'

import csv
import os

us_stations = []
with open('/home/me/Downloads/ish-history.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')



    for row in reader:
        splits = map(lambda x : x.split(','),row)
        stations = []
        for s in splits:
            stations = stations + s
        state = str(stations[4].strip())
        if state == '"US"':
            id = int(stations[0].strip().replace('"',''))
            print str(id)
            us_stations.append(id)


files_to_delete = []
dir = "/home/me/Downloads/weather"
for f in os.listdir(dir) :
    file_id = int(f.split('-')[0])
    if file_id not in us_stations :
        print file_id
        files_to_delete.append(f)

for file_to_delete in files_to_delete :
    d = dir + "/" + file_to_delete
    os.remove(d)