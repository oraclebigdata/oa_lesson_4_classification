#! /usr/bin/env python

import sys

for line in sys.stdin:
    if "STN" not in line:
        stn = line[0:6]
        wban = line[7:11]
        year = line[14:18]
        month = line[18:20]
        day = line[20:22]
        temp = line[24:30]
        dewp = line[35:41]
        conditions = line[132:138]
        weather = ""
        if "1" not in conditions:
            weather = "Sunny"
        else:
            if conditions[0] == "1":
                weather = "Fog"
            if conditions[1] == "1":
                weather = "Rain"
            if conditions[2] == "1":
                weather = "Snow"
            if conditions[3] == "1":
                weather = "Hail"
            if conditions[4] == "1":
                weather = "Thunder"
            if conditions[5] == "1":
                weather = "Tornado"
                
            
        
        print "\t".join([stn, wban, year, month, day, temp, dewp, weather])
        
    
    
