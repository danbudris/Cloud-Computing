from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonTaxi")
    taxilines = sc.textFile(sys.argv[1], 1)
    
#    pointofInterest = sc.textFile(sys.argv[2], 1)
    
    #difference btw map and flatmap
    # map : go each and change
    # flatmap : make a new list
    
    
    # listline [3] = dropoff_datetime
    # listline [8] = dropoff_longitude
    # listline [9] = dropoff_latitude
    
    # filter out file within 8 ~ 11 am
    def morningTime(value):
        date = value.split(' ')
        time = date[1].split(':')
        hour = int(time[0])
        if hour >= 8 and hour < 11:
            return True
        return False
  
    # remove lines if they don't have 16 values
    # also if values are empty.
    def correctFormat(listline):
        if(len(listline) == 17):
            time = listline[3]
            longi = listline[8]
            lati  = listline[9]
            
            if longi and lati and time: #is not empty and
                if longi !=0 and lati != 0: #if value is not 0.
                    if morningTime(time):
                        return listline
    
    def getCellID(lat, lon):
        return (str(round(lat, 2)) + "-"+str(round(lon, 2)))
    
    def getDay(value):
        dateform = value.split(' ')
        date = dateform[0].split('-')
        day = datetime.date(int(date[0]), int(date[1]), int(date[2]))
        return day.weekday()
    
    # 1. filter out
    # 2. get only taxi ID and driver ID
    # 3. distinct items if taxi ID and driver ID are same
    # 4. after distinct, i don't need to care about driver ID anymore. replace 1 with driver ID
    # 5. add up.
    # 6. swap key and value for sorting top
    
    
    filteredTaxi = taxilines.map(lambda x: x.split(',')) \
    .filter(correctFormat) \
    .map(lambda x: (getCellID(float(x[9]), float(x[8])), getDay(x[3]) ) )
    
    newtaxi = filteredTaxi.top(10)
    
print(newtaxi)
