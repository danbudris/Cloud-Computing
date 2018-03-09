from __future__ import print_function
import sys
import datetime
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonTaxi")
    taxilines = sc.textFile(sys.argv[1], 1)
    
    pointofInterest = sc.textFile(sys.argv[2], 1)
    
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
                if longi !=0.0 and lati != 0.0: #if value is not 0.
                    if morningTime(time):
                        return listline
    
    def getCellID(lat, lon):
        return (str(round(lat, 2)) + " & "+str(round(lon, 2)))
    
    #daytime.
    #weekday() Return the day of the week as an integer, where Monday is 0 and Sunday is 6.
    def getDay(value):
        dateform = value.split(' ')
        date = dateform[0].split('-')
        day = datetime.date(int(date[0]), int(date[1]), int(date[2]))
        return day.weekday()
    
    # value[1] is a day. 
    def isSunday(value):
        if value[1] == 0:
            return value
        
    # 1. filter out
    # 2. get only Cell ID and day
    # 3. 
    # 4. 
    # 5. add up.
    # 6. swap key and value for sorting top
    
    
    filteredTaxi = taxilines.map(lambda x: x.split(',')) \
    .filter(correctFormat) \
    .map(lambda x: (getCellID(float(x[9]), float(x[8])), getDay(x[3]) ) ) \
  
    
    sundayTaxi = filteredTaxi.filter(isSunday) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[1],x[0])) \
    .top(20)
    
    # different way to filter
    weekTaxi = filteredTaxi.filter(lambda x: x[1] != 0) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[1],x[0])) \
    .top(20)
    
    
    


#print(filteredTaxi.collect())
         
print(sundayTaxi)
print(weekTaxi)
