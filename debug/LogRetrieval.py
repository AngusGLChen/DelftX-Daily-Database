'''
Created on Aug 4, 2016

@author: Angus
'''

import json, operator, datetime
from time import *
from translation.Functions import cmp_datetime

# This function is used to retrieve records with respect to a specific learner.
def RetrieveEventLogs(log_path, learner_id):
    
    path = log_path
    input_file = open(path, "r")
    lines = input_file.readlines()
    
    log_array = []
    
    for line in lines:
        jsonObject = json.loads(line)
        
        user_id = str(jsonObject["context"]["user_id"])
        event_type = jsonObject["event_type"]
        
        time = jsonObject["time"]
        time = time[0:19]
        time = time.replace("T", " ")
        time = datetime.datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
        
        if user_id == learner_id:
            log_array.append({"time":time, "event_type":event_type})
            
    log_array.sort(cmp=cmp_datetime, key=operator.itemgetter('time'))
            
    print "Begin"
    for tuple in log_array:
        print str(tuple["time"]) + "\t" + tuple["event_type"]
    print "Finished"


log_path = ""
learner_id = ""
RetrieveEventLogs(log_path, learner_id)
        