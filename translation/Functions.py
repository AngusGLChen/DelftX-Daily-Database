'''
Created on Jun 16, 2016

@author: Angus
'''

import os, json, datetime, sys, unicodedata, pytz


def ExtractCourseInformation(metadata_path):
    
    course_metadata_map = {}
    
    files = os.listdir(metadata_path)
    
    # Processing course_structure data                
    for file in files:             
        if "course_structure" in file:           
            course_structure_file = open(str(metadata_path + file), "r")
            child_parent_map = {}
            element_time_map = {}
            
            # Add and element_time_map_due to record the due of sequential
            element_time_map_due = {}
            element_type_map = {}
            
            element_without_time = []
            
            quiz_question_map = {}
            block_type_map = {}
            
            jsonObject = json.loads(course_structure_file.read())
            for record in jsonObject:
                if jsonObject[record]["category"] == "course":
                    
                    # Course ID
                    course_id = record
                    if course_id.startswith("block-"):
                        course_id = course_id.replace("block-","course-")
                        course_id = course_id.replace("+type@course+block@course", "")
                    if course_id.startswith("i4x://"):
                        course_id = course_id.replace("i4x://", "")
                        course_id = course_id.replace("course/", "")
                    course_metadata_map["course_id"] = course_id
                    
                    # Course name
                    course_name = jsonObject[record]["metadata"]["display_name"]
                    course_metadata_map["course_name"] = course_name

                    start_time = jsonObject[record]["metadata"]["start"]
                    end_time = jsonObject[record]["metadata"]["end"]
                    
                    # Start & End data
                    start_date = start_time[0:start_time.index("T")]
                    end_date = end_time[0:end_time.index("T")]
                    course_metadata_map["start_date"] = datetime.datetime.strptime(start_date,"%Y-%m-%d")
                    course_metadata_map["end_date"] = datetime.datetime.strptime(end_date,"%Y-%m-%d")
                    
                    # Start & End time                    
                    start_time = start_time[0:19]
                    start_time = start_time.replace("T", " ")                    
                    start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
                       
                    end_time = end_time[0:19]
                    end_time = end_time.replace("T", " ")                    
                    end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
                    
                    course_metadata_map["start_time"] = start_time
                    course_metadata_map["end_time"] = end_time
                    
                    for child in jsonObject[record]["children"]:
                        child_parent_map[child] = record
                    element_time_map[record] = start_time
                    
                    element_type_map[record] = jsonObject[record]["category"]
                
                else:
                    
                    element_id = record
                    
                    # Child to parent relation                    
                    for child in jsonObject[element_id]["children"]:
                        child_parent_map[child] = element_id
                                       
                    # Element time
                    if "start" in jsonObject[element_id]["metadata"]:
                        element_start_time = jsonObject[element_id]["metadata"]["start"]
                        element_start_time = element_start_time[0:19]
                        element_start_time = element_start_time.replace("T", " ")                    
                        element_start_time = datetime.datetime.strptime(element_start_time,"%Y-%m-%d %H:%M:%S")
                        element_time_map[element_id] = element_start_time
                    else:
                        element_without_time.append(element_id)

                    # Element due time
                    if "due" in jsonObject[element_id]["metadata"]:
                        element_due_time = jsonObject[element_id]["metadata"]["due"]
                        element_due_time = element_due_time[0:19]
                        element_due_time = element_due_time.replace("T", " ")                    
                        element_due_time = datetime.datetime.strptime(element_due_time,"%Y-%m-%d %H:%M:%S")
                        element_time_map_due[element_id] = element_due_time                     
                    
                    # Element type
                    element_type_map[element_id] = jsonObject[element_id]["category"]
                    
                    # Quiz questions
                    if jsonObject[element_id]["category"] == "problem":
                        # quiz_question_map.append(element_id)
                        if jsonObject[element_id]["metadata"].has_key("weight"):
                            quiz_question_map[element_id] = jsonObject[element_id]["metadata"]["weight"]
                        else:
                            # By default the weight is 1?
                            quiz_question_map[element_id] = 1.0
                        
                    # Types of blocks to which quiz questions belong
                    if jsonObject[element_id]["category"] == "sequential":
                        if "display_name" in jsonObject[element_id]["metadata"]:
                            block_type = jsonObject[element_id]["metadata"]["display_name"]
                            block_type_map[element_id] = block_type

            # Decide the start time for each element   
            for element_id in element_without_time:
                element_start_time = ""
                while element_start_time == "":                     
                    element_parent = child_parent_map[element_id]
                    while not element_time_map.has_key(element_parent):
                        element_parent = child_parent_map[element_parent]
                    element_start_time = element_time_map[element_parent]
                element_time_map[element_id] = element_start_time
                
            course_metadata_map["element_time_map"] = element_time_map
            course_metadata_map["element_time_map_due"] = element_time_map_due
            course_metadata_map["element_type_map"] = element_type_map
            course_metadata_map["quiz_question_map"] = quiz_question_map
            course_metadata_map["child_parent_map"] = child_parent_map
            course_metadata_map["block_type_map"] = block_type_map
                              
    return course_metadata_map

    
def getDayDiff(beginDate,endDate):
    oneday = datetime.timedelta(days=1)  
    count = 0
    while (endDate - beginDate) >= oneday:  
        endDate = endDate - oneday
        count += 1
    return count


def getNextDay(current_day):
    format="%Y-%m-%d"
    oneday = datetime.timedelta(days=1)
    next_day = current_day + oneday   
    return next_day


def cmp_datetime(a_datetime, b_datetime):    
    utc=pytz.UTC    
    a_datetime = a_datetime.replace(tzinfo=utc)
    b_datetime = b_datetime.replace(tzinfo=utc)
    
    if a_datetime < b_datetime:
        return -1
    elif a_datetime > b_datetime:
        return 1
    else:
        return 0

# This function takes an string as input, and either returns if the string is 'NONE' or '', or the original string otherwise
def process_null(inputString):
    if isinstance(inputString, str) or isinstance(inputString, unicode):
        if len(inputString)==0 or inputString=='NULL':
            return None
        else:
            return inputString
    return inputString

def cleanUnicode(text):
    if isinstance(text, unicode):
        return unicodedata.normalize('NFC', text);
    else:
        return text

# This main function is used to test above functions
def main(argv):
    
    begindatetime_str = argv[0]
    begindatetime_str = begindatetime_str.replace("T", " ")
    begindatetime = datetime.datetime.strptime(begindatetime_str,"%Y-%m-%d %H:%M:%S")
    print begindatetime
    
    begindate = begindatetime.date()
    enddatetime_str = argv[1]
    enddatetime_str = enddatetime_str.replace("T", " ")   
    enddatetime = datetime.datetime.strptime(enddatetime_str,"%Y-%m-%d %H:%M:%S")
    print enddatetime
    
    enddate = enddatetime.date()
    daydiff = getDayDiff(begindatetime, enddatetime)
    print daydiff
    print (daydiff/7 + 1)
    
    daydiff = getDayDiff(begindate, enddate)
    print daydiff
    print (daydiff/7 + 1)

if __name__ == '__main__':
    main(sys.argv[1:])
