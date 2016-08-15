'''
Created on Aug 4, 2016

@author: Angus
'''

import os, json, datetime, operator, pytz
from bson import json_util
from translation.Functions import ExtractCourseInformation, cmp_datetime, getDayDiff, process_null
from translation.ForumMode import forum_interaction_mongo

   
def learner_mode(metadata_path, course_code, cursor):
    
    # Collect course information
    course_metadata_map = ExtractCourseInformation(metadata_path)
    array = (course_metadata_map["course_id"], course_metadata_map["course_name"], course_metadata_map["start_time"], course_metadata_map["end_time"])
    sql = "insert into courses(course_id, course_name, start_time, end_time) values (%s,%s,%s,%s)" 
    cursor.execute(sql, array)
    
    # Course_element table     
    for element_id in course_metadata_map["element_time_map"].keys():
                  
        element_start_time = course_metadata_map["element_time_map"][element_id]
        
        # Some contents released just one hour earlier than the hour of start time.
        # For example, start time is 2015-10-15 09:00:00, while 2nd week contents' release time is 2015-10-22 08:00:00.
        # However, those 2nd week contents are count as 1st week.
        # In order to avoid above situation, I use date to replace datetime here.
        week = process_null(getDayDiff(course_metadata_map["start_time"].date(), element_start_time.date()) / 7 + 1)
                
        array = (element_id, course_metadata_map["element_type_map"][element_id], week, course_metadata_map["course_id"])
        sql = "insert into course_elements(element_id, element_type, week, course_id) values (%s,%s,%s,%s)" 
        cursor.execute(sql, array)
        
    # Quiz_question table
    quiz_question_map = course_metadata_map["quiz_question_map"]
    block_type_map = course_metadata_map["block_type_map"]
    element_time_map_due = course_metadata_map["element_time_map_due"]

    for question_id in quiz_question_map:

        question_due = ""
        question_weight = quiz_question_map[question_id]
        quiz_question_parent = course_metadata_map["child_parent_map"][question_id]
        
        if (question_due == "") and (quiz_question_parent in element_time_map_due):
            question_due = element_time_map_due[quiz_question_parent]

        while not block_type_map.has_key(quiz_question_parent):
            quiz_question_parent = course_metadata_map["child_parent_map"][quiz_question_parent]
            if (question_due == "") and (quiz_question_parent in element_time_map_due):
                question_due = element_time_map_due[quiz_question_parent]        
        
        quiz_question_type = block_type_map[quiz_question_parent]
        question_due = process_null(question_due)
        
        array = (question_id, quiz_question_type, question_weight, question_due)
        sql = "insert into quiz_questions(question_id, question_type, question_weight, question_due) values (%s,%s,%s,%s)"                            
        cursor.execute(sql, array)        
    
    files = os.listdir(metadata_path)
    
    # Learner_demographic table
    learner_mail_map = {}
    
    # Course_learner table
    course_learner_map = {}
    learner_enrollment_time_map = {}
    
    # Enrolled learners set
    enrolled_learner_set = set()
    
    course_id = ""
    
    # Processing student_courseenrollment data  
    for file in files:       
        if "student_courseenrollment" in file:
            input_file = open(str(metadata_path + file), "r")
            input_file.readline()
            lines = input_file.readlines()                        
            for line in lines:
                record = line.split("\t")
                global_learner_id = record[1]
                course_id = record[2]
                time = datetime.datetime.strptime(record[3], "%Y-%m-%d %H:%M:%S")
                course_learner_id = course_id + "_" + global_learner_id
                    
                if cmp_datetime(course_metadata_map["end_time"], time):           
                    
                    enrolled_learner_set.add(global_learner_id)
                    
                    array = (global_learner_id, course_id, course_learner_id)
                    sql = "insert into learner_index(global_learner_id, course_id, course_learner_id) values (%s,%s,%s)"
                    cursor.execute(sql, array)

                    course_learner_map[global_learner_id] = course_learner_id
                    learner_enrollment_time_map[global_learner_id] = time                    
            input_file.close()
  
    # Processing auth_user data  
    for file in files:               
        if "auth_user-" in file:
            input_file = open(str(metadata_path + file), "r")
            input_file.readline()
            lines = input_file.readlines()
            for line in lines:
                record = line.split("\t")
                if record[0] in enrolled_learner_set:
                    learner_mail_map[record[0]] = record[4]
            input_file.close()
                    
    # Processing certificates_generatedcertificate data
    num_uncertifiedLearners = 0
    num_certifiedLearners = 0    
    for file in files:       
        if "certificates_generatedcertificate" in file:
            input_file = open(str(metadata_path + file), "r")
            input_file.readline()
            lines = input_file.readlines()
            
            for line in lines:
                record = line.split("\t")
                global_learner_id = record[1]
                final_grade = process_null(record[3])
                enrollment_mode = record[14].replace("\n", "")
                certificate_status = record[7]
                
                register_time = ""
                if course_learner_map.has_key(global_learner_id):
                    register_time = learner_enrollment_time_map[global_learner_id]
                register_time = process_null(register_time)          
                
                if course_learner_map.has_key(global_learner_id):
                    num_certifiedLearners += 1
                    array = (course_learner_map[global_learner_id], final_grade, enrollment_mode, certificate_status, register_time)
                    sql = "insert into course_learner(course_learner_id, final_grade, enrollment_mode, certificate_status, register_time) values (%s,%s,%s,%s,%s)"
                    cursor.execute(sql, array)
                else:
                    num_uncertifiedLearners += 1            
            input_file.close()
    
    # Processing auth_userprofile data                    
    for file in files:       
        if "auth_userprofile" in file:
            input_file = open(str(metadata_path + file), "r")
            input_file.readline()
            lines = input_file.readlines()
                        
            for line in lines:
                record = line.split("\t")
                global_learner_id = record[1]
                gender = record[7]
                year_of_birth = process_null(process_null(record[9]))
                level_of_education = record[10]
                country = record[13]
                
                course_learner_id = process_null(course_id + "_" + global_learner_id)
                                
                if global_learner_id in enrolled_learner_set:
                    array = (course_learner_id, gender, year_of_birth, level_of_education, country, learner_mail_map[global_learner_id])
                    sql = "insert into learner_demographic(course_learner_id, gender, year_of_birth, level_of_education, country, email) values (%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, array)           
            input_file.close()
            
    # Generating forum_interaction records for courses starting before 1T2015
    if "1T2015" in course_code or "2014" in course_code or "2013" in course_code:
        forum_interaction_mongo(metadata_path, cursor)
    
        
def sessions(metadata_path, daily_log_path, remaining_session_log_path, cursor):
    
    utc = pytz.UTC
    
    # Collect course information
    course_metadata_map = ExtractCourseInformation(metadata_path)
    end_date = course_metadata_map["end_date"]
    
    learner_logs = {}
    remaining_learner_logs = {}
    
    # Read remaining event logs
    if os.path.exists(remaining_session_log_path):
        remaining_input_file = open(remaining_session_log_path)
        learner_logs = json.loads(remaining_input_file.read(), object_hook=json_util.object_hook)
    
    # Course_learner_id set
    course_learner_id_set = set()
    for course_learner_id in learner_logs.keys():
        course_learner_id_set.add(course_learner_id)
    
    input_file = open(daily_log_path, "r")
    for line in input_file:
        
        jsonObject = json.loads(line)
        
        # Skip records without user_id
        if "user_id" not in jsonObject["context"] or jsonObject["context"]["user_id"] == "" or jsonObject["context"]["user_id"] == None:
            continue

        # For session separation
        global_learner_id = jsonObject["context"]["user_id"]
        event_type = str(jsonObject["event_type"])
                    
        course_id = jsonObject["context"]["course_id"]
        course_learner_id = course_id + "_" + str(global_learner_id)
                   
        event_time = jsonObject["time"]
        
        # Check whether the event record belongs to that day
        log_date = event_time[0:10]
        if log_date not in daily_log_path:
            # print "Log not belonging to the day...\t" + log_date
            continue
        
        event_time = event_time[0:19]
        event_time = event_time.replace("T", " ")
        event_time = datetime.datetime.strptime(event_time,"%Y-%m-%d %H:%M:%S")
        event_time = event_time.replace(tzinfo=utc)
                   
        if course_learner_id in course_learner_id_set:
            learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type})
        else:
            learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type}]
            course_learner_id_set.add(course_learner_id)
            
    input_file.close()
                    
    # For session separation             
    for learner in learner_logs.keys():
            
        course_learner_id = learner                    
        event_logs = learner_logs[learner]
                                 
        # Sorting
        event_logs.sort(cmp=cmp_datetime, key=operator.itemgetter('event_time'))
                      
        session_id = ""
        start_time = ""
        end_time = ""
                    
        final_time = ""
                    
        for i in range(len(event_logs)):
                        
            if start_time == "":
                            
                # Initialization
                start_time = event_logs[i]["event_time"]
                end_time = event_logs[i]["event_time"]
                            
            else:
                            
                if event_logs[i]["event_time"] > end_time + datetime.timedelta(hours=0.5):
                                
                    session_id = course_learner_id + "_" + str(start_time) + "_" + str(end_time)
                    duration = (end_time - start_time).days * 24 * 60 * 60 + (end_time - start_time).seconds
                                
                    if duration > 5:
                        array = (session_id, course_learner_id, start_time, end_time, process_null(duration))
                        sql = "replace into sessions(session_id, course_learner_id, start_time, end_time, duration) values (%s,%s,%s,%s,%s)"
                        try:
                            cursor.execute(sql, array)
                        except Exception as e:
                            pass                    
                                    
                    final_time = event_logs[i]["event_time"]
                                    
                    # Re-initialization
                    session_id = ""
                    start_time = event_logs[i]["event_time"]
                    end_time = event_logs[i]["event_time"]
                            
                else:
                                
                    if event_logs[i]["event_type"] == "page_close":
                                    
                        end_time = event_logs[i]["event_time"]
                                    
                        session_id = course_learner_id + "_" + str(start_time) + "_" + str(end_time)
                        duration = (end_time - start_time).days * 24 * 60 * 60 + (end_time - start_time).seconds
                                
                        if duration > 5:
                            array = (session_id, course_learner_id, start_time, end_time, process_null(duration))
                            sql = "replace into sessions(session_id, course_learner_id, start_time, end_time, duration) values (%s,%s,%s,%s,%s)"
                            try:
                                cursor.execute(sql, array)
                            except Exception as e:
                                pass
                                        
                        # Re-initialization
                        session_id = ""
                        start_time = ""
                        end_time = ""
                                    
                        final_time = event_logs[i]["event_time"]
                                    
                    else:
                                    
                        end_time = event_logs[i]["event_time"]
                        
        if final_time != "":
            new_logs = []                
            for log in event_logs:                 
                if log["event_time"] > final_time:
                    new_logs.append(log)
                                
            remaining_learner_logs[course_learner_id] = new_logs
            
    # Output remaining logs
    if str(end_date)[0:10] not in daily_log_path:
        output_file = open(remaining_session_log_path, "w")
        output_file.write(json.dumps(remaining_learner_logs, default=json_util.default))
        output_file.close()
    else:
        os.remove(remaining_session_log_path)                
        
   
    
    
    