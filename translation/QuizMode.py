'''
Created on Aug 5, 2016

@author: Angus
'''

import os, json, datetime, operator, pytz, sys
from bson import json_util
from translation.Functions import ExtractCourseInformation, cmp_datetime, process_null

reload(sys)
sys.setdefaultencoding('utf-8')


def quiz_mode(daily_log_path, cursor):
    
    input_file = open(daily_log_path,"r")                
    for line in input_file:                              
        jsonObject = json.loads(line)        
        event_type = jsonObject["event_type"]
        event_source = jsonObject["event_source"]
            
        if event_type == "problem_check" and event_source == "server":
            
            # Skip records without user_id
            if "user_id" not in jsonObject["context"] or jsonObject["context"]["user_id"] == "" or jsonObject["context"]["user_id"] == None:
                continue       
                        
            course_learner_id = jsonObject["context"]["course_id"] + "_" + str(jsonObject["context"]["user_id"])
                                        
            question_id = ""
            grade = ""
            max_grade = ""
                            
            event_time = jsonObject["time"]
        
            # Check whether the event record belongs to that day
            log_date = event_time[0:10]
            if log_date not in daily_log_path:
                print "Log not belonging to the day...\t" + log_date
                continue
            
            event_time = event_time[0:19]
            event_time = event_time.replace("T", " ")
            event_time = datetime.datetime.strptime(event_time,"%Y-%m-%d %H:%M:%S")
            
            question_id = jsonObject["event"]["problem_id"]
                                            
            grade = jsonObject["event"]["grade"]
            max_grade = jsonObject["event"]["max_grade"]
                
            if question_id != "":
                                
                submission_id = course_learner_id + "_" + question_id + "_" + str(event_time)
                submission_timestamp = event_time
                
                '''
                Some "problem_check" events have almost the same content (only slightly different in their time, 
                like 2015-09-10T08:54:09.927618+00:00 vs. 2015-09-10T08:54:09.014793+00:00), which might cause duplicate
                submission_id/assessment_id issue. So I used the try/except handling here. One possible reason for 
                such event records might be some learners just keep clicking the "Check" button in a very short time
                or cased by network issues.
                '''
                       
                # For submissions
                sql = "insert into submissions(submission_id, course_learner_id, question_id, submission_timestamp) values (%s,%s,%s,%s)"
                array = (submission_id, course_learner_id, question_id, submission_timestamp)                    
                try:
                    cursor.execute(sql, array)
                except Exception as e:
                    pass
                    # print "Submissions table...\t" + str(e) + "\t" + submission_id    
                            
                # For assessments
                if grade != "" and max_grade != "":                                    
                    assessment_id = submission_id
                    sql = "insert into assessments(assessment_id, course_learner_id, max_grade, grade) values (%s,%s,%s,%s)"
                    array = (assessment_id, course_learner_id, max_grade, grade)
                    try:
                        cursor.execute(sql, array)
                    except Exception as e:
                        pass
                        # print "Assessments table...\t" + str(e) + "\t" + assessment_id    
        
def quiz_sessions(metadata_path, daily_log_path, remaining_forum_session_log_path, cursor):
    
    utc = pytz.UTC
    
    course_metadata_map =  ExtractCourseInformation(metadata_path)
    end_date = course_metadata_map["end_date"]       
                            
    # Quiz-related events
    quiz_event_types = []

    # Problem check
    quiz_event_types.append("problem_check")     # Server
    quiz_event_types.append("save_problem_check")
    quiz_event_types.append("problem_check_fail")
    quiz_event_types.append("save_problem_check_fail")
    
    # The server emits a problem_graded event each time a user selects Check for a problem and it is graded success- fully.
    quiz_event_types.append("problem_graded")
    
    # The server emits problem_rescore events when a problem is successfully rescored.
    quiz_event_types.append("problem_rescore")
    quiz_event_types.append("problem_rescore_fail")
    
    quiz_event_types.append("problem_reset") # event_source: serve
    quiz_event_types.append("reset_problem")
    quiz_event_types.append("reset_problem_fail")
    
    # The server emits problem_save events after a user saves a problem.
    quiz_event_types.append("problem_save") # event_source: server
    quiz_event_types.append("save_problem_fail")
    quiz_event_types.append("save_problem_success")
    
    # Show answer
    quiz_event_types.append("problem_show")
    quiz_event_types.append("showanswer")
    
    quiz_event_types.append("edx.problem.hint.demandhint_displayed")
    quiz_event_types.append("edx.problem.hint.feedback_displayed")
    
    child_parent_map = course_metadata_map["child_parent_map"]
    
    learner_logs = {}
    remaining_learner_logs = {}
    
    quiz_sessions = {}
    
    # Read remaining event logs
    if os.path.exists(remaining_forum_session_log_path):
        remaining_input_file = open(remaining_forum_session_log_path)
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
        
        # For quiz session separation
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
        
        if learner_logs.has_key(course_learner_id):
            learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type})
        else:
            learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type}]
            
    input_file.close()
    
    # For quiz session separation
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
                        
            if session_id == "":
                            
                if "problem+block" in event_logs[i]["event_type"] or "_problem;_" in event_logs[i]["event_type"]:
                                
                    event_type_array = event_logs[i]["event_type"].split("/")
                                
                    if "problem+block" in event_logs[i]["event_type"]:
                        question_id = event_type_array[4]
                                    
                    if "_problem;_" in event_logs[i]["event_type"]:
                        question_id = event_type_array[6].replace(";_", "/")
                                
                    if question_id in child_parent_map.keys():                                    
                        parent_block_id = child_parent_map[question_id]                                
                        session_id = "quiz_session_" + parent_block_id + "_" + course_learner_id
                        start_time = event_logs[i]["event_time"]
                        end_time = event_logs[i]["event_time"]                                
                                                                                        
            else:
                            
                if "problem+block" in event_logs[i]["event_type"] or "_problem;_" in event_logs[i]["event_type"] or event_logs[i]["event_type"] in quiz_event_types:

                    if event_logs[i]["event_time"] > end_time + datetime.timedelta(hours=0.5):
                        
                        if quiz_sessions.has_key(session_id):
                            quiz_sessions[session_id]["time_array"].append({"start_time":start_time, "end_time":end_time})
                        else:
                            quiz_sessions[session_id] = {"course_learner_id":course_learner_id, "time_array":[{"start_time":start_time, "end_time":end_time}]}
                        
                        final_time = event_logs[i]["event_time"]
                        
                        if "problem+block" in event_logs[i]["event_type"] or "_problem;_" in event_logs[i]["event_type"] or event_logs[i]["event_type"] in quiz_event_types:
                            event_type_array = event_logs[i]["event_type"].split("/")
                            
                            if "problem+block" in event_logs[i]["event_type"]:
                                question_id = event_type_array[4]
                        
                            if "_problem;_" in event_logs[i]["event_type"]:
                                question_id = event_type_array[6].replace(";_", "/")
                            
                            if question_id in child_parent_map.keys():
                                parent_block_id = child_parent_map[question_id]
                                session_id = "quiz_session_" + parent_block_id + "_" +course_learner_id
                                start_time = event_logs[i]["event_time"]
                                end_time = event_logs[i]["event_time"]
                            else:
                                session_id = ""
                                start_time = ""
                                end_time = ""     
                    else:                                    
                        end_time = event_logs[i]["event_time"]
                                                                
                else:

                    if event_logs[i]["event_time"] <= end_time + datetime.timedelta(hours=0.5):
                        end_time = event_logs[i]["event_time"]
                    
                    if quiz_sessions.has_key(session_id):
                        quiz_sessions[session_id]["time_array"].append({"start_time":start_time, "end_time":end_time})
                    else:
                        quiz_sessions[session_id] = {"course_learner_id":course_learner_id, "time_array":[{"start_time":start_time, "end_time":end_time}]}
                    
                    final_time = event_logs[i]["event_time"]
                    
                    session_id = ""
                    start_time = ""
                    end_time = ""
                                
        if final_time != "":
            new_logs = []                
            for log in event_logs:                 
                if log["event_time"] > final_time:
                    new_logs.append(log)
                    
            remaining_learner_logs[course_learner_id] = new_logs
            
    # Output remaining logs
    if str(end_date)[0:10] not in daily_log_path:
        output_file = open(remaining_forum_session_log_path, "w")
        output_file.write(json.dumps(remaining_learner_logs, default=json_util.default))
        output_file.close()
    else:
        os.remove(remaining_forum_session_log_path)    
    
    # To compress the session event_logs
    for session_id in quiz_sessions.keys():
        if len(quiz_sessions[session_id]["time_array"]) > 1:            
            
            start_time = ""
            end_time = ""
            updated_time_array = []
            
            for i in range(len(quiz_sessions[session_id]["time_array"])):                
                if i == 0:
                    start_time = quiz_sessions[session_id]["time_array"][i]["start_time"]
                    end_time = quiz_sessions[session_id]["time_array"][i]["end_time"]
                else:
                    if quiz_sessions[session_id]["time_array"][i]["start_time"] > end_time + datetime.timedelta(hours=0.5):
                        updated_time_array.append({"start_time":start_time, "end_time":end_time})                        
                        start_time = quiz_sessions[session_id]["time_array"][i]["start_time"]
                        end_time = quiz_sessions[session_id]["time_array"][i]["end_time"]
                        if i == len(quiz_sessions[session_id]["time_array"]) - 1:
                            updated_time_array.append({"start_time":start_time, "end_time":end_time})
                    else:
                        end_time = quiz_sessions[session_id]["time_array"][i]["end_time"]
                        
                        if i == len(quiz_sessions[session_id]["time_array"]) - 1:
                            updated_time_array.append({"start_time":start_time, "end_time":end_time})
            
            quiz_sessions[session_id]["time_array"] = updated_time_array
    
    for session_id in quiz_sessions.keys():
        course_learner_id = quiz_sessions[session_id]["course_learner_id"]
        for i in range(len(quiz_sessions[session_id]["time_array"])):
            start_time = quiz_sessions[session_id]["time_array"][i]["start_time"]
            end_time = quiz_sessions[session_id]["time_array"][i]["end_time"]
            if start_time < end_time:
                duration = process_null((end_time - start_time).days * 24 * 60 * 60 + (end_time - start_time).seconds)
                final_session_id = session_id + "_" + str(start_time) + "_" + str(end_time)
                if duration > 5:
                    array = (final_session_id, course_learner_id, start_time, end_time, duration)
                    sql = "insert into quiz_sessions (session_id, course_learner_id, start_time, end_time, duration) values (%s,%s,%s,%s,%s)"
                    try:
                        cursor.execute(sql, array)
                    except Exception as e:
                        pass
