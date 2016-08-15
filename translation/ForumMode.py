'''
Created on Aug 2, 2016

@author: Angus
'''

import sys, json, datetime, os, operator, pytz
from bson import json_util
from time import *
from translation.Functions import ExtractCourseInformation, cleanUnicode, cmp_datetime, process_null

reload(sys)
sys.setdefaultencoding('utf-8')


def forum_interaction(metadata_path, daily_log_path, cursor):
    
    course_metadata_map = ExtractCourseInformation(metadata_path)
           
    # Forum-related events
    forum_event_types = []
    forum_event_types.append("edx.forum.thread.created")
    forum_event_types.append("edx.forum.response.created")
    forum_event_types.append("edx.forum.comment.created")     
        
    input_file = open(daily_log_path, "r")
    for line in input_file:
        jsonObject = json.loads(line)
            
        event_type = jsonObject["event_type"]
            
        if event_type in forum_event_types:
                            
            # Skip records without user_id
            if "user_id" not in jsonObject["context"] or jsonObject["context"]["user_id"] == "" or jsonObject["context"]["user_id"] == None:
                continue
            
            post_timestamp = jsonObject["time"]
            
            # Check whether the event record belongs to that day
            log_date = post_timestamp[0:10]
            if log_date not in daily_log_path:
                print "Log not belonging to the day...\t" + log_date
                continue
                            
            post_id = jsonObject["event"]["id"]
            course_learner_id = jsonObject["context"]["course_id"] + "_" + str(jsonObject["context"]["user_id"])
                
            post_type = ""
            if event_type == "edx.forum.thread.created":
                post_type = "Thread_" + jsonObject["event"]["thread_type"]                   
            if event_type == "edx.forum.response.created":
                post_type = "Response"
            if event_type == "edx.forum.comment.created":
                post_type = "Comment"
                    
            post_title = ""
            if event_type == "edx.forum.thread.created":
                post_title = jsonObject["event"]["title"]
                               
            post_content = jsonObject["event"]["body"]
            
            post_timestamp = post_timestamp[0:19]
            post_timestamp = post_timestamp.replace("T", " ")
            post_timestamp = datetime.datetime.strptime(post_timestamp,"%Y-%m-%d %H:%M:%S")
                
            post_parent_id = ""
            if event_type == "edx.forum.comment.created":
                post_parent_id = jsonObject["event"]["response"]["id"]          
                    
            post_thread_id = ""    
            if event_type == "edx.forum.response.created":
                post_thread_id = jsonObject["event"]["discussion"]["id"]                
                
            # Pre-processing title & content
            post_title = post_title.replace("\n", " ")
            post_title = post_title.replace("\\", "\\\\")
            post_title = post_title.replace("\'", "\\'")
            post_content = post_content.replace("\n", " ")
            post_content = post_content.replace("\\", "\\\\")
            post_content = post_content.replace("\'", "\\'")
                
            post_title = cleanUnicode(post_title)
            post_content = cleanUnicode(post_content)
                
            if post_timestamp < course_metadata_map["end_time"]:
                array = [post_id, course_learner_id, post_type, post_title, post_content, post_timestamp, post_parent_id, post_thread_id]
                sql = "replace into forum_interaction(post_id, course_learner_id, post_type, post_title, post_content, post_timestamp, post_parent_id, post_thread_id) values (%s,%s,%s,%s,%s,%s,%s,%s);"
                try:
                    cursor.execute(sql, array)
                except Exception as e:
                    pass
            
    input_file.close()    
      

def forum_interaction_mongo(metadata_path, cursor):
    
    course_metadata_map = ExtractCourseInformation(metadata_path)
    
    files = os.listdir(metadata_path)     
    for file in files:
        if ".mongo" in file:
            forum_file = open(str(metadata_path + file),"r")   
            for line in forum_file:
                jsonObject = json.loads(line)
                   
                post_id = jsonObject["_id"]["$oid"]                
                course_learner_id = jsonObject["course_id"] + "_" + jsonObject["author_id"]                

                post_type = jsonObject["_type"]
                if post_type == "CommentThread":
                    post_type += "_" + jsonObject["thread_type"]                
                if "parent_id" in jsonObject and jsonObject["parent_id"] != "":
                    post_type = "Comment_Reply"
                
                post_title = ""
                if "title" in jsonObject:
                    post_title=jsonObject["title"]
                
                post_content = jsonObject["body"]
                
                post_timestamp = jsonObject["created_at"]["$date"]
                if type(post_timestamp) == type(100):
                    post_timestamp = strftime("%Y-%m-%d %H:%M:%S",gmtime(post_timestamp/1000))
                    post_timestamp = datetime.datetime.strptime(post_timestamp,"%Y-%m-%d %H:%M:%S")
                if isinstance(post_timestamp, unicode):
                    post_timestamp = post_timestamp[0:19]
                    post_timestamp = post_timestamp.replace("T", " ")
                    post_timestamp = datetime.datetime.strptime(post_timestamp,"%Y-%m-%d %H:%M:%S")
                
                post_parent_id = ""
                if "parent_id" in jsonObject:
                    post_parent_id = jsonObject["parent_id"]["$oid"]
                
                post_thread_id = ""    
                if "comment_thread_id" in jsonObject:
                    post_thread_id = jsonObject["comment_thread_id"]["$oid"]                
                
                post_title = post_title.replace("\n", " ")
                post_title = post_title.replace("\\", "\\\\")
                post_title = post_title.replace("\'", "\\'")
                
                post_content = post_content.replace("\n", " ")
                post_content = post_content.replace("\\", "\\\\")
                post_content = post_content.replace("\'", "\\'")
                
                if post_timestamp < course_metadata_map["end_time"]:
                    
                    array = [post_id, course_learner_id, post_type, post_title, post_content, post_timestamp, post_parent_id, post_thread_id]
                    sql = "replace into forum_interaction(post_id, course_learner_id, post_type, post_title, post_content, post_timestamp, post_parent_id, post_thread_id) values (%s,%s,%s,%s,%s,%s,%s,%s);"
                    cursor.execute(sql, array)
                    
            forum_file.close()

def forum_sessions(metadata_path, daily_log_path, remaining_forum_session_log_path, cursor):
    
    utc = pytz.UTC
    
    course_metadata_map =  ExtractCourseInformation(metadata_path)
    end_date = course_metadata_map["end_date"]
    
    # Forum-related events
    forum_event_types = []
    forum_event_types.append("edx.forum.comment.created")
    forum_event_types.append("edx.forum.response.created")
    forum_event_types.append("edx.forum.response.voted")
    forum_event_types.append("edx.forum.thread.created")
    forum_event_types.append("edx.forum.thread.voted")
    forum_event_types.append("edx.forum.searched")
    
    learner_logs = {}
    remaining_learner_logs = {}
    
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
            
        # For forum session separation
        global_learner_id = jsonObject["context"]["user_id"]
        event_type = str(jsonObject["event_type"])
                    
        if "/discussion/" in event_type or event_type in forum_event_types:
            if event_type != "edx.forum.searched":
                event_type = "forum_activity"
                                            
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
                            
    # For forum session separation
    for learner in learner_logs.keys():
                    
        course_learner_id = learner                    
        event_logs = learner_logs[learner]
                    
        # Sorting
        event_logs.sort(cmp=cmp_datetime, key=operator.itemgetter('event_time'))
                    
        session_id = ""
        start_time = ""
        end_time = ""                    
        times_search = 0
                    
        final_time = ""
                    
        for i in range(len(event_logs)):
            
            if session_id == "":                            
                            
                if event_logs[i]["event_type"] in ["forum_activity", "edx.forum.searched"]:
                    # Initialization
                    session_id = "forum_session_" + course_learner_id
                    start_time = event_logs[i]["event_time"]
                    end_time = event_logs[i]["event_time"]
                    if event_logs[i]["event_type"] == "edx.forum.searched":
                        times_search += 1                                                     
            else:
                            
                if event_logs[i]["event_type"] in ["forum_activity", "edx.forum.searched"]:

                    if event_logs[i]["event_time"] > end_time + datetime.timedelta(hours=0.5):
                                    
                        session_id = session_id + "_" + str(start_time) + "_" + str(end_time)
                        duration = (end_time - start_time).days * 24 * 60 * 60 + (end_time - start_time).seconds
                        
                        times_search = process_null(times_search)
                        duration = process_null(duration)
                                    
                        if duration > 5:                                
                            array = (session_id, course_learner_id, times_search, start_time, end_time, duration, "")    
                            sql = "insert into forum_sessions (session_id, course_learner_id, times_search, start_time, end_time, duration, relevent_element_id) values (%s,%s,%s,%s,%s,%s,%s)"
                            try:
                                cursor.execute(sql, array)
                            except Exception as e:
                                pass                            
                                    
                        final_time = event_logs[i]["event_time"]
                                    
                        # Re-initialization
                        session_id = "forum_session_" + course_learner_id
                        start_time = event_logs[i]["event_time"]
                        end_time = event_logs[i]["event_time"]
                        if event_logs[i]["event_type"] == "edx.forum.searched":
                            times_search = 1
                        
                    else:
                                    
                        end_time = event_logs[i]["event_time"]
                        if event_logs[i]["event_type"] == "edx.forum.searched":
                            times_search += 1
                                                        
                else:
                                
                    if event_logs[i]["event_time"] <= end_time + datetime.timedelta(hours=0.5):
                        end_time = event_logs[i]["event_time"]

                    session_id = session_id + "_" + str(start_time) + "_" + str(end_time)
                    duration = (end_time - start_time).days * 24 * 60 * 60 + (end_time - start_time).seconds
                    
                    times_search = process_null(times_search)
                    duration = process_null(duration)
                                
                    if duration > 5:                                
                        array = (session_id, course_learner_id, times_search, start_time, end_time, duration, "")                        
                        sql = "insert into forum_sessions (session_id, course_learner_id, times_search, start_time, end_time, duration, relevent_element_id) values (%s,%s,%s,%s,%s,%s,%s)"
                        try:
                            cursor.execute(sql, array)
                        except Exception as e:
                            pass
                                    
                    final_time = event_logs[i]["event_time"]
                                    
                    # Re-initialization
                    session_id = ""
                    start_time = ""
                    end_time = ""
                    times_search = 0
  
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
        
        