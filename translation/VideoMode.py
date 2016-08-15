'''
Created on Aug 7, 2016

@author: Angus
'''

import os, json, datetime, operator, pytz
from bson import json_util
from translation.Functions import ExtractCourseInformation, cmp_datetime, process_null


def video_interaction(metadata_path, daily_log_path, remaining_video_interaction_log_path, cursor):
    
    utc = pytz.UTC
    
    course_metadata_map = ExtractCourseInformation(metadata_path)
    end_date = course_metadata_map["end_date"]
    
    video_interaction_map = {}
    
    # Video-related event types
    video_event_types = []
    video_event_types.append("hide_transcript")
    video_event_types.append("edx.video.transcript.hidden")
    video_event_types.append("edx.video.closed_captions.hidden")
    video_event_types.append("edx.video.closed_captions.shown")
    video_event_types.append("load_video")
    video_event_types.append("edx.video.loaded")
    video_event_types.append("pause_video")
    video_event_types.append("edx.video.paused")
    video_event_types.append("play_video")
    video_event_types.append("edx.video.played")
    video_event_types.append("seek_video")
    video_event_types.append("edx.video.position.changed")
    video_event_types.append("show_transcript")
    video_event_types.append("edx.video.transcript.shown")
    video_event_types.append("speed_change_video")
    video_event_types.append("stop_video")
    video_event_types.append("edx.video.stopped") 
    video_event_types.append("video_hide_cc_menu")
    video_event_types.append("edx.video.language_menu.hidden")
    video_event_types.append("video_show_cc_menu")
    video_event_types.append("edx.video.language_menu.shown")
    
    learner_logs = {}
    remaining_learner_logs = {}
    
    # Read remaining event logs
    if os.path.exists(remaining_video_interaction_log_path):
        remaining_input_file = open(remaining_video_interaction_log_path)
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
        
        global_learner_id = jsonObject["context"]["user_id"]
        event_type = jsonObject["event_type"]
        
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
        
        # For video-related events      
        if event_type in video_event_types:
            
            video_id = ""
        
            # For seek event
            new_time = 0
            old_time = 0
        
            # For speed change event
            new_speed = 0
            old_speed = 0
        
            if isinstance(jsonObject["event"], unicode):
                event_jsonObject = json.loads(jsonObject["event"])
                video_id = event_jsonObject["id"]
                
                video_id = video_id.replace("-", "://", 1)
                video_id = video_id.replace("-", "/")
            
                # For video seek event
                if "new_time" in event_jsonObject and "old_time" in event_jsonObject:
                    new_time = event_jsonObject["new_time"]
                    old_time = event_jsonObject["old_time"]                                                                      
                                                                
                # For video speed change event           
                if "new_speed" in event_jsonObject and "old_speed" in event_jsonObject:
                    new_speed = event_jsonObject["new_speed"]
                    old_speed = event_jsonObject["old_speed"]
        
            # To record video seek event                
            if event_type in ["seek_video","edx.video.position.changed"]:
                if new_time is not None and old_time is not None:
                    if course_learner_id in course_learner_id_set:
                        learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type, "video_id":video_id, "new_time":new_time, "old_time":old_time})
                    else:
                        learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type, "video_id":video_id, "new_time":new_time, "old_time":old_time}]
                        course_learner_id_set.add(course_learner_id)
                continue
        
            # To record video speed change event                
            if event_type in ["speed_change_video"]:
                if course_learner_id in course_learner_id_set:
                    learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type, "video_id":video_id, "new_speed":new_speed, "old_speed":old_speed})
                else:
                    learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type, "video_id":video_id, "new_speed":new_speed, "old_speed":old_speed}]
                    course_learner_id_set.add(course_learner_id)
                continue                                                                      
         
            if course_learner_id in course_learner_id_set:
                learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type, "video_id":video_id})
            else:
                learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type, "video_id":video_id}]
                course_learner_id_set.add(course_learner_id)
        
        # For non-video-related events                                    
        if event_type not in video_event_types:
            if course_learner_id in course_learner_id_set:
                learner_logs[course_learner_id].append({"event_time":event_time, "event_type":event_type})
            else:
                learner_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type}]
                course_learner_id_set.add(course_learner_id)
    
    input_file.close()
    
    # For video interaction extraction                  
    for learner in learner_logs.keys():
        
        course_learner_id = learner                    
        event_logs = learner_logs[learner]
        
        # Sorting
        event_logs.sort(cmp=cmp_datetime, key=operator.itemgetter('event_time'))
        
        video_id = ""
        video_start_time = ""
        final_time = ""
        
        # For video seek event
        times_forward_seek = 0
        duration_forward_seek = 0
        times_backward_seek = 0
        duration_backward_seek = 0
        
        # For video speed change event
        speed_change_last_time = ""
        times_speed_up = 0
        times_speed_down = 0               
        
        # For video pause event                   
        pause_check = False
        pause_start_time = ""
        duration_pause = 0                    
                          
        for i in range(len(event_logs)):
            
            if event_logs[i]["event_type"] in ["play_video", "edx.video.played"]:
                video_start_time = event_logs[i]["event_time"]
                video_id = event_logs[i]["video_id"]

                if pause_check:
                    duration_pause = (event_logs[i]["event_time"] - pause_start_time).days * 24 * 60 * 60 + (event_logs[i]["event_time"] - pause_start_time).seconds
                    video_interaction_id = course_learner_id + "_" + video_id + "_" + str(pause_start_time)
                    if duration_pause > 2 and duration_pause < 600:
                        if video_interaction_id in video_interaction_map.keys():
                            video_interaction_map[video_interaction_id]["times_pause"] = 1                                        
                            video_interaction_map[video_interaction_id]["duration_pause"] = duration_pause
                    pause_check = False                  
                continue 
            
            if video_start_time != "":                                                    
               
                if event_logs[i]["event_time"] > video_start_time + datetime.timedelta(hours=0.5):
                    
                    video_start_time = ""
                    video_id = ""
                    final_time = event_logs[i]["event_time"]
                    
                else:
                    
                    # 0. Seek
                    if event_logs[i]["event_type"] in ["seek_video", "edx.video.position.changed"] and video_id == event_logs[i]["video_id"]:                                                                       
                        # Forward seek event
                        if event_logs[i]["new_time"] > event_logs[i]["old_time"]:
                            times_forward_seek += 1
                            duration_forward_seek += event_logs[i]["new_time"] - event_logs[i]["old_time"]
                        # Backward seek event                                    
                        if event_logs[i]["new_time"] < event_logs[i]["old_time"]:
                            times_backward_seek += 1
                            duration_backward_seek += event_logs[i]["old_time"] - event_logs[i]["new_time"]
                        continue
                    
                    # 1. Speed change
                    if event_logs[i]["event_type"] == "speed_change_video" and video_id == event_logs[i]["video_id"]:
                        if speed_change_last_time == "":
                            speed_change_last_time = event_logs[i]["event_time"]
                            old_speed = event_logs[i]["old_speed"]
                            new_speed = event_logs[i]["new_speed"]                                        
                            if old_speed < new_speed:
                                times_speed_up += 1
                            if old_speed > new_speed:
                                times_speed_down += 1
                        else:
                            if (event_logs[i]["event_time"] - speed_change_last_time).seconds > 10:
                                old_speed = event_logs[i]["old_speed"]
                                new_speed = event_logs[i]["new_speed"]                                        
                                if old_speed < new_speed:
                                    times_speed_up += 1
                                if old_speed > new_speed:
                                    times_speed_down += 1
                            speed_change_last_time = event_logs[i]["event_time"]
                        continue
                    
                    # 2. Pause/Stop situation
                    if event_logs[i]["event_type"] in ["pause_video", "edx.video.paused", "stop_video", "edx.video.stopped"] and video_id == event_logs[i]["video_id"]:                                    
                        
                        watch_duration = (event_logs[i]["event_time"] - video_start_time).seconds
                        
                        video_end_time = event_logs[i]["event_time"]
                        video_interaction_id = course_learner_id + "_" + video_id + "_" + str(video_start_time) + "_" + str(video_end_time)
                     
                        if watch_duration > 5:                                        
                            video_interaction_map[video_interaction_id] = {"course_learner_id":course_learner_id, "video_id":video_id, "type": "video", "watch_duration":watch_duration,
                                                            "times_forward_seek":times_forward_seek, "duration_forward_seek":duration_forward_seek, 
                                                            "times_backward_seek":times_backward_seek, "duration_backward_seek":duration_backward_seek,
                                                            "times_speed_up":times_speed_up, "times_speed_down":times_speed_down,
                                                            "start_time":video_start_time, "end_time":video_end_time}

                        if event_logs[i]["event_type"] in ["pause_video", "edx.video.paused"]:
                            pause_check = True
                            pause_start_time = video_end_time
                        
                        # For video seek event
                        times_forward_seek = 0
                        duration_forward_seek = 0
                        times_backward_seek = 0
                        duration_backward_seek = 0
                        
                        # For video speed change event
                        speed_change_last_time = ""
                        times_speed_up = 0
                        times_speed_down = 0
                        
                        # For video general information                                  
                        video_start_time =""
                        video_id = ""
                        final_time = event_logs[i]["event_time"]
                        
                        continue
                        
                    # 3/4  Page changed/Session closed
                    if event_logs[i]["event_type"] not in video_event_types:
                        
                        video_end_time = event_logs[i]["event_time"]
                        watch_duration = (video_end_time - video_start_time).seconds                
                        video_interaction_id = course_learner_id + "_" + video_id + "_" + str(video_start_time) + "_" + str(video_end_time)
                    
                        if watch_duration > 5:                                        
                            video_interaction_map[video_interaction_id] = {"course_learner_id":course_learner_id, "video_id":video_id, "type": "video", "watch_duration":watch_duration,
                                                            "times_forward_seek":times_forward_seek, "duration_forward_seek":duration_forward_seek, 
                                                            "times_backward_seek":times_backward_seek, "duration_backward_seek":duration_backward_seek,
                                                            "times_speed_up":times_speed_up, "times_speed_down":times_speed_down,
                                                            "start_time":video_start_time, "end_time":video_end_time}
                        
                        # For video seek event
                        times_forward_seek = 0
                        duration_forward_seek = 0
                        times_backward_seek = 0
                        duration_backward_seek = 0
                        
                        # For video speed change event
                        speed_change_last_time = ""
                        times_speed_up = 0
                        times_speed_down = 0
                        
                        # For video general information
                        video_start_time = ""                                    
                        video_id = ""
                        final_time = event_logs[i]["event_time"]
                        
                        continue
            
        if final_time != "":
            new_logs = []                
            for log in event_logs:                 
                if log["event_time"] > final_time:
                    new_logs.append(log)
                    
            remaining_learner_logs[course_learner_id] = new_logs                
                     
    # Output remaining logs
    if str(end_date)[0:10] not in daily_log_path:
        output_file = open(remaining_video_interaction_log_path, "w")
        output_file.write(json.dumps(remaining_learner_logs, default=json_util.default))
        output_file.close()
    else:
        os.remove(remaining_video_interaction_log_path)    
    
    for interaction_id in video_interaction_map.keys():
        video_interaction_id = interaction_id
        course_learner_id = video_interaction_map[interaction_id]["course_learner_id"]
        video_id = video_interaction_map[interaction_id]["video_id"]
        duration = process_null(video_interaction_map[interaction_id]["watch_duration"])
        times_forward_seek = process_null(video_interaction_map[interaction_id]["times_forward_seek"])
        duration_forward_seek = process_null(video_interaction_map[interaction_id]["duration_forward_seek"])
        times_backward_seek = process_null(video_interaction_map[interaction_id]["times_backward_seek"])
        duration_backward_seek = process_null(video_interaction_map[interaction_id]["duration_backward_seek"])
        times_speed_up = process_null(video_interaction_map[interaction_id]["times_speed_up"])
        times_speed_down = process_null(video_interaction_map[interaction_id]["times_speed_down"])
        start_time = video_interaction_map[interaction_id]["start_time"]
        end_time = video_interaction_map[interaction_id]["end_time"]
        
        if "times_pause" in video_interaction_map[interaction_id]:
            times_pause = process_null(video_interaction_map[interaction_id]["times_pause"])
            duration_pause = process_null(video_interaction_map[interaction_id]["duration_pause"])
        else:
            times_pause = 0
            duration_pause = 0
            
        array = [video_interaction_id, course_learner_id, video_id, duration, times_forward_seek, duration_forward_seek, times_backward_seek, duration_backward_seek, times_speed_up, times_speed_down, times_pause, duration_pause, start_time, end_time]
        sql = "insert into video_interaction(interaction_id, course_learner_id, video_id, duration, times_forward_seek, duration_forward_seek, times_backward_seek, duration_backward_seek, times_speed_up, times_speed_down, times_pause, duration_pause, start_time, end_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql, array)
        except Exception as e:
            pass
        
        
                    
        
        