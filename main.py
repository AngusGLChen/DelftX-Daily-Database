'''
Created on Aug 2, 2016

@author: Angus
'''


import mysql.connector, os, json, gzip, ConfigParser, sys
from translation.CourseRecordRemoval import RemoveCourseRecords
from translation.Functions import ExtractCourseInformation, getDayDiff, getNextDay
from translation.LearnerMode import learner_mode, sessions
from translation.VideoMode import video_interaction
from translation.QuizMode import quiz_mode, quiz_sessions
from translation.ForumMode import forum_interaction, forum_sessions
from translation.SurveyMode import survey_mode


def main(argv):
    
    # Read configs
    config = ConfigParser.ConfigParser()
    config.read(argv[0])
    
    # All the configs are read as string
    course_log_path = config.get("data", "path")
    remove_filtered_logs = config.get("data", "remove_filtered_logs")
    log_update_list = json.loads(config.get("data", "log_update_list"))
    metadata_update_list = json.loads(config.get("data", "metadata_update_list"))
    survey_update_map = json.loads(config.get("data", "survey_update_map"))
    
    user = config.get("mysqld", "user")
    password = config.get("mysqld", "password")
    host = config.get("mysqld", "host")
    database = config.get("mysqld", "database")
        
    # Database
    connection = mysql.connector.connect(user=user, password=password, host=host, database=database, charset='utf8mb4')
    cursor = connection.cursor()

    # Delete relevant records before updating the database
    print "Removing log records..."
    for course_code in log_update_list:
        print str("\t" + course_code)
        RemoveCourseRecords(course_log_path, course_code, "log", cursor)
    print "Removing metadata records..."
    for course_code in metadata_update_list:
        print str("\t" + course_code)
        RemoveCourseRecords(course_log_path, course_code, "metadata", cursor)
    print "Removing survey records..."
    for course_code in survey_update_map.keys():
        print str("\t" + course_code)
        RemoveCourseRecords(course_log_path, course_code, "survey", cursor)
        
    print
    
    folders = os.listdir(course_log_path)
    for folder in folders:
        if folder != "daily_logs":
                            
            # Only for Mac OS
            if folder == ".DS_Store":
                continue
                
            course_code = folder
            
            print "Processing\t" + course_code
                
            # A file named "course_processing_tracker" (JSON format) is created 
            # for each course to keep track of the processing files
            tracker_path = str(course_log_path + course_code + "/course_processing_tracker")
            if not os.path.exists(tracker_path):
                    
                output_file = open(tracker_path, "w")
                tracker_map = {}
                    
                # This value is used to keep track of the processing status for the course' daily log files, 
                # i.e., "False" (not finished yet) and "True" (finished)
                tracker_map["status"] = False
                    
                tracker_map["processed_dates"] = []
                tracker_map["num_processed_dates"] = 0                
                output_file.write(json.dumps(tracker_map))
                output_file.close()
                    
            # Read the "course_processing_tracker" file
            input_file =  open(tracker_path, "r")
            tracker_map = json.loads(input_file.read())
            input_file.close()
            
            metadata_path = str(course_log_path + course_code + "/metadata/")
            
            # Determine whether the course_structure file is present
            mark = False
            files = os.listdir(metadata_path)
            for file in files:
                if "course_structure" in file:
                    mark = True
                    break
            if not mark:
                print "The course structure file is missing.\n"
                continue
            
            # Learner mode
            if course_code in metadata_update_list:
                print "Learner Mode processing..."        
                learner_mode(metadata_path, course_code, cursor)
            
            # Survey mode
            survey_path = str(course_log_path + course_code + "/surveys/")
            if course_code in survey_update_map.keys():
                print "Survey Mode processing..."        
                pre_id_index = int(survey_update_map[course_code][0])
                post_id_index = int(survey_update_map[course_code][1])
                survey_mode(metadata_path, survey_path, cursor, pre_id_index, post_id_index)
                    
            if tracker_map["status"]:
                print
                continue
                            
            # Retrieve the start/end date of the course
            course_metadata_map = ExtractCourseInformation(metadata_path)
            course_id = course_metadata_map["course_id"]
            start_date = course_metadata_map["start_date"]
            end_date = course_metadata_map["end_date"]
                               
            current_date = start_date
            while current_date <= end_date:
                    
                current_date_string = str(current_date)[0:10]
                if current_date_string not in tracker_map["processed_dates"]:                  
                                            
                    daily_log_file = str("delftx-edx-events-" + current_date_string + ".log.gz")                           
                    if os.path.exists(str(course_log_path + "/daily_logs/" + daily_log_file)):
                                                   
                        print daily_log_file
                                                                            
                        # Decompress log files
                        unzip_file_path = str(course_log_path + course_code + "/unzip_daily_logs/")
                        if not os.path.exists(unzip_file_path):
                            os.mkdir(unzip_file_path)
                            
                        output_path = str(unzip_file_path + daily_log_file[0:-3])
                            
                        if not os.path.exists(output_path):                        
                            output_file = open(output_path, 'w')
                            with gzip.open(str(course_log_path + "/daily_logs/" + daily_log_file), 'r') as f:
                                for line in f:
                                    jsonObject = json.loads(line)
                                    if course_id in jsonObject["context"]["course_id"]:
                                        output_file.write(line)                
                            output_file.close()    
                                                  
                        daily_log_path = output_path
                        
                        # Video_interaction table
                        # print "1.\t Video_interaction table processing..."        
                        remaining_video_interaction_log_path = course_log_path + course_code + "/remaining_video_interaction_logs"
                        video_interaction(metadata_path, daily_log_path, remaining_video_interaction_log_path, cursor)
                        
                        # Quiz mode
                        # print "2.\t Quiz mode processing..."  
                        quiz_mode(daily_log_path, cursor)
                        
                        # Quiz_sessions table
                        # print "3.\t Quiz_sessions table processing..."  
                        remaining_quiz_session_log_path = course_log_path + course_code + "/remaining_quiz_session_logs"
                        quiz_sessions(metadata_path, daily_log_path, remaining_quiz_session_log_path, cursor)
                        
                        # Forum_interaction table
                        # print "4.\t Forum_interaction table processing..."  
                        forum_interaction(metadata_path, daily_log_path, cursor)
                            
                        # Forum_sessions table
                        # print "5.\t Forum_sessions table processing..."  
                        remaining_forum_session_log_path = course_log_path + course_code + "/remaining_forum_session_logs"
                        forum_sessions(metadata_path, daily_log_path, remaining_forum_session_log_path, cursor)                     
                                        
                        # Sessions table
                        # print "6.\t Sessions table processing..."  
                        remaining_session_log_path = course_log_path + course_code + "/remaining_session_logs"
                        sessions(metadata_path, daily_log_path, remaining_session_log_path, cursor)                    
                                                                            
                        tracker_map["processed_dates"].append(current_date_string)
                            
                current_date = getNextDay(current_date)
                                            
            if len(tracker_map["processed_dates"]) == getDayDiff(start_date, end_date) + 1:
                tracker_map["status"] = True
                    
            if tracker_map["num_processed_dates"] != len(tracker_map["processed_dates"]):                
                tracker_map["num_processed_dates"] = len(tracker_map["processed_dates"])                
                output_file = open(tracker_path, "w")
                output_file.write(json.dumps(tracker_map))
                output_file.close()
            
            # Delete the decompressed files
            if remove_filtered_logs == "1":
                log_files = os.listdir(str(course_log_path + "/daily_logs/"))
                for log_file in log_files:
                    os.remove(str(course_log_path + "/daily_logs/" + log_file))
                    
        print
        



           
###############################################################################
if __name__ == '__main__':
    
    configFile = sys.argv[1:]
    if len(configFile) == 0:
        configFile = 'config'
    main(configFile)
    
    print "All finished."
                
    

    
    
    
    
    
    
    
    
