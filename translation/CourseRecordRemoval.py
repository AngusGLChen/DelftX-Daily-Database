'''
Created on Aug 10, 2016

@author: Angus
'''

import os
from translation.Functions import ExtractCourseInformation


def RemoveCourseRecords(course_log_path, course_code, mode, cursor):
    
    # Retrieve all of the course_learner_id
    metadata_path = str(course_log_path + course_code + "/metadata/")
    course_metadata_map = ExtractCourseInformation(metadata_path)
    course_id = course_metadata_map["course_id"]
    
    course_learner_id_set = set()
    
    sql = "SELECT learner_index.course_learner_id FROM learner_index where learner_index.course_id = '" + course_id + "';"
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        course_learner_id = result[0]
        course_learner_id_set.add(course_learner_id)
    
    if mode == "log":
        tables = ["video_interaction", "submissions", "assessments", "quiz_sessions", "forum_interaction", "forum_sessions", "sessions"]
        for table in tables:
            for course_learner_id in course_learner_id_set:
                sql = "delete from " + table + " where " + table + ".course_learner_id = '" + course_learner_id + "';"
                cursor.execute(sql)
                
        # Remove all of the temporary files
        files = os.listdir(str(course_log_path + course_code + "/"))
        for file in files:
            if not os.path.isdir(str(course_log_path + course_code + "/" + file)):
                os.remove(str(course_log_path + course_code + "/" + file))     
                
    if mode == "metadata":        
        # Quiz_questions table
        sql = "delete quiz_questions from quiz_questions, course_elements where course_elements.course_id = '" + course_id + "' and course_elements.element_id = quiz_questions.question_id;"
        cursor.execute(sql)
        
        tables = ["courses", "course_elements"] 
        for table in tables:
            sql = "delete from " + table + " where " + table + ".course_id = '" + course_id + "';"
            cursor.execute(sql)
        
        tables = ["course_learner", "learner_demographic", "learner_index"] 
        for table in tables:
            for course_learner_id in course_learner_id_set:
                sql = "delete from " + table + " where " + table + ".course_learner_id = '" + course_learner_id + "';"
                cursor.execute(sql)
                
    if mode == "survey":
        # Survey_descriptions table
        sql = "delete from survey_descriptions where survey_descriptions.course_id = '" + course_id + "';"
        cursor.execute(sql)        
        # Survey_responses table
        for course_learner_id in course_learner_id_set:
            sql = "delete from survey_responses where survey_responses.course_learner_id = '" + course_learner_id + "';"
            cursor.execute(sql)
            
            
            
            