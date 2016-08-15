'''
Created on Aug 10, 2016

@author: Angus
'''

import os, csv, sys
from translation.Functions import ExtractCourseInformation

reload(sys)
sys.setdefaultencoding('utf-8')


def survey_mode(metadata_path, survey_path, cursor, pre_id_index, post_id_index):
        
    # Collect course information
    course_metadata_map = ExtractCourseInformation(metadata_path)
    course_id = course_metadata_map["course_id"]
    
    learner_id_map = {}
    
    files = os.listdir(survey_path)
       
    # Processing ID information
    for file in files:
        if "anon-ids.csv" in file:
            id_file = open(str(survey_path + file), "r")
            id_reader = csv.reader(id_file)
            id_reader.next()
            for row in id_reader:
                global_learner_id = row[0].replace("\"","")
                anonymized_id = row[1].replace("\"","")
                learner_id_map[anonymized_id] = global_learner_id
    
    # Processing Pre-survey information      
    for file in files:    
        if "pre-survey" in file:
            pre_file = open(str(survey_path + file), "r")
            pre_reader = csv.reader(pre_file)
            
            question_id_row = pre_reader.next()
            question_description_row = pre_reader.next()
                                               
            for i in range(len(list(question_id_row))):
                question_id = course_id + "_pre_" + str(i) + "_" + question_id_row[i].replace("\"","")
                question_description = question_description_row[i].replace("\'", "\\'")
                array = [question_id, course_id, "pre", question_description]
                sql = "insert into survey_descriptions (question_id, course_id, question_type, question_description) values (%s,%s,%s,%s)"        
                cursor.execute(sql, array)
                
            for row in pre_reader:
                learner_id = row[pre_id_index]
                if learner_id in learner_id_map.keys():
                    course_learner_id = course_id + "_" + learner_id_map[learner_id]
                    
                    for i in range(len(list(question_id_row))):
                        question_id = course_id + "_pre_" + question_id_row[i].replace("\"","")
                        response_id = course_learner_id + "_" + "pre" + "_" + question_id_row[i].replace("\"","")
                        answer = row[i]                        
                        array = [response_id, course_learner_id, question_id, answer]
                        sql = "replace into survey_responses (response_id, course_learner_id, question_id, answer) values (%s,%s,%s,%s)"
                        cursor.execute(sql, array)       
            pre_file.close()

    # Processing Post-survey information      
    for file in files:    
        if "post-survey" in file:
            post_file = open(str(survey_path + file), "r")
            post_reader = csv.reader(post_file)
            
            question_id_row = post_reader.next()
            question_description_row = post_reader.next()
                                               
            for i in range(len(list(question_id_row))):
                question_id = course_id + "_post_" + str(i) + "_" + question_id_row[i].replace("\"","")
                question_description = question_description_row[i].replace("\'", "\\'")
                array = [question_id, course_id, "post", question_description]
                sql = "insert into survey_descriptions (question_id, course_id, question_type, question_description) values (%s,%s,%s,%s)"
                cursor.execute(sql, array)
                
            for row in post_reader:
                learner_id = row[post_id_index]
                if learner_id in learner_id_map.keys():
                    course_learner_id = course_id + "_" + learner_id_map[learner_id]
                    
                    for i in range(len(list(question_id_row))):
                        question_id = course_id + "_post_" + question_id_row[i].replace("\"","")
                        response_id = course_learner_id + "_post_" + question_id_row[i].replace("\"","")
                        answer = row[i]                        
                        array = [response_id, course_learner_id, question_id, answer]
                        sql = "replace into survey_responses (response_id, course_learner_id, question_id, answer) values (%s,%s,%s,%s)"
                        cursor.execute(sql, array)
            post_file.close()
            
                