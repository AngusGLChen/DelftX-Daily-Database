'''
Created on Aug 11, 2016

@author: Angus
'''

import csv


'''
This function retrieves the learners' anonymized ID used in the pre/post surveys. 
'''
def RetrieveSurveyLearnerIDIndex(survey_path):
    
    input_file = open(survey_path, "r")
    reader = csv.reader(input_file)
    id_row = reader.next()
    # description_row = reader.next()
    
    for index in range(len(id_row)):      
        if id_row[index] == "user_id":
            print "The index is:\t" + str(index)
        
        
survey_path = ""
RetrieveSurveyLearnerIDIndex(survey_path)