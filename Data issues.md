# Issues encountered when building the database

### 1. The forum-related events (including ```edx.forum.comment.created```, ```edx.forum.response.created```, ```edx.forum.thread.created```) are added on 5 March 2015. That means, for courses started prior to March 2015, we should use the .mongo file contained in the course metadata folder to extract learners' forum interaction records. As there are courses running around 5 March 2015, in order to avoid inserting duplicate ```forum_interaction``` records with the same key, we use ```replace``` when adding new records.

### 2. Some event logs have no/empty/None ```user_id``` value. We skip these logs when building the database.

### 3. Some event logs have ```user_id``` value that are not contained in the relevant course metadata files (e.g., ```DelftX-``course\_code```-student_courseenrollment-prod-analytics.sql```). This would cause problems when updating the database (e.g., re-processing the data of a course). Therefore, we use the try/except handling in calculating the ```sessions```, ```forum_sessions```, ```quiz_sessions``` and ```video_interaction``` tables.

### 4. For quiz\_interaction table, some ``problem_check`` events have almost the same content (only slightly different in their time, like ```2015-09-10T08:54:09.927618+00:00``` vs. ```2015-09-10T08:54:09.014793+00:00```), which might cause duplicate submission_id/assessment_id issue. So we used the try/except handling here. One possible reason for such event records might be some learners just keep clicking the "Check" button in a very short time or cased by network issues.

### 5 Not all the problems in courses are tagged with correct weights. For example, the only 2 point question in course Functional Programming (FP101x-3T2015) do not have a explicit weight, so we treated its weight as 1.0 at the beginning. It also happens in course_structure data of course Data Analysis(EX101x-3T2015). Solution: We manually correct the weight of each problems in courses Functional Programming and Data Analysis based on the points students got and the current course settings on edX.

### 6. The timestamps of submissions may be later than the due. At the beginning, we filtered all the submissions which submitted later than the due. However, we found that those submissions are counted by manually checking the submission time and the corresponding grades students got.

### 7. How to calculate the real grades students got in their submissions?

* In the records of student submissions and assessment of each problem in daily logs, there are two fields named grade and max_grade. max_grade means the number of blanks need to be filled. grade means the number of correct blanks students submitted.
* In the metadata, each problem has their own weights.
* real_grade = weight * ( grade / max_grade )

### 8. Not all the passed students have enough records in assessment. In the course Functional Programming (FP101x_3T2015), 8/1143 passing students have less than 170 points in the final week. In the course Data Analysis (EX101x_3T2015), 11/1156 passing students have less than 105 points in the final week. By manually checking daily logs of corresponding courses, all the grades of those students in daily logs are correctly loaded into the database. 
