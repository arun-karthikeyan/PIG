--Load a CSV
--since pig doesn't have a base csv loader, we need to register and define the csv loader function
--register the jar
register /usr/lib/pig/piggybank.jar;

--define the CSVLoader function
define CSVLoader 
org.apache.pig.piggybank.storage.CSVLoader();

yelp_data_raw = LOAD '/user/cloudera/pig/yelp-data-analysis/input/yelp_data.csv' USING CSVLoader() AS (
business_id: chararray, 
cool, 
date, 
funny, 
id, 
stars: int, 
text: chararray, 
type, 
useful: int, 
user_id, 
name, 
full_address, 
latitude, 
longitude, 
neighborhoods, 
open, 
review_count, 
state
);

--perform filtering to ensure math operations on the specified fields does not return a null
yelp_data = FILTER yelp_data_raw BY (useful IS NOT NULL AND stars IS NOT NULL);

--clipping field values
yelp_rate = FOREACH yelp_data GENERATE business_id, stars, (useful>5 ? 5 : useful) as useful_clipped;

--adding the weighted stars field
yelp_rate2 = FOREACH yelp_rate GENERATE $0.., (double)stars*(useful_clipped/5) AS wtd_stars;

--GROUP BY business id
yelp_grouped = GROUP yelp_rate2 BY business_id;



--generate the average that we are interested in 

yelp_modified = FOREACH yelp_grouped GENERATE 
group AS business_id_group, 
COUNT(yelp_rate2.stars) AS num_ratings, 
AVG(yelp_rate2.stars) AS avg_stars, 
AVG(yelp_rate2.useful_clipped) AS avg_useful, 
AVG(yelp_rate2.wtd_stars) AS avg_wtdstars;

-- then we rank it
yelp_ranks = RANK yelp_modified BY avg_wtdstars DESC;

-- store the output
STORE yelp_ranks INTO '/user/cloudera/pig/yelp-data-analysis/output/';

-- assignment question 2
--find the average of average weighted stars across all business that have a rating of more than 1, strategy 1
yelp_modified2 = FILTER yelp_modified BY (num_ratings>1);
yelp_group_all = GROUP yelp_modified2 ALL;
yelp_avg_avgwtdstr = FOREACH yelp_group_all GENERATE AVG(yelp_modified2.avg_wtdstars);
STORE yelp_avg_avgwtdstr INTO '/user/cloudera/pig/yelp-data-analysis/output2/';

--assignment question 3
--Strategy 2 to find the average weighted stars across all business that have a rating of more than 1
yelp_jnd = JOIN yelp_rate2 BY business_id, yelp_modified2 BY business_id_group;
yelp_jnd_group_all = GROUP yelp_jnd ALL;
yelp_avg_avgwtdstr2 = FOREACH yelp_jnd_group_all GENERATE AVG(yelp_jnd.yelp_modified2::avg_wtdstars);
STORE yelp_avg_avgwtdstr2 INTO '/user/cloudera/pig/yelp-data-analysis/output3/';

--assignment question 5
--Alternative to join for strategy 1
yelp_grouped_2 = FOREACH yelp_grouped GENERATE COUNT(yelp_rate2.stars) AS num_ratings, yelp_rate2.wtd_stars AS wtd_stars;
yelp_modified3 = FILTER yelp_grouped_2 BY (num_ratings>1);
yelp_modified4 = FOREACH yelp_modified3 GENERATE FLATTEN(wtd_stars.wtd_stars) AS wtd_stars;
yelp_m4_grpd_all = GROUP yelp_modified4 ALL;
yelp_avgwtdstr = FOREACH yelp_m4_grpd_all GENERATE AVG(yelp_modified4.wtd_stars) AS avg_wtd_strs;
STORE yelp_avgwtdstr INTO '/user/cloudera/pig/yelp-data-analysis/output4/';

--writing everything to local output folder
--ranks of businesses according to weighted stars
fs -getmerge /user/cloudera/pig/yelp-data-analysis/output/* /home/cloudera/pig/yelp-data-analysis/output/output1.txt;
--average weighted stars across all businesses strategy 1
fs -getmerge /user/cloudera/pig/yelp-data-analysis/output2/* /home/cloudera/pig/yelp-data-analysis/output/output2.txt;
--average weighted stars across all businesses strategy 2
fs -getmerge /user/cloudera/pig/yelp-data-analysis/output3/* /home/cloudera/pig/yelp-data-analysis/output/output3.txt;
--average weighted stars across all businesses strategy 1 without join
fs -getmerge /user/cloudera/pig/yelp-data-analysis/output4/* /home/cloudera/pig/yelp-data-analysis/output/output4.txt;
