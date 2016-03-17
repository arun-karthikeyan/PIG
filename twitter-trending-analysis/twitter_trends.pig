--An example script using JSON Loader for Twitter Trend Analysis from sampled data
--to be run in 'pig -x mapreduce' mode


--Assuming the input twitter json data is available in the following location in the hdfs
twitter_data = LOAD '/user/cloudera/pig/twitter-trending-analysis/input/twitter_data.txt' USING JsonLoader('created_time: chararray, text: chararray, user_id: chararray, id: chararray, created_date: chararray');

--Extracting a sample of the twitter data, here 0.1 is the sample rate
twitter_sample = SAMPLE twitter_data 0.1;

--filtering data that match a particular date
twitter_sample_filtered = FILTER twitter_sample BY created_date MATCHES 'Mon Oct 08 2012';

--to get count of each word, we first have to flatten based on text tokens
twitter_sample_words = FOREACH twitter_sample_filtered GENERATE FLATTEN(TOKENIZE(text)) AS words;

--then we have to group them according to the words
twitter_sample_grpd = GROUP twitter_sample_words BY words;

--now we can get the count of all the grouped words
twitter_sample_count = FOREACH twitter_sample_grpd GENERATE group AS word, COUNT(twitter_sample_words.words) AS count;

--now we have to get the overall word count from the sample for all dates, which we can achieve by just repeating the above steps on the unfiltered sample data
twitter_sample_all_words = FOREACH twitter_sample GENERATE FLATTEN(TOKENIZE(text)) AS words;
twitter_sample_all_grpd = GROUP twitter_sample_all_words BY words;
twitter_sample_all_count = FOREACH twitter_sample_all_grpd GENERATE group AS word, COUNT(twitter_sample_all_words.words) AS count;

--now we need to get the total different days available in the sample to calculate the expectation
--first we have to group by all to get all fields into a single row
twitter_sample_grp_by_all = GROUP twitter_sample ALL;

--now we get the count of the distinct values in the created_date field, which will give us the count of the different dates available
twitter_sample_date_count = FOREACH twitter_sample_grp_by_all {
distinct_dates = DISTINCT twitter_sample.created_date; 
GENERATE COUNT(distinct_dates) AS date_count;
};

--now to calculate the expectation
--we have to join the word-counts for the particular date and the word-counts throughout the sample
twitter_sample_jnd = JOIN twitter_sample_count BY word, twitter_sample_all_count BY word;

--now to calculate the actual expectation
twitter_sample_expectations = FOREACH twitter_sample_jnd {
	date_freq = (double) twitter_sample_count::count;
	exp_freq = ((double) twitter_sample_all_count::count)/twitter_sample_date_count.date_count;
	unexpected = date_freq - exp_freq;
	GENERATE twitter_sample_count::word AS word, date_freq AS date_freq, exp_freq AS exp_freq, unexpected AS unexpected;
};

--order by unexpected values
twitter_sample_trend = ORDER twitter_sample_expectations BY unexpected DESC;

--Store the results in specified output directory
STORE twitter_sample_trend into '/user/cloudera/pig/twitter-trending-analysis/output';
