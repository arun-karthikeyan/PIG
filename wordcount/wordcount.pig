--my first pig script ;)
--the following pig script is for a basic wordcount program
--this script is to be run using the mapreduce mode
--place input files in /user/cloudera/pig/wordcount/input/
--output files are generated in /user/cloudera/pig/wordcount/output

--loads the input files in the specified directory into the relation input_file
input_file = LOAD '/user/cloudera/pig/wordcount/input/*' USING PigStorage('\n') AS (linesin:chararray);

--tokenizes the contents of the relation input_file by placing each word in input_file in a new line
input_flat = FOREACH input_file GENERATE FLATTEN(TOKENIZE(linesin)) AS wordin;

--groups the inputs by fieldname 'wordin'
input_grpd = GROUP input_flat BY wordin;

--counts the size of each group from relation input_grpd
word_counts = FOREACH input_grpd GENERATE group,COUNT(input_flat.wordin);

--sorts the contents of the relation word_counts by the fieldname group
word_count_sorted = ORDER word_counts BY group;

--stores the contents of the relation word_count_sorted into the specified directory
STORE word_count_sorted into '/user/cloudera/pig/wordcount/output';
