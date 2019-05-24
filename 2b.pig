register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject
subjects = group ntriples by (subject) PARALLEL 50;

count1 = FOREACH subjects GENERATE COUNT($1);
count2 = group count1 by $0;

pairs = FOREACH count2 GENERATE $0, COUNT($1);

store pairs into '/user/hadoop/q2-result' using PigStorage();