REGISTER /home/acadgild/install/pig/pig-0.16.0/lib/piggybank.jar;
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath;
REGISTER '/home/acadgild/RITESH/STATE/Filter80.jar';
data = load 'hdfs://localhost:8020/flume_import/StatewiseDistrictwisePhysicalProgress.xml' 
using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);
req_data = foreach data generate XPath(x,'row/State_Name') as state, XPath(x,'row/District_Name') as DistrictName, XPath(x,'row/Project_Objectives_IHHL_BPL') as POIHHLBPL, XPath(x,'row/Project_Performance-IHHL_BPL') as PPIHHLBPL;
filter_null = filter req_data by (PPIHHLBPL is not null) AND (POIHHLBPL is not null);
percentObjective = foreach filter_null  generate state,DistrictName,ROUND_TO(pigUDF.Filter80((double)PPIHHLBPL,(double)POIHHLBPL),2) as BPL_Percent;
percent80 = FILTER percentObjective BY BPL_Percent>=80.00;
store percent80 into 'hdfs://localhost:8020/pig_output/percent80achieved' using PigStorage(',');
