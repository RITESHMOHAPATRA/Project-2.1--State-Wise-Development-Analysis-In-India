REGISTER /home/acadgild/install/pig/pig-0.16.0/lib/piggybank.jar;
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath;
data = load 'hdfs://localhost:8020/flume_import/StatewiseDistrictwisePhysicalProgress.xml' 
using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);
req_data = foreach data generate XPath(x,'row/State_Name') as state, XPath(x,'row/District_Name') as DistrictName, XPath(x,'row/Project_Objectives_IHHL_BPL') as POIHHLBPL, XPath(x,'row/Project_Performance-IHHL_BPL') as PPIHHLBPL;
filter_null = filter req_data by (PPIHHLBPL is not null) AND (POIHHLBPL is not null);
centPercentObjective = filter filter_null by ((int)PPIHHLBPL==(int)POIHHLBPL);
districts = foreach centPercentObjective generate state,DistrictName,POIHHLBPL,PPIHHLBPL;
store districts into 'hdfs://localhost:8020/pig_output/districtwiseachieved' using PigStorage(',');
