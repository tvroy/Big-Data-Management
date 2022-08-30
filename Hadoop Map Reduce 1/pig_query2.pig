cust = LOAD 'cust_file.txt' USING PigStorage(',') AS (CustId:int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
cust_group = GROUP cust by CountryCode;
cust_count = FOREACH cust_group GENERATE group, COUNT(cust.Name) as c;
filtered = FILTER cust_count by c<2000 or c>5000;
STORE filtered INTO 'pigq2';
