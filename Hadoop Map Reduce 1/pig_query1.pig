trans = LOAD 'trans_file.txt' USING PigStorage (',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:charArray);
cust = LOAD 'cust_file.txt' USING PigStorage (',') AS (CustID:int, Name:charArray, Age:int, Gender:charArray, CountryCode:int, Salary:float);
joined = JOIN trans by CustID, cust by CustID;
grouped = GROUP joined BY Name;
trans_count = FOREACH grouped GENERATE group, COUNT(joined.TransID) as c;
min_trans = ORDER trans_count by c ASC;
STORE min_trans INTO 'pigq1';

