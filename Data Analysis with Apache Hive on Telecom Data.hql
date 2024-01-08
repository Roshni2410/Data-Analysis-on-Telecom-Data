-- Data Analysis with Apache Hive on Telecom Data


-- a. Download the dataset and load it into a Hive table.


-- 1. make a directory named input_data:
-- hdfs dfs -mkdir input_data 

-- 2. make a directory named Telecom in input_data: 
-- hdfs dfs -mkdir /input_data/Telecom

-- 3. load Telecom_customer_churn_data.csv file in telecom directory
-- hdfs dfs -put Telecom_customer_churn_data.csv /input_data/Telecom/

-- we are creating Internal (Managed) Table 

CREATE TABLE telecom_data (
customerID STRING,
gender STRING,
SeniorCitizen INT,
Partner STRING,
Dependents STRING,
tenure INT,
PhoneService STRING,
MultipleLines STRING,
InternetService STRING,
OnlineSecurity STRING,
OnlineBackup STRING,
DeviceProtection STRING,
TechSupport STRING,
StreamingTV STRING,
StreamingMovies STRING,
Contract STRING,
PaperlessBilling STRING,
PaymentMethod STRING,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input_data/Telecom/'
tblproperties ("skip.header.line.count" = "1");


-- b. Write a query to display the top 10 rows of the table.

SELECT * FROM telecom_data limit 10;



-- 2. Data Exploration (Beginner)


-- a. Write a HiveQL query to find the total number of customers in the dataset.


SELECT COUNT(customerID) FROM telecom_data;


-- b. Write a HiveQL query to find the number of customers who have churned.

SELECT COUNT(customerID) FROM telecom_data
WHERE Churn = 'Yes';


-- c. Analyze the distribution of customers based on gender and SeniorCitizen status.

SELECT gender, SeniorCitizen,COUNT(customerID)
FROM telecom_data
GROUP BY gender,SeniorCitizen;


-- d. Determine the total charge to the company due to churned customers

SELECT SUM(TotalCharges) 
FROM telecom_data
WHERE Churn = 'Yes';


-- 3. Data Analysis (Intermediate)

-- a. Write a HiveQL query to find the number of customers who have churned, grouped by their Contract type.

SELECT Contract,COUNT(customerID) FROM telecom_data
WHERE Churn = 'Yes'
GROUP BY Contract;

-- b. Write a HiveQL query to find the average MonthlyCharges for customers who have churned vs those who have not.

SELECT Churn,AVG(MonthlyCharges)
FROM telecom_data
GROUP BY Churn;

-- c. Determine the maximum, minimum, and average tenure of the customers.

SELECT customerID,MAX(tenure), MIN(tenure),AVG(tenure)
FROM telecom_data
GROUP BY customerID;

-- d. Find out which PaymentMethod is most popular among customers.

SELECT PaymentMethod,COUNT(PaymentMethod) 
FROM telecom_data
GROUP BY PaymentMethod
ORDER BY COUNT(PaymentMethod) DESC
LIMIT 1;

-- e. Analyze the relationship between PaperlessBilling and churn rate.

SELECT PaperlessBilling, COUNT(*)
FROM telecom_data
WHERE Churn = 'Yes'
GROUP BY PaperlessBilling;


-- 4. Partitioning (Intermediate)

-- a. Create a partitioned table by Contract and load the data from the original table.

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE telecom_data_partitioned
(customerID STRING,
gender STRING,
SeniorCitizen INT,
Partner STRING,
Dependents STRING,
tenure INT,
PhoneService STRING,
MultipleLines STRING,
InternetService STRING,
OnlineSecurity STRING,
OnlineBackup STRING,
DeviceProtection STRING,
TechSupport STRING,
StreamingTV STRING,
StreamingMovies STRING,
PaperlessBilling STRING,
PaymentMethod STRING,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING)
PARTITIONED BY (Contract STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE telecom_data_partitioned
PARTITION(Contract)
SELECT 
customerID,
gender,
SeniorCitizen,
Partner,
Dependents,
tenure,
PhoneService,
MultipleLines,
InternetService,
OnlineSecurity,
OnlineBackup,
DeviceProtection,
TechSupport,
StreamingTV,
StreamingMovies,
PaperlessBilling,
PaymentMethod,
MonthlyCharges,
TotalCharges,
Churn,
Contract
FROM telecom_data;


-- Always remember while overwriting data the partitoned column comes in end


-- b. Write a HiveQL query to find the number of customers who have churned in each Contract type using the partitioned table.

SELECT Contract,COUNT(*)
FROM telecom_data_partitioned
WHERE Churn = 'Yes'
GROUP BY Contract;

-- c. Find the average MonthlyCharges for each type of Contract using the partitioned table.

SELECT Contract, AVG(MonthlyCharges)
FROM telecom_data_partitioned
GROUP BY Contract;


-- d. Determine the maximum tenure in each Contract type partition.

SELECT Contract, MAX(tenure)
FROM telecom_data
GROUP BY Contract;


-- 5. Bucketing (Advanced)

-- a. Create a Sorted bucketed table by tenure into 6 buckets.

CREATE TABLE telecom_data_bucketed(
customerID STRING,
gender STRING,
SeniorCitizen INT,
Partner STRING,
Dependents STRING,
tenure INT,
PhoneService STRING,
MultipleLines STRING,
InternetService STRING,
OnlineSecurity STRING,
OnlineBackup STRING,
DeviceProtection STRING,
TechSupport STRING,
StreamingTV STRING,
StreamingMovies STRING,
Contract STRING,
PaperlessBilling STRING,
PaymentMethod STRING,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING
)
CLUSTERED BY (tenure)
SORTED BY (tenure)
INTO 6 buckets;


-- b. Load the data from the original table into the bucketed table.


INSERT OVERWRITE TABLE telecom_data_bucketed SELECT * FROM telecom_data;

-- c. Write a HiveQL query to find the average MonthlyCharges for customers in each bucket.

SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 1 out of 6 on tenure)
UNION ALL
SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 2 out of 6 on tenure)
UNION ALL 
SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 3 out of 6 on tenure)
UNION ALL
SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 4 out of 6 on tenure)
UNION ALL 
SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 5 out of 6 on tenure)
UNION ALL 
SELECT AVG(MonthlyCharges)
FROM telecom_data_bucketed
tablesample(bucket 6 out of 6 on tenure);


-- d. Find the highest TotalCharges in each tenure bucket.


SELECT 'Bucket 1' as Bucket, MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 1 out of 6 on tenure)
UNION ALL 
SELECT 'Bucket 2' as Bucket,MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 2 out of 6 on tenure)
UNION ALL 
SELECT 'Bucket 3' as Bucket,MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 3 out of 6 on tenure)
UNION ALL 
SELECT 'Bucket 4' as Bucket,MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 4 out of 6 on tenure)
UNION ALL 
SELECT 'Bucket 5' as Bucket,MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 5 out of 6 on tenure)
UNION ALL 
SELECT 'Bucket 6' as Bucket,MAX(TotalCharges)
FROM telecom_data_bucketed
tablesample(bucket 6 out of 6 on tenure);


-- 6. Performance Optimization with Joins (Advanced)


-- 1. make a directory named Demographics in input_data: 
-- hdfs dfs -mkdir /input_data/Demographics

-- 2. load CustomerDemographics.csv file in Demographics directory
-- hdfs dfs -put CustomerDemographics.csv /input_data/Demographics/

-- we are creating Internal (Managed) Table 

-- Assume another dataset, CustomerDemographics.csv, that contains the details of the demographic data of each customer.

-- 3. Load the demographics dataset into another Hive table.

CREATE TABLE demographics 
(customerID STRING,
City STRING,
Lat FLOAT,
Long FLOAT,
country STRING,
iso2 STRING,
State STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input_data/Demographics/'
tblproperties ("skip.header.line.count" = "1");


-- b. Write HiveQL queries to join the customer churn table and the demographics table on customerID using different types of joins- 
-- Common join, Map join, Bucket Map join,Sorted Merge Bucket join, Skew Join these Queries are running on telecom_data 


--  Common Join

SELECT t.customerID,d.country
FROM telecom_data t
JOIN demographics d 
ON t.customerID = d.customerID;

-- Time taken: 23.248 seconds, Fetched: 7043 row(s)
-- Time taken: 19.138 seconds, Fetched: 7043 row(s)

-- Map Side Join
-- it doesnt use reducers

SET hive.auto.convert.join=true; 

SELECT t.customerID,d.country
FROM telecom_data t
JOIN demographics d 
ON t.customerID = d.customerID;

-- Time taken: 19.248 seconds, Fetched: 7043 row(s)
-- Time taken: 18.149 seconds, Fetched: 7043 row(s)

-- Skew Join 

SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=50000;

SELECT t.customerID,d.country
FROM telecom_data t
JOIN demographics d 
ON t.customerID = d.customerID;

-- Time taken: 18.71 seconds, Fetched: 7043 row(s)
-- Time taken: 19.054 seconds, Fetched: 7043 row(s)


-- now on bucket data

-- Bucket Map Join

set hive.optimize.bucketmapjoin=true; 
SET hive.auto.convert.join=true;

SELECT t.customerID,d.country
FROM telecom_data_bucketed t
JOIN demographics d 
ON t.customerID = d.customerID;

-- Time taken: 18.343 seconds, Fetched: 7043 row(s)
-- Time taken: 10.563 seconds, Fetched: 7043 row(s)

-- Sorted Merge Bucket Map Join

set hive.auto.convert.sortmerge.join=true; 
set hive.optimize.bucketmapjoin.sortedmerge = true;


SELECT t.customerID,d.country
FROM telecom_data_bucketed t
INNER JOIN demographics d ON
t.customerID = d.customerID;

-- Time taken: 18.942 seconds, Fetched: 7043 row(s)
-- Time taken: 12.07 seconds, Fetched: 7043 row(s)

-- Skew Join 

SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=50000; // setting limit to 50,000

SELECT t.customerID,d.country
FROM telecom_data_bucketed t
INNER JOIN demographics d ON
t.customerID = d.customerID;

-- Time taken: 11.23 seconds, Fetched: 7043 row(s)
-- Time taken: 11.351 seconds, Fetched: 7043 row(s)

-- c. Observe and document the performance of each join type 

-- as you can see i have tried 
-- common join, Map Side Join, Bucket Map Join, Sorted Merge Bucket Map Join and skew join
-- and performance wise there was not much of a difference in common join (23.248 seconds) (19.138 seconds)
-- and map side join (19.248 seconds) (18.149 seconds)
-- but obviously map side join is little bit faster
-- but when i tried Bucket Map Join it was much faster than the previous two
-- with a query run time of (18.343 seconds) (10.563 seconds)
-- after that i tried Sorted Merge Bucket Map Join (18.942 seconds) (12.07 seconds)
-- now i tried skew join (18.71 seconds) for normal table and (11.23 seconds) for bucketed table.

-- so from the above results i can say that in general

-- Bucket Map Join < Skew Join < Sorted Merge Bucket Map Join < Map Side Join < Common Join 

-- with Bucket Map Join being the fastest and common join being the slowest.


-- 7. Advanced Analysis (Expert)

-- a. Find the distribution of PaymentMethod among churned customers.

SELECT PaymentMethod, COUNT(*)
FROM telecom_data_partitioned
WHERE Churn = 'Yes'
GROUP BY PaymentMethod;


-- b. Calculate the churn rate (percentage of customers who left) for each InternetService category.

SELECT InternetService,
ROUND(
    COUNT(CASE WHEN Churn = 'Yes' THEN 1 END)*100/COUNT(*),2
) as ChurnRate
FROM telecom_data_partitioned
GROUP BY InternetService
ORDER BY ChurnRate DESC;





-- c. Find the number of customers who have no dependents and have churned, grouped by Contract type.

SELECT Contract,COUNT(*)
FROM telecom_data_partitioned
WHERE Dependents = 'No' AND Churn = 'Yes'
GROUP BY Contract;


-- d. Find the top 5 tenure lengths that have the highest churn rates.

SELECT tenure, ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM telecom_data WHERE Churn = 'Yes'),2) as Churned
FROM telecom_data
WHERE Churn = 'Yes'
GROUP BY Tenure 
ORDER BY Churned DESC 
LIMIT 5;


-- e. Calculate the average MonthlyCharges for customers who have PhoneService and have churned, grouped by Contract type.

SELECT Contract,AVG(MonthlyCharges) as AverageMonthly
FROM telecom_data_bucketed
WHERE PhoneService = 'Yes' AND Churn = 'Yes'
GROUP BY Contract
ORDER BY AverageMonthly DESC;



-- f. Identify which InternetService type is most associated with churned
customers.

SELECT InternetService,COUNT(*) as Churned
FROM telecom_data
WHERE Churn = 'Yes'
GROUP BY InternetService
ORDER BY Churned DESC
LIMIT 1;




-- g. Determine if customers with a partner have a lower churn rate compared to those without.

SELECT 'With Partner' as Partner, 
ROUND(
    COUNT(CASE WHEN Partner = 'Yes' AND Churn = 'Yes' THEN 1 END)*100.00/COUNT(*),2
)
FROM telecom_data
UNION ALL 
SELECT 'Without Partner' as Partner, 
ROUND(
    COUNT(CASE WHEN Partner = 'No' AND Churn = 'Yes' THEN 1 END)*100.00/COUNT(*),2
)
FROM telecom_data;


-- h. Analyze the relationship between MultipleLines and churn rate.


SELECT 'No' as MultipleLines,
ROUND(
    COUNT(CASE WHEN Churn = 'Yes' AND MultipleLines = 'No' THEN 1 END)*100.00/COUNT(*),2
) as ChurnRate
FROM telecom_data
UNION ALL 
SELECT 'Yes' as MultipleLines,
ROUND(
    COUNT(CASE WHEN Churn = 'Yes' AND MultipleLines = 'Yes' THEN 1 END)*100.00/COUNT(*),2
) as ChurnRate
FROM telecom_data
UNION ALL
SELECT 'No phone service' as MultipleLines,
ROUND(
    COUNT(CASE WHEN Churn = 'Yes' AND MultipleLines = 'No phone service' THEN 1 END)*100.00/COUNT(*),2
) as ChurnRate
FROM telecom_data;



