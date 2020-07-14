-- Create the tables
DROP TABLE IF EXISTS hadoop_2020_group_e.transactions;
CREATE TABLE IF NOT EXISTS hadoop_2020_group_e.transactions (step INT, type STRING, amount BIGINT, nameOrig STRING, oldbalanceOrg BIGINT, newbalanceOrig BIGINT, nameDest STRING, oldbalanceDest BIGINT, newbalanceDest BIGINT, isFraud INT, isFlaggedFraud INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Upload the data into the table
LOAD DATA INPATH 'hadoop_assignment/PS_20174392719_1491204439457_log.csv' OVERWRITE INTO TABLE hadoop_2020_group_e.transactions ;

--Check content at the database
Select * FROM hadoop_2020_group_e.transactions LIMIT 5;