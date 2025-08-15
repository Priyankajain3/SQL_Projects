CREATE TABLE banking_data (
    Customer_ID VARCHAR(100),
    Customer_Name VARCHAR(255),
    Account_Number BIGINT,
    Transaction_Date DATE,
    Transaction_Type VARCHAR(10),
    Amount DECIMAL(12,2),
    Balance DECIMAL(12,2),
    Description VARCHAR(255),
    Branch VARCHAR(100),
    Transaction_Method VARCHAR(50),
    Currency VARCHAR(10),
    Bank_Name VARCHAR(100),
    High_Risk_Flag VARCHAR(20)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Debit and Credit banking_data.csv'
INTO TABLE banking_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Customer_ID, Customer_Name, Account_Number, @Transaction_Date, Transaction_Type, Amount, Balance, Description, Branch, Transaction_Method, Currency, Bank_Name, High_Risk_Flag)
SET
    Transaction_Date = STR_TO_DATE(TRIM(@Transaction_Date), '%Y-%m-%d');

# uploded data 

select * from banking_data ;

 # 1. Total Credit Amount

SELECT SUM(Amount) AS Total_Credit_Amount
FROM banking_data
WHERE Transaction_Type = 'Credit';


# 2. Total Debit Amount

SELECT SUM(Amount) AS Total_Debit_Amount
FROM banking_data
WHERE Transaction_Type = 'Debit';

 # 3. Credit to Debit Ratio
 SELECT 
    ROUND(
        SUM(CASE WHEN Transaction_Type = 'Credit' THEN Amount ELSE 0 END) /
        NULLIF(SUM(CASE WHEN Transaction_Type = 'Debit' THEN Amount ELSE 0 END), 0),
    2) AS Credit_to_Debit_Ratio
FROM banking_data;


# 4-Net Transaction Amount:
SELECT 
    ROUND(
        SUM(CASE WHEN Transaction_Type = 'Credit' THEN Amount ELSE 0 END) -
        SUM(CASE WHEN Transaction_Type = 'Debit' THEN Amount ELSE 0 END),
    2) AS Net_Transaction_Amount
FROM banking_data;


# 5-Account Activity Ratio
SELECT 
    Account_Number,
    COUNT(*) AS Total_Transactions,
    MAX(Balance) AS Latest_Balance,
    ROUND(COUNT(*) / NULLIF(MAX(Balance), 0), 4) AS Account_Activity_Ratio
FROM banking_data
GROUP BY Account_Number;


#6-Transactions per Day/Week/Month:
# day
SELECT 
    Transaction_Date,
    COUNT(*) AS Transactions_Per_Day
FROM banking_data
GROUP BY Transaction_Date
ORDER BY Transaction_Date;

# month
SELECT 
    DATE_FORMAT(Transaction_Date, '%Y-%m') AS Month,
    COUNT(*) AS Transactions_Per_Month
FROM banking_data
GROUP BY Month
ORDER BY Month;

# 7-Total Transaction Amount by Branch:
SELECT 
    Branch,
    SUM(Amount) AS Total_Transaction_Amount
FROM banking_data
GROUP BY Branch
ORDER BY Total_Transaction_Amount DESC;

# 8-Transaction Volume by Bank:
SELECT 
    Bank_Name,
    SUM(Amount) AS Total_Transaction_Amount
FROM banking_data
GROUP BY Bank_Name
ORDER BY Total_Transaction_Amount DESC;


# 9-Transaction Method Distribution:
 SELECT 
    Transaction_Method,
    COUNT(*) AS Method_Count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM banking_data), 2) AS Method_Percentage
FROM banking_data
GROUP BY Transaction_Method
ORDER BY Method_Count DESC;

 # 10-Branch Transaction Growth:
WITH MonthlyBranchTotals AS (
    SELECT 
        Branch,
        DATE_FORMAT(Transaction_Date, '%Y-%m') AS Month,
        SUM(Amount) AS Total_Amount
    FROM banking_data
    GROUP BY Branch, Month
),
GrowthCalc AS (
    SELECT 
        Branch,
        Month,
        Total_Amount,
        LAG(Total_Amount) OVER (PARTITION BY Branch ORDER BY Month) AS Prev_Month_Amount
    FROM MonthlyBranchTotals
)
SELECT 
    Branch,
    Month,
    Total_Amount,
    Prev_Month_Amount,
    ROUND(
        100.0 * (Total_Amount - Prev_Month_Amount) / NULLIF(Prev_Month_Amount, 0),
        2
    ) AS Percent_Growth
FROM GrowthCalc
ORDER BY Branch, Month;

 # 11-High-Risk Transaction Flag:
 SET SQL_SAFE_UPDATES = 0;

UPDATE banking_data
SET High_Risk_Flag = 
    CASE 
        WHEN Amount > 4000 THEN 'High-Risk'
        WHEN Transaction_Type = 'Debit' AND Amount > 4000 THEN 'High-Risk'
        ELSE 'Normal'
    END;

SELECT 
    Bank_Name,
    COUNT(*) AS High_Risk_Transaction_Count
FROM banking_data
WHERE High_Risk_Flag = 'High-Risk'
GROUP BY Bank_Name
ORDER BY High_Risk_Transaction_Count DESC;


 # 12-Suspicious Transaction Frequency:
SELECT 
    DATE_FORMAT(Transaction_Date, '%Y-%m') AS Month,
    COUNT(*) AS High_Risk_Transaction_Count
FROM banking_data
WHERE High_Risk_Flag = 'High-Risk'
GROUP BY DATE_FORMAT(Transaction_Date, '%Y-%m')
ORDER BY Month;

