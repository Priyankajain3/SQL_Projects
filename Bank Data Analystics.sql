-- Create the main banking data table with initial structure
CREATE TABLE banking_data1 (
    State_Abbr VARCHAR(100),
    Account_ID VARCHAR(100),
    Age VARCHAR(50),  -- Stored as VARCHAR to handle ranges like "20-25"
    BH_Name VARCHAR(100),
    Bank_Name VARCHAR(100),
    Branch_Name VARCHAR(100),
    Caste VARCHAR(100),
    Center_Id VARCHAR(100),
    City VARCHAR(100),
    Client_id VARCHAR(100),
    Client_Name VARCHAR(100),
    Close_Client VARCHAR(100),
    Closed_Date DATE,
    Credit_Officer_Name VARCHAR(100),
    Date_of_Birth DATE,
    Disb_By VARCHAR(100),
    Disbursement_Date DATE,
    Disbursement_Date_Years VARCHAR(10),
    Gender_ID VARCHAR(100),
    Home_Ownership VARCHAR(100),
    Loan_Status VARCHAR(100),
    Loan_Transfer_Date VARCHAR(20),
    NextMeetingDate DATE,
    Product_Code VARCHAR(100),
    Grade VARCHAR(100),
    Sub_Grade VARCHAR(20),
    Product_Id VARCHAR(100),
    Purpose_Category VARCHAR(100),
    Region_Name VARCHAR(100),
    Religion VARCHAR(100),
    Verification_Status VARCHAR(100),
    State_Abbr_2 VARCHAR(100),
    State_Name VARCHAR(100),
    Transfer_Logic VARCHAR(100),
    Is_Delinquent_Loan TINYINT,
    Is_Default_Loan TINYINT,
    Age_T INT,
    Delinq_2_Yrs INT,
    Application_Type VARCHAR(100),
    Loan_Amount DECIMAL(15,2),
    Funded_Amount DECIMAL(15,2),
    Funded_Amount_Inv DECIMAL(15,2),
    Term VARCHAR(100),
    Int_Rate DECIMAL(5,2),
    Total_Pymnt DECIMAL(15,2),
    Total_Pymnt_Inv DECIMAL(15,2),
    Total_Rec_Prncp DECIMAL(15,2),
    Total_Fees DECIMAL(15,2),
    Total_Rec_Int DECIMAL(15,2),
    Total_Rec_Late_Fee DECIMAL(15,2),
    Recoveries DECIMAL(15,2),
    Collection_Recovery_Fee DECIMAL(15,2),
    Disbursed_Amount DECIMAL(15,2),
    Disbursed_Date DATE,
    EMI DECIMAL(15,2),
    Disbursed_Year INT,
    Insurance_Amount DECIMAL(15,2),
    Insurance_Premium DECIMAL(15,2),
    Interest_Paid DECIMAL(15,2),
    Interest_Rate DECIMAL(5,2),
    Reg_Date DATE,
    Recovery_Fee DECIMAL(15,2)
);


-- Load cleaned data from the CSV into banking_data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Bank Data Analystics.csv'
INTO TABLE banking_data1
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'  
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    state_abbr, account_id, @age, bh_name, bank_name, branch_name, caste,
    center_id, city, client_id, client_name, close_client, 
    @closed_date, @date_of_birth, @credit_officer_name, disb_by, 
    @disbursement_date, @disbursement_date_years, gender_id, home_ownership, loan_status, 
    @loan_transfer_date, @next_meeting_date, product_code, @grade, sub_grade, 
    product_id, purpose_category, region_name, religion, verification_status,
    state_abbr_2, state_name, @transfer_logic, @is_delinquent_loan, @is_default_loan,
    @age_t, delinq_2_yrs, application_type, 
    @loan_amount, @funded_amount, @funded_amount_inv,
    term, @int_rate, @total_pymnt, @total_pymnt_inv, @total_rec_prncp, 
    @total_fees, @total_rec_int, @total_rec_late_fee, @recoveries, @collection_recovery_fee
)
SET
    -- Clean and transform age
    age = CASE
        WHEN @age LIKE '%-%' THEN CAST(SUBSTRING_INDEX(@age, '-', 1) AS UNSIGNED)
        WHEN @age REGEXP '^[0-9]+$' THEN CAST(@age AS UNSIGNED)
        ELSE NULL
    END,
    
    -- Handle nullable and inconsistent dates
    closed_date = IF(@closed_date IN ('', 'No', 'N/A') OR @closed_date NOT LIKE '%-%-%', NULL, STR_TO_DATE(@closed_date, '%d-%m-%Y')),
    date_of_birth = IF(@date_of_birth IN ('', 'No', 'N/A') OR @date_of_birth NOT LIKE '%-%-%', NULL, STR_TO_DATE(@date_of_birth, '%d-%m-%Y')),
    disbursement_date = IF(@disbursement_date IN ('', 'No', 'N/A') OR @disbursement_date NOT LIKE '%-%-%', NULL, STR_TO_DATE(@disbursement_date, '%d-%m-%Y')),
    disbursement_date_years = NULLIF(TRIM(@disbursement_date_years), ''),
    loan_transfer_date = IF(@loan_transfer_date IN ('', 'No', 'N/A') OR @loan_transfer_date NOT LIKE '%-%-%', NULL, STR_TO_DATE(@loan_transfer_date, '%d-%m-%Y')),
    nextmeetingdate = IF(@next_meeting_date IN ('', 'No', 'N/A') OR @next_meeting_date NOT LIKE '%-%-%', NULL, STR_TO_DATE(@next_meeting_date, '%d-%m-%Y')),

    -- Clean string-based columns
    credit_officer_name = NULLIF(@credit_officer_name, ''),
    grade = NULLIF(@grade, ''),
    transfer_logic = NULLIF(@transfer_logic, ''),
    
    -- Convert Y/N values to TINYINT
    is_delinquent_loan = CASE
        WHEN LOWER(TRIM(@is_delinquent_loan)) = 'y' THEN 1
        WHEN LOWER(TRIM(@is_delinquent_loan)) = 'n' THEN 0
        ELSE NULL
    END,
    is_default_loan = CASE
        WHEN LOWER(TRIM(@is_default_loan)) = 'y' THEN 1
        WHEN LOWER(TRIM(@is_default_loan)) = 'n' THEN 0
        ELSE NULL
    END,

    -- Numeric cleanup (removing commas and converting to decimal)
    age_t = NULLIF(@age_t, ''),
    loan_amount = NULLIF(REPLACE(@loan_amount, ',', ''), '') * 1,
    funded_amount = NULLIF(REPLACE(@funded_amount, ',', ''), '') * 1,
    funded_amount_inv = NULLIF(REPLACE(@funded_amount_inv, ',', ''), '') * 1,
    int_rate = NULLIF(REPLACE(@int_rate, ',', ''), '') * 1,
    total_pymnt = NULLIF(REPLACE(@total_pymnt, ',', ''), '') * 1,
    total_pymnt_inv = NULLIF(REPLACE(@total_pymnt_inv, ',', ''), '') * 1,
    total_rec_prncp = NULLIF(REPLACE(@total_rec_prncp, ',', ''), '') * 1,
    total_fees = NULLIF(REPLACE(@total_fees, ',', ''), '') * 1,
    total_rec_int = NULLIF(REPLACE(@total_rec_int, ',', ''), '') * 1,
    total_rec_late_fee = NULLIF(REPLACE(@total_rec_late_fee, ',', ''), '') * 1,
    recoveries = NULLIF(REPLACE(@recoveries, ',', ''), '') * 1,
    collection_recovery_fee = NULLIF(REPLACE(@collection_recovery_fee, ',', ''), '') * 1;



DROP TABLE banking_data;

CREATE TABLE temp_banking_data1 AS
SELECT DISTINCT * FROM banking_data1;

DROP TABLE banking_data1;
RENAME TABLE temp_banking_data1 TO banking_data1;

ALTER TABLE banking_data1
DROP COLUMN Loan_Transfer_Date,
DROP COLUMN Interest_Paid,
DROP COLUMN Insurance_Premium;


# ------------------------------------------------------------------------------------------------------------
select * from banking_data1 ;

SELECT COUNT(*) FROM banking_data1;  -- Total rows
SELECT COUNT(DISTINCT account_id) FROM banking_data1;


# 1. Total Loan Amount Funded
		SELECT 
			SUM(funded_amount) AS Total_Loan_Amount_Funded
				FROM 
			banking_data1;

# 2.0 Total Loans
		SELECT COUNT(*) AS Total_Loans
			FROM banking_data1;
            
# 3 Total Collection
			SELECT 
				SUM(Total_Rec_Prncp + Total_Rec_Int) AS Total_Collection
				FROM 
			banking_data1;

# 4. Total Interest
		SELECT 
			SUM(Total_Rec_Int) AS Total_Interest
		FROM 
			banking_data1;

# 5. Branch-Wise (Interest, Fees, Total Revenue)
SELECT 
    Branch_Name,
    SUM(Total_Rec_Int) AS Interest_Revenue,
    SUM(Total_Fees) AS Fees_Revenue,
    SUM(Total_Rec_Int + Total_Fees) AS Total_Revenue
FROM 
    banking_data1
GROUP BY 
    Branch_Name
ORDER BY 
    Total_Revenue DESC;

# 6  State-Wise Loan
SELECT 
    State_Name,
    COUNT(*) AS Total_Loans,
    SUM(Loan_Amount) AS Total_Loan_Amount
FROM 
    banking_data1
GROUP BY 
    State_Name
ORDER BY 
    Total_Loan_Amount DESC;

# 7 Religion-Wise Loan 
SELECT 
    Religion,
    COUNT(*) AS Total_Loans,
    SUM(Loan_Amount) AS Total_Loan_Amount
FROM 
    banking_data1
GROUP BY 
    Religion
ORDER BY 
    Total_Loan_Amount DESC;
    
# 8  Product Group-Wise Loan
SELECT 
    product_code AS Product_Type,
    COUNT(*) AS Total_Loans,
    SUM(loan_amount) AS Total_Loan_Amount
FROM 
    banking_data1
GROUP BY 
    product_code
ORDER BY 
    Total_Loan_Amount DESC;

# 9 . Disbursement Trend
SELECT 
    YEAR(disbursement_date) AS Year,
    MONTH(disbursement_date) AS Month,
    COUNT(*) AS Total_Loans_Disbursed,
    SUM(loan_amount) AS Total_Loan_Amount_Disbursed
FROM 
    banking_data1
WHERE 
    disbursement_date IS NOT NULL
GROUP BY 
    YEAR(disbursement_date), MONTH(disbursement_date)
ORDER BY 
    Year DESC, Month DESC;
    
# 10 Grade-Wise Loan
SELECT 
    grade,
    COUNT(*) AS Total_Loans,
    SUM(loan_amount) AS Total_Loan_Amount,
    AVG(int_rate) AS Average_Interest_Rate
FROM 
    banking_data1
WHERE 
    grade IS NOT NULL
GROUP BY 
    grade
ORDER BY 
    grade;

# 11 Count of Default Loan
SELECT 
    COUNT(*) AS Total_Default_Loans
FROM 
    banking_data1
WHERE 
    is_default_loan = 1;

# 12 Count of Delinquent Clients
SELECT 
    COUNT(DISTINCT client_id) AS Total_Delinquent_Clients
FROM 
    banking_data1
WHERE 
    is_delinquent_loan = 1;

# 13. Delinquent Loans Rate
SELECT 
    (COUNT(CASE WHEN is_delinquent_loan = 1 THEN 1 END) / COUNT(*)) * 100 AS Delinquent_Loans_Rate
FROM 
    banking_data1;

# 14 Default Loan Rate

SELECT ROUND((SUM(CASE WHEN Is_Default_Loan = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2
    ) AS Default_Loan_Rate_Percent
FROM 
    banking_data1;
    
#15 Loan Status-Wise Loan
SELECT 
    Loan_Status,
    COUNT(*) AS Total_Loans
FROM 
    banking_data1
GROUP BY 
    Loan_Status
ORDER BY 
    Total_Loans DESC;
    
#16 Age Group-Wise Loan
SELECT
    CASE 
        WHEN Age_T < 20 THEN 'Below 20'
        WHEN Age_T BETWEEN 20 AND 29 THEN '20-29'
        WHEN Age_T BETWEEN 30 AND 39 THEN '30-39'
        WHEN Age_T BETWEEN 40 AND 49 THEN '40-49'
        WHEN Age_T BETWEEN 50 AND 59 THEN '50-59'
        ELSE '60 and above'
    END AS Age_Group,
    COUNT(*) AS Total_Loans
FROM banking_data1
WHERE Age_T IS NOT NULL
GROUP BY Age_Group
ORDER BY Age_Group;

#17 No Verified Loan
SELECT 
    COUNT(*) AS No_Verified_Loans
FROM 
    banking_data1
WHERE 
    Verification_Status IS NULL 
    OR Verification_Status NOT LIKE '%Verified%';

#18 Loan Maturity
SELECT 
    CAST(REPLACE(Term, ' months', '') AS UNSIGNED) AS Maturity_Months,
    COUNT(*) AS Total_Loans
FROM 
    banking_data1
GROUP BY 
    Maturity_Months
ORDER BY 
    Maturity_Months;




