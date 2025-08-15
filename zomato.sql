CREATE TABLE zomato_data (
    RestaurantID INT,
    RestaurantName VARCHAR(255),
    CountryCode INT,
    Country_Name VARCHAR(100),
    City VARCHAR(100),
    Address VARCHAR(255),
    Locality VARCHAR(100),
    LocalityVerbose VARCHAR(255),
    Longitude FLOAT,
    Latitude FLOAT,
    Cuisines VARCHAR(255),
    Currency VARCHAR(50),
    Has_Table_booking BOOLEAN,
    Has_Online_delivery BOOLEAN,
    Is_delivering_now BOOLEAN,
    Switch_to_order_menu BOOLEAN,
    Price_range INT,
    Votes INT,
    Average_Cost_for_two INT,
    Rating DECIMAL(3,2),
    Datekey_Opening DATE
);



SHOW CREATE TABLE zomato_data;
ALTER TABLE zomato_data CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

SHOW VARIABLES LIKE 'secure_file_priv';



LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/zomato1.csv'
INTO TABLE zomato_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(RestaurantID, RestaurantName, CountryCode, Country_Name, City, Address, Locality, LocalityVerbose, Longitude, Latitude, Cuisines, Currency, 
@Has_Table_booking, @Has_Online_delivery, @Is_delivering_now, @Switch_to_order_menu, Price_range, Votes, Average_Cost_for_two, Rating, @Datekey_Opening)
SET 
    Has_Table_booking = CASE WHEN @Has_Table_booking = 'Yes' THEN 1 WHEN @Has_Table_booking = 'No' THEN 0 ELSE NULL END,
    Has_Online_delivery = CASE WHEN @Has_Online_delivery = 'Yes' THEN 1 WHEN @Has_Online_delivery = 'No' THEN 0 ELSE NULL END,
    Is_delivering_now = CASE WHEN @Is_delivering_now = 'Yes' THEN 1 WHEN @Is_delivering_now = 'No' THEN 0 ELSE NULL END,
    Switch_to_order_menu = CASE WHEN @Switch_to_order_menu = 'Yes' THEN 1 WHEN @Switch_to_order_menu = 'No' THEN 0 ELSE NULL END,
    Datekey_Opening = STR_TO_DATE(TRIM(@Datekey_Opening), '%d-%m-%Y');


# zomtato table
SELECT *FROM zomato_data ;

# check all row in table

SELECT count(restaurantid) FROM zomato_data ;

# calendar_table

CREATE TABLE calendar_table AS
SELECT 
    DATE(Datekey_Opening) AS DateKey,
    YEAR(Datekey_Opening) AS Year,
    MONTH(Datekey_Opening) AS MonthNo,
    DATE_FORMAT(Datekey_Opening, '%M') AS MonthFullName,
    CONCAT(YEAR(Datekey_Opening), '-', DATE_FORMAT(Datekey_Opening, '%b')) AS YearMonth,
    QUARTER(Datekey_Opening) AS Quarter,
    DAYOFWEEK(Datekey_Opening) AS WeekdayNo,
    DAYNAME(Datekey_Opening) AS WeekdayName,
    CASE 
        WHEN MONTH(Datekey_Opening) = 4 THEN 'FM1'
        WHEN MONTH(Datekey_Opening) = 5 THEN 'FM2'
        WHEN MONTH(Datekey_Opening) = 6 THEN 'FM3'
        WHEN MONTH(Datekey_Opening) = 7 THEN 'FM4'
        WHEN MONTH(Datekey_Opening) = 8 THEN 'FM5'
        WHEN MONTH(Datekey_Opening) = 9 THEN 'FM6'
        WHEN MONTH(Datekey_Opening) = 10 THEN 'FM7'
        WHEN MONTH(Datekey_Opening) = 11 THEN 'FM8'
        WHEN MONTH(Datekey_Opening) = 12 THEN 'FM9'
        WHEN MONTH(Datekey_Opening) = 1 THEN 'FM10'
        WHEN MONTH(Datekey_Opening) = 2 THEN 'FM11'
        WHEN MONTH(Datekey_Opening) = 3 THEN 'FM12'
    END AS FinancialMonth,
    CASE 
        WHEN QUARTER(Datekey_Opening) = 1 THEN 'FQ4'
        WHEN QUARTER(Datekey_Opening) = 2 THEN 'FQ1'
        WHEN QUARTER(Datekey_Opening) = 3 THEN 'FQ2'
        WHEN QUARTER(Datekey_Opening) = 4 THEN 'FQ3'
    END AS FinancialQuarter
FROM zomato_data;

select*from calendar_table;



# CurrencyExchange table

  CREATE TABLE CurrencyExchange (
    Currency VARCHAR(50),
    ExchangeRateToUSD DECIMAL(10,6),
    Code VARCHAR(10)
);

INSERT INTO CurrencyExchange (Currency, ExchangeRateToUSD, Code) VALUES
('Indian Rupees(Rs.)', 0.0114, 'INR'),
('Turkish Lira(TL)', 0.0276, 'TL'),
('Dollar($)', 1.0000, 'USD'),
('Rand(R)', 0.0544, 'R'),
('Brazilian Real(R$)', 0.1755, 'BRL'),
('Botswana Pula(P)', 0.0724, 'BWP'),
('NewZealand($)', 0.6200, 'NZD'),
('Pounds(£)', 1.2587, 'GBP'),
('Emirati Diram(AED)', 0.2700, 'AED'),
('Indonesian Rupiah(IDR)', 0.000065, 'IDR'),
('Sri Lankan Rupee(LKR)', 0.0031, 'LKR'),
('Qatari Rial(QR)', 0.2744, 'QAR');

SELECT * FROM CurrencyExchange;


# Currency Exchange in usd and inr


SELECT 
    z.RestaurantID, 
    z.RestaurantName, 
    z.Country_name, 
    z.Currency, 
    z.Average_Cost_for_two, 
    (z.Average_Cost_for_two * c.ExchangeRatetoUSD) AS Cost_in_USD,
    (z.Average_Cost_for_two * c.ExchangeRatetoUSD)*87.72 AS Cost_in_INR
FROM zomato_data AS z
JOIN currencyexchange AS c 
    ON z.Currency COLLATE utf8mb4_unicode_ci = c.Currency COLLATE utf8mb4_unicode_ci;


# Number of Restaurants by City & Country


SELECT Country_Name, City, COUNT(*) AS Num_Restaurants
FROM zomato_data
GROUP BY Country_Name, City
ORDER BY  Num_Restaurants DESC;


# Number of Restaurants Opened by Year, Quarter, Month

SELECT 
    c.Year, 
    c.Quarter, 
    c.MonthFullName, 
    COUNT(z.RestaurantID) AS Num_Restaurants
FROM calendar_table c
JOIN zomato_data z 
    ON z.Datekey_Opening = c.DateKey
GROUP BY c.Year, c.Quarter, c.MonthFullName, c.MonthNo
ORDER BY c.Year, c.Quarter, c.MonthNo;

# Count of Restaurants Based on Ratings

SELECT 
    FLOOR(Rating) AS Rating_Bucket, 
    COUNT(*) AS Num_Restaurants
FROM zomato_data
GROUP BY Rating_Bucket
ORDER BY Rating_Bucket;

# Percentage of Restaurants with has  Table Booking

SELECT 
    (SUM(CASE WHEN Has_Table_booking = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS Percentage_Table_Booking
FROM zomato_data;

# Percentage of Restaurants with has  Online Delivery

SELECT 
    (SUM(CASE WHEN Has_Online_delivery = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS Percentage_Online_Delivery
FROM zomato_data;



# Top 10 Cuisines

SELECT Cuisines, COUNT(*) AS Num_Restaurants
FROM zomato_data
GROUP BY Cuisines
ORDER BY Num_Restaurants DESC
LIMIT 10;

# Top 10 City

SELECT City, COUNT(*) AS Num_Restaurants
FROM zomato_data
GROUP BY City
ORDER BY Num_Restaurants DESC
LIMIT 10;

# Ratings Distribution

SELECT Rating, COUNT(*) AS Num_Restaurants
FROM zomato_data
GROUP BY Rating
ORDER BY Rating DESC;




# Create Buckets for usd 

WITH Converted_Cost AS (
    SELECT 
        z.RestaurantID, 
        z.RestaurantName, 
        z.Country_name, 
        z.Currency, 
        z.Average_Cost_for_two, 
        (z.Average_Cost_for_two * c.ExchangeRatetoUSD) AS Average_Cost_for_two_USD
    FROM zomato_data AS z
    JOIN currencyexchange AS c 
        ON z.Currency COLLATE utf8mb4_unicode_ci = c.Currency COLLATE utf8mb4_unicode_ci
)

SELECT 
    CASE 
        WHEN Average_Cost_for_two_USD <= 10 THEN 'Low (<=10 USD)'
        WHEN Average_Cost_for_two_USD BETWEEN 11 AND 30 THEN 'Medium (11-30 USD)'
        WHEN Average_Cost_for_two_USD BETWEEN 31 AND 50 THEN 'High (31-50 USD)'
        ELSE 'Luxury (>50 USD)'
    END AS Price_Bucket,
    COUNT(*) AS Num_Restaurants
FROM Converted_Cost
GROUP BY Price_Bucket
ORDER BY Num_Restaurants DESC;

