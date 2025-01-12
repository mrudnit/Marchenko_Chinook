CREATE OR REPLACE DATABASE GATOR_ChinookDB;

USE DATABASE GATOR_ChinookDB;

USE WAREHOUSE GATOR_WH;

CREATE SCHEMA GATOR_ChinookDB.staging;

USE SCHEMA GATOR_ChinookDB.staging;

CREATE TABLE artist_staging (
    ArtistId INT PRIMARY KEY,
    Name VARCHAR(120) NOT NULL
);

CREATE TABLE genre_staging (
    GenreId INT PRIMARY KEY,
    Name VARCHAR(120)
);

CREATE TABLE mediatype_staging (
    MediaTypeId INT PRIMARY KEY,
    Name VARCHAR(120) NOT NULL
);

CREATE TABLE playlist_staging (
    PlaylistId INT PRIMARY KEY,
    Name VARCHAR(120)
);

CREATE TABLE employee_staging (
    EmployeeId INT PRIMARY KEY,
    LastName VARCHAR(20) NOT NULL,
    FirstName VARCHAR(20) NOT NULL,
    Title VARCHAR(30),
    ReportsTo INT,
    BirthDate DATETIME,
    HireDate DATETIME,
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60),
    FOREIGN KEY (ReportsTo) REFERENCES employee_staging(EmployeeId)
);

CREATE TABLE customer_staging (
    CustomerId INT PRIMARY KEY,
    FirstName VARCHAR(40) NOT NULL,
    LastName VARCHAR(20) NOT NULL,
    Company VARCHAR(80),
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60) NOT NULL,
    SupportRepId INT,
    FOREIGN KEY (SupportRepId) REFERENCES employee_staging(EmployeeId)
);

CREATE TABLE album_staging (
    AlbumId INT PRIMARY KEY,
    Ttile VARCHAR(160) NOT NULL,
    ArtistId INT NOT NULL,
    FOREIGN KEY (ArtistId) REFERENCES artist_staging(ArtistId)
);

CREATE TABLE track_staging (
    TrackId INT PRIMARY KEY,
    Name VARCHAR(200) NOT NULL,
    AlbumId INT NOT NULL,
    MediaTypeId INT NOT NULL,
    GenreId INT NOT NULL,
    Composer VARCHAR(220) NOT NULL,
    Miliseconds INT NOT NULL,
    Bytes INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (AlbumId) REFERENCES album_staging(AlbumId),
    FOREIGN KEY (MediaTypeId) REFERENCES mediatype_staging(MediaTypeId),
    FOREIGN KEY (GenreId) REFERENCES genre_staging(GenreId)
);

CREATE TABLE playlisttrack_staging (
    PlaylistId INT,
    TrackId INT,
    PRIMARY KEY (PlaylistId, TrackId)
);

CREATE TABLE invoice_staging (
    InvoiceId INT PRIMARY KEY,
    CustomerId INT NOT NULL,
    InvoiceDate DATETIME NOT NULL,
    BillingAddress VARCHAR(70),
    BillingCity VARCHAR(40),
    BillingState VARCHAR(40),
    BillingCountry VARCHAR(40),
    BillingPostalCode VARCHAR(10),
    Total DECIMAL(10, 2),
    FOREIGN KEY (CustomerId) REFERENCES customer_staging(CustomerId)
);

CREATE TABLE invoiceline_staging (
    InvoiceLineId INT PRIMARY KEY,
    InvoiceId INT NOT NULL,
    TrackId INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    Quantity INT NOT NULL,
    FOREIGN KEY (InvoiceId) REFERENCES invoice_staging(InvoiceId),
    FOREIGN KEY (TrackId) REFERENCES track_staging(TrackId)
);

// ELT - LOADING

CREATE OR REPLACE STAGE my_stage;

DROP STAGE my_stage;

COPY INTO artist_staging
FROM @my_stage/Artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genre_staging
FROM @my_stage/Genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO mediatype_staging
FROM @my_stage/MediaType.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlist_staging
FROM @my_stage/Playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employee_staging
FROM @my_stage/Employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL'));

COPY INTO customer_staging
FROM @my_stage/Customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO album_staging
FROM @my_stage/Album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO track_staging
FROM @my_stage/Track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO playlisttrack_staging
FROM @my_stage/PlaylistTrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoice_staging
FROM @my_stage/Invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoiceline_staging
FROM @my_stage/Invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

// ELT - TRANSFORM

CREATE TABLE dim_track AS
SELECT DISTINCT
    t.TrackId AS dim_trackId,
    t.Name AS Name,
    t.Composer AS Composer,
    CASE 
        WHEN t.UnitPrice < 0.99 THEN 'Low-cost'
        WHEN t.UnitPrice BETWEEN 0.99 AND 1.99 THEN 'Standard'
        ELSE 'Premium'
    END AS UnitPrice,
    a.Ttile AS AlbumTitle,
    m.Name AS MediaType,
    CASE 
        WHEN g.Name IN ('TV Shows', 'Science Fiction', 'Drama', 'Comedy', 'Sci Fi & Fantasy', 'Soundtrack') THEN 'TV'
        ELSE 'MUSIC'
    END AS Genre,
FROM track_staging t
JOIN genre_staging g ON t.GenreId = g.GenreId
JOIN album_staging a ON t.AlbumId = a.AlbumId
JOIN mediatype_staging m ON t.MediaTypeId = m.MediaTypeId;

DROP TABLE dim_track;
SELECT * FROM dim_track;

CREATE TABLE dim_customer AS
SELECT DISTINCT
    customerId AS dim_customerId,      
    Company AS Company,          
    Country AS Country,     
FROM customer_staging;

DROP TABLE dim_customer;
SELECT * FROM dim_customer;

CREATE TABLE dim_address AS
SELECT DISTINCT
    invoiceId AS dim_addressId,
    BillingCountry AS Country,
    BillingState AS State,
    BillingCity AS City,
    BillingAddress AS Street,
    BillingPostalCode AS PostalCode
FROM invoice_staging;

DROP TABLE dim_address;
SELECT * FROM dim_address;

CREATE TABLE dim_invoice AS
SELECT DISTINCT
    invoiceId AS dim_invoiceId,      
    InvoiceDate AS InvoiceDate,      
    Total AS Total,
    customerid AS CustomerId
FROM invoice_staging;

DROP TABLE dim_invoice;
SELECT * FROM dim_invoice;

CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', InvoiceDate)) AS dim_timeID, 
    TO_CHAR(InvoiceDate, 'HH24:MI:SS') AS Time,                                           
    CASE
        WHEN DATE_PART('hour', InvoiceDate) = 0 THEN 12
        WHEN DATE_PART('hour', InvoiceDate) <= 12 THEN DATE_PART('hour', InvoiceDate)
        ELSE DATE_PART('hour', InvoiceDate) - 12
    END AS Hour,                                     
    DATE_PART('minute', InvoiceDate) AS Minute,                                 
    DATE_PART('second', InvoiceDate) AS Second,                                                                                       
    CASE
        WHEN DATE_PART('hour', InvoiceDate) < 12 THEN 'AM'
        ELSE 'PM'
    END AS PartTime                                                                -- 
FROM invoice_staging
GROUP BY InvoiceDate;

DROP TABLE dim_time;
SELECT * FROM dim_time;

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(InvoiceDate AS DATE)) AS dim_dateID, 
    CAST(InvoiceDate AS DATE) AS FullDate,                             
    DATE_PART('day', InvoiceDate) AS Day,
    CASE DATE_PART('dow', InvoiceDate) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS DayWeek,
    DATE_PART('week', InvoiceDate) AS Week,
    CASE DATE_PART('month', InvoiceDate)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS Month,                                                 
    DATE_PART('year', InvoiceDate) AS Year,                                          
    DATE_PART('quarter', InvoiceDate) AS Quarter                        
FROM invoice_staging
GROUP BY CAST(InvoiceDate AS DATE),
         DATE_PART('day', InvoiceDate),
         DATE_PART('dow', InvoiceDate),
         DATE_PART('week', InvoiceDate),
         DATE_PART('month', InvoiceDate),
         DATE_PART('year', InvoiceDate),
         DATE_PART('quarter', InvoiceDate);

DROP TABLE dim_date;
SELECT * FROM dim_date;

CREATE TABLE fact_invoiceline AS
SELECT 
    il.InvoiceLineId AS fact_invoicelineId,
    il.UnitPrice AS UnitPrice,
    il.Quantity AS Quantity,
    tr.dim_trackId AS trackId,
    i.dim_invoiceId AS invoiceId,
    i.CustomerId AS customerId,
    t.dim_timeId AS timeId,
    d.dim_dateId AS dateId,
    a.dim_addressId AS addressId
FROM invoiceline_staging il
JOIN dim_invoice i ON il.InvoiceId = i.dim_invoiceId
JOIN dim_date d ON CAST(i.InvoiceDate AS DATE) = d.Fulldate
JOIN dim_time t ON TO_CHAR(i.InvoiceDate, 'HH24:MI:SS') = t.Time
JOIN dim_address a ON il.InvoiceId = a.dim_addressId
JOIN dim_track tr ON il.TrackId = tr.dim_trackId;

DROP TABLE fact_invoiceline;
SELECT * FROM fact_invoiceline;

DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS mediatype_staging;
DROP TABLE IF EXISTS playlist_staging;
DROP TABLE IF EXISTS employee_staging;
DROP TABLE IF EXISTS customer_staging;
DROP TABLE IF EXISTS album_staging;
DROP TABLE IF EXISTS track_staging;
DROP TABLE IF EXISTS playlisttrack_staging;
DROP TABLE IF EXISTS invoice_staging;
DROP TABLE IF EXISTS invoiceline_staging;