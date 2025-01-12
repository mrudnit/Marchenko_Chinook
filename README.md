# **Proces práce s databázou Chinook**

_Toto úložisko obsahuje informácie a výsledky z procesu ETL v spoločnosti <ins>Snowflake inc.</ins> pri analýze a práci s údajmi projektu Chinook. Na údajoch sa vykonalo veľa práce, ktorá pomôže pri vykonávaní ich rôznych analýz. Hlavným cieľom a hlavným výskumom bolo štatisticky a úplne analyzovať informácie o sledovaní interakcie (nákupu) ich zákazníkov s ohľadom na čas a miesto._

---
## **1. Práca so surovými údajmi**
_Východiskovými údajmi sú typy hudby, ich predaj a informácie o kupujúcich a pracovníkoch. Čo nám pomáha zistiť obľúbenejšie žánre relevantné pre krajinu, záujem používateľov o ne a ďalšie informácie o nákupoch (kedy a kde). 
Surové údaje boli prevzaté z otvoreného prístupu „“, ktoré sú k dispozícii na prezeranie tu._

Údaje, ktoré boli získané, obsahujú 11 tabuliek:
Hlavné z nich sú  v analýze:
```
~ Invoice
~ InvoiceLine
~ Track
~ Customer
~ Employee
```
Pomocné údaje pre jasnejšie informácie:
```
~ Album
~ Artist
~ MediaType
~ Genre
~ Playlist
~ PlaylistTrack
```
_Hlavným cieľom bolo transformovať a sprístupniť tieto údaje na rôzne analýzy_
___
### **Architektúra údajov**

### **Schéma ERD**

<p align="center">
  <img src="https://github.com/mrudnit/Marchenko_Chinook/blob/main/Chinook_ERD.png" alt="ERD Schema">
<br>
</p>

_Nespracované údaje v relačnom modeli, ktoré sa zobrazujú pomocou Schéma vzťahov medzi entitami_
<sub>To nám potom umožní vytvoriť štruktúrovanejšiu a definovanejšiu tému na analýzu.</sub>

---
## **2. Schéma STAR**
_V spodnej časti bude schéma, kde hlavnou tabuľkou faktov bude fact_invoiceline, ktorá bude prepojená :_
```
~ dim_track : Tabuľka spájajúca všetky tabuľky týkajúce sa skladieb naraz s podrobnými informáciami
~ dim_invoice : Informácie o výške nákupu používateľa
~ dim_customer : Informácie, ktoré budú užitočné pri analýze nákupov používateľa v budúcnosti.
~ dim_time : Zaznamenaný čas nákupu
~ dim_date : Presný dátum nákupu
~ dim_adress : Presná adresa nákupu
```
<p align="center">
  <img src="" alt="Star Schema">
  <br>
</p>

_Uvedená schéma predstavuje vzájomný vzťah tabuliek_

___

## **3. ETL proces**
_Tento proces pozostával z 3 krokov extrakcie, konverzie a načítania. Celý tento proces sa uskutočnil v programe Snowflake, ktorý nám pomohol vytvoriť dobré prepojenie tabuliek, schému, ktorá sa bude dať ľahko analyzovať a vizualizovať, čo je pre nás teraz hlavným cieľom._

### **3.1 Preberanie údajov na prácu**
_Počiatočné tabuľky vo formáte .csv sme načítali do programu <ins>Snowflake</ins>, aby sme s nimi mohli ďalej pracovať. Na tento účel sme vytvorili scénu my_stage. Jej účelom je dočasne uložiť súbory na import a export._
```
CREATE OR REPLACE STAGE my_stage;
```
_Formát tabuľky sme potom vytvorili ručne so všetkými nastaveniami a podmienkami._
Príklad:
```
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

```
_V každej šablóne, ktorú sme vytvorili v programe Snowflake, boli do scény načítané dočasné tabuľky, kód:_
```
COPY INTO customer_staging
FROM @my_stage/Customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
atď.
```
Ak sa vyskytli chyby pri rozpoznávaní údajov, bol zapísaný príkaz ON_ERROR = „CONTINUE“.
### 3.2 **Transformácia a vytváranie tabuliek**
_Hlavným cieľom je pripraviť tabuľky faktov a meraní, ktoré sa budú ďalej používať pri efektívnych analýzach.

Tabuľka <sup>dim_track</sup>, ktorá obsahuje údaje o skladbe: názov, skladateľ, dĺžka skladby, UnitPrice, bola tiež zlúčená a prevzatá z ďalších tabuliek (Žáner, Zoznam skladieb, Album, Umelec, Skladateľ), aby sa uľahčilo prepojenie tabuliek, ktorých atribúty sa vzťahujú na <ins>SCD 0</ins>. Pre lepšiu distribúciu bolo pridané oddelenie medzi lacnou skladbou drahou a štandardnou, ako aj uľahčenie pomocou žánru „TV“ alebo „Hudba“._
```
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
```
_Údaje o meraní sú typu <ins>SCD 0</ins>, pretože čas bol prevzatý z InvoiceDate, čo je čas nákupu, ktorý nemožno zmeniť. Pre zjednodušenie bolo pridané , ak je noc, potom <ins>„PM“</ins>, inak <ins>„AM“</ins>._
```
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
```
_Dimenzia <sup>dim_date</sup> uchováva údaje o dátume (rok, mesiac, týždeň, deň, štvrťrok) Pre ľahšie pochopenie boli pridané aj faktory ako fulldate vo formáte RRRR-MM-DD. Dimenzia patrí do klasifikácie aj <ins>SCD 0</ins>, pretože tu je pevne stanovený dátum nákupu, ktorý sa nemení._
```
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
```
_Hlavnou tabuľkou tejto schémy je <sup>fact_invoiceline</sup>, ktorá obsahuje množstvo a cenu za kus._
```
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
```
### **3.3 Odstránenie dočasných tabuliek**
_Pri vytváraní tabuliek STAR, ich vzájomného vzťahu, informácií, ktoré boli prevzaté z dočasných tabuliek. Tieto tabuľky môžeme vymazať, aby sme urýchlili ukladanie bez dodatočného zaťažovania nezmyselných súborov v tejto fáze._
```
DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS mediatype_staging;
DROP TABLE IF EXISTS playlist_staging;
DROP TABLE IF EXISTS employee_staging;
atď.
```
_Na záver môžeme povedať, že proces ETL nám umožnil spracovať údaje z tabuľky .csv prostredníctvom dočasných tabuliek, ktoré sme vytvorili počas vykonanej práce. Vytvoriť štruktúrovanejšiu pre analýzu schému modelu Star. Analyzovať nákupy používateľov, sumy výdavkov a dátumy transakcií._
___






