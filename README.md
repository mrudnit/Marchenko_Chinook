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
  <img src="https://github.com/mrudnit/Marchenko_Chinook/blob/main/Chinook_STAR.jpg" alt="Star Schema">
  <br>
</p>

_Uvedená schéma predstavuje vzájomný vzťah tabuliek_

___

## **3. ETL proces**
_Tento proces pozostával z 3 krokov extrakcie, konverzie a načítania. Celý tento proces sa uskutočnil v programe Snowflake, ktorý nám pomohol vytvoriť dobré prepojenie tabuliek, schému, ktorá sa bude dať ľahko analyzovať a vizualizovať, čo je pre nás teraz hlavným cieľom._

### **3.1 Preberanie údajov na prácu**
_Počiatočné tabuľky vo formáte .csv sme načítali do programu <ins>Snowflake</ins>, aby sme s nimi mohli ďalej pracovať. Na tento účel sme vytvorili scénu my_stage. Jej účelom je dočasne uložiť súbory na import a export._
```sql
CREATE OR REPLACE STAGE my_stage;
```
_Formát tabuľky sme potom vytvorili ručne so všetkými nastaveniami a podmienkami._
Príklad:
```sql
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
```sql
COPY INTO customer_staging
FROM @my_stage/Customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
atď.
```
Ak sa vyskytli chyby pri rozpoznávaní údajov, bol zapísaný príkaz ON_ERROR = „CONTINUE“.
### 3.2 **Transformácia a vytváranie tabuliek**

_Hlavným cieľom je pripraviť tabuľky faktov a meraní, ktoré sa budú ďalej používať pri efektívnych analýzach._

_Tabuľka <sup>dim_track</sup>, ktorá obsahuje údaje o skladbe: názov, skladateľ, dĺžka skladby, UnitPrice, bola tiež zlúčená a prevzatá z ďalších tabuliek (Žáner, Zoznam skladieb, Album, Umelec, Skladateľ), aby sa uľahčilo prepojenie tabuliek, ktorých atribúty sa vzťahujú na <ins>SCD 0</ins>. Pre lepšiu distribúciu bolo pridané oddelenie medzi lacnou skladbou drahou a štandardnou, ako aj uľahčenie pomocou žánru „TV“ alebo „Hudba“._
```sql
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
_Údaje o meraní sú typu <ins>SCD 0</ins>, pretože čas bol prevzatý z InvoiceDate, čo je čas nákupu, ktorý nemožno zmeniť. Pre zjednodušenie bolo pridané , ak je noc, potom <ins> „PM“ </ins>, inak <ins> „AM“ </ins>._
```sql
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
```sql
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
```sql
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
```sql
DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS mediatype_staging;
DROP TABLE IF EXISTS playlist_staging;
DROP TABLE IF EXISTS employee_staging;
atď.
```
_Na záver môžeme povedať, že proces ETL nám umožnil spracovať údaje z tabuľky .csv prostredníctvom dočasných tabuliek, ktoré sme vytvorili počas vykonanej práce. Vytvoriť štruktúrovanejšiu pre analýzu schému modelu Star. Analyzovať nákupy používateľov, sumy výdavkov a dátumy transakcií._
___
## **4.Vizualizácia údajov**

_V tejto časti sa uvádza 5 hlavných analýz, ktoré slúžia na všeobecnú kontrolu tohto projektu. Väčšina z nich súvisí so štatistikami predaja (kedy, kde a čo presne?)._

<p align="center">
  <img src="">
  <br>
</p>

---
### **GRAF 1 "Average sum of buying track in company"**

_Táto vizualizácia zobrazuje hodnotu spoločností vo vzťahu k hudbe ich nákladov. Tento graf vám pomôže naznačiť, ktorú spoločnosť môžete osloviť s požiadavkou na predaj alebo vytvoriť stratégiu na zvýšenie objemu a zamerať sa na týchto zákazníkov vo vzťahu k analýze. Spoločnosť "JetBrains s.r.o." je teraz lídrom v tejto oblasti._
```sql
SELECT c.Company AS Customer, AVG (i.total) AS Average
FROM fact_invoiceline il
JOIN dim_customer c ON il.customerId = c.dim_customerId
JOIN dim_invoice i ON il.invoiceId = i.dim_invoiceId
GROUP BY c.Company 
ORDER BY Average DESC
LIMIT 10;
```
---
### **GRAF 2 "Favored media types"**
_Táto vizualizácia zobrazuje záujem zákazníkov o konkrétny typ médií. Môže byť užitočná na to, aby ľudia pochopili, ktorý typ médií je momentálne obľúbený. Lídrom je momentálne zvukový súbor "MPEG" s počtom 687 000 vydaných zvukových súborov_
```sql
SELECT t.MediaType AS MediaType, COUNT(t.dim_trackId) AS TotalTrack
FROM fact_invoiceline il
JOIN dim_track t ON il.trackId = t.dim_trackId
GROUP BY MediaType
ORDER BY TotalTrack DESC;
```
---
### **GRAF 3 "Sales seasonality"**
_Táto tabuľka pomáha určiť sezónnosť tratí. Momentálne najlepší výsledok bol 22. januára, ale ostatné výsledky v hornej časti po januári sú všetky z teplejších ročných období. Pomôže vám to pochopiť, v ktorom období sa predával najlepšie a ako sa táto úroveň udrží do budúcnosti. Prečo som použil <sup>SUM(il.Quantity * il.UnitPrice)</sup>, pretože to je presne celková suma predaja vzhľadom na množstvo a cenu. Takéto <sup>SUM(il.Quantity), SUM(i.Total)</sup> nie sú vhodné, pretože je potrebné to konkretizovať._
```sql
SELECT d.Year AS Year, d.Month AS Mounth, SUM(il.UnitPrice*il.Quantity) AS TotalSales
FROM fact_invoiceline il
JOIN dim_date d ON il.dateid = d.dim_dateId
GROUP BY d.Year, d.Month
ORDER BY TotalSales DESC
LIMIT 100;
```
---
### **GRAF 4 "Geographically the best selling point"**
_Graf ukazuje, v ktorých krajinách je o skladby najväčší záujem a kde sa ich kúpilo viac. To pomôže marketérom pochopiť, na ktorú skupinu ľudí, na ktorú národnosť sa majú zamerať, aby získali viac predaja._
```sql
SELECT a.Country AS Country, SUM(il.Quantity) AS TrackSold
FROM fact_invoiceline il
JOIN dim_address a ON il.addressId = a.dim_addressId
GROUP BY a.Country
ORDER BY TrackSold DESC
LIMIT 5;
```
---
### **GRAF 5 "Most sales Track"**

_Tento graf zobrazuje 10 najpredávanejších skladieb. "The Trooper" V súčasnosti je najobľúbenejší. Vďaka tomu môžete určiť, ktoré skladby sú momentálne v kurze, čo je dôležité z marketingového hľadiska pre popularizáciu tohto typu hudby._
```sql
SELECT t.Name AS Track, SUM(il.Quantity) AS TotalSold
FROM fact_invoiceline il
JOIN dim_track t ON il.trackId = t.dim_trackId
GROUP BY t.Name 
ORDER BY TotalSold DESC
LIMIT 10;
```

---

_Vizualizácia môže pomôcť stručne vysvetliť človeku otázky, ktoré sú pre neho dôležité, a odhaliť jeho otázku pomocou kresby._

**Author:** Maksym Marchenko





