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










