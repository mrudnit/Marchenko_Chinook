-- Average sum of buying track in company
SELECT c.Company AS Customer, AVG (i.total) AS Average
FROM fact_invoiceline il
JOIN dim_customer c ON il.customerId = c.dim_customerId
JOIN dim_invoice i ON il.invoiceId = i.dim_invoiceId
GROUP BY c.Company 
ORDER BY Average DESC
LIMIT 10;

-- Favored media types
SELECT t.MediaType AS MediaType, COUNT(t.dim_trackId) AS TotalTrack
FROM fact_invoiceline il
JOIN dim_track t ON il.trackId = t.dim_trackId
GROUP BY MediaType
ORDER BY TotalTrack DESC;

-- Sales seasonality
SELECT d.Year AS Year, d.Month AS Month, SUM(il.UnitPrice * il.Quantity) AS TotalSales
FROM fact_invoiceline il
JOIN dim_date d ON il.dateId = d.dim_dateId
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month;

-- Geographically the best selling point
SELECT a.Country AS Country, SUM(il.Quantity) AS TrackSold
FROM fact_invoiceline il
JOIN dim_address a ON il.addressId = a.dim_addressId
GROUP BY a.Country
ORDER BY TrackSold DESC
LIMIT 5;

-- Most sales Track
SELECT t.Name AS Track, SUM(il.Quantity) AS TotalSold
FROM fact_invoiceline il
JOIN dim_track t ON il.trackId = t.dim_trackId
GROUP BY t.Name 
ORDER BY TotalSold DESC
LIMIT 10;
