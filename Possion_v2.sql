SELECT DISTINCT
       InvoiceDate,
       CustomerAddressKey,
       dd.CurrentWorkDayOffset,
	   (CASE WHEN ReturnAmount > 0 THEN ReturnAmount * -1  ELSE ExtendedPrice END) AS Revenue
INTO #CustData
FROM [USVBIAnalytics].[DWAF].[vwFactSales] fs
    INNER JOIN [USVBIAnalytics].[DW].[vwDimDate] dd
        ON fs.InvoiceDateKey = dd.DateKey
WHERE CustomerAddressKey > 0
      AND dd.CurrentWorkDayOffset >= -261 AND dd.CurrentWorkDayOffset <= 0
	  AND fs.CustomerID NOT IN (135653, 256202, 256204, 595108, 899078, 089703, 133868, 379113, 788841, 133867, 513270, 256201, 899077, 595107, 379111, 089702, 425497, 788840)

--SELECT CustomerAddressKey,
--	   SUM(Revenue) AS Total_Revenue
-- FROM #CustData

SELECT -- ROW_NUMBER() OVER (PARTITION BY CustomerAddressKey ORDER BY InvoiceDate DESC) AS Row#,
       InvoiceDate,
	   CurrentWorkDayOffset,
       CustomerAddressKey,
	   SUM(Revenue) OVER (PARTITION BY CustomerAddressKey, InvoiceDate ORDER BY InvoiceDate DESC) AS Daily_Revenue
INTO #SalesData
FROM #CustData;

SELECT DISTINCT * 
INTO #DistinctData 
FROM #SalesData 


SELECT ROW_NUMBER() OVER (PARTITION BY CustomerAddressKey ORDER BY InvoiceDate DESC) AS Row#, *
INTO #IndexedData
FROM #DistinctData

SELECT 
       A.InvoiceDate AS Date1,
       B.InvoiceDate AS Date2,
       A.CustomerAddressKey,
       CASE WHEN ABS(b.CurrentWorkDayOffset - a.CurrentWorkDayOffset) = 0 THEN 1  
	   ELSE ABS(b.CurrentWorkDayOffset - a.CurrentWorkDayOffset) End AS InnerEventTime,
       A.Row# AS Row1,
       B.Row# AS Row2,
	   B.CurrentWorkDayOffset,
	   A.Daily_Revenue
INTO #AllButLast
FROM #IndexedData A
    JOIN #IndexedData B
        ON A.Row# = B.Row# + 1
           AND B.CustomerAddressKey = A.CustomerAddressKey
--WHERE A.CustomerAddressKey IN ( '3')
ORDER BY Row1 ASC

   SELECT Date1,
       Date2,
       CustomerAddressKey,
       InnerEventTime,
       Row1,
	   Daily_Revenue
FROM #AllButLast
       
UNION

    SELECT
    Date2 AS Date1,
    CAST(GETDATE()  AS DATE) AS Date2,
    CustomerAddressKey AS CustomerAddressKey,
    CASE WHEN ABS(CurrentWorkDayOffset) = 0 THEN 1  
	ELSE ABS(CurrentWorkDayOffset) End AS InnerEventTime,
    '1' AS Row1,
	'0' AS Daily_Revenue
    FROM #AllButLast 
	WHERE Row1  = 2 ORDER BY CustomerAddressKey, Row1

DROP TABLE #CustData, #SalesData, #DistinctData, #IndexedData, #AllButLast