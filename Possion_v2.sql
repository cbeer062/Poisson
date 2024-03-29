SELECT DISTINCT
       [InvoiceDate]
     , [CustomerAddressKey]
     , [dd].[CurrentWorkDayOffset]
     , SUM(   CASE
                  WHEN [ReturnAmount] > 0 THEN
                      [ReturnAmount] * -1
                  ELSE
                      [ExtendedPrice]
              END
          ) OVER (PARTITION BY [CustomerAddressKey]
                             , [InvoiceDate]
                  ORDER BY [InvoiceDate] DESC
                 ) AS [Daily_Revenue]
INTO [#CustData]
FROM [USVBIAnalytics].[DWAF].[vwFactSales]       AS [fs]
    INNER JOIN [USVBIAnalytics].[DW].[vwDimDate] AS [dd]
        ON [fs].[InvoiceDateKey] = [dd].[DateKey]
WHERE [CustomerAddressKey] > 0
      AND [dd].[CurrentWorkDayOffset] >= -261
      AND [dd].[CurrentWorkDayOffset] <= 0
      AND [fs].[CustomerID] NOT IN ( 135653, 256202, 256204, 595108, 899078, 089703, 133868, 379113, 788841, 133867
                                   , 513270, 256201, 899077, 595107, 379111, 089702, 425497, 788840
                                   );

--******************************************************************************************************************--

SELECT ROW_NUMBER() OVER (PARTITION BY [CustomerAddressKey] ORDER BY [InvoiceDate] DESC) AS [rowNumber]
     , [CurrentWorkDayOffset]
     , [InvoiceDate]
     , [CustomerAddressKey]
     , [Daily_Revenue]
INTO [#IndexedData]
FROM [#CustData];

--******************************************************************************************************************--

SELECT [A].[InvoiceDate]          AS [Date1]
     , [B].[InvoiceDate]          AS [Date2]
     , [A].[CustomerAddressKey]
     , CAST(CASE
                WHEN ABS([B].[CurrentWorkDayOffset] - [A].[CurrentWorkDayOffset]) = 0 THEN
                    1
                ELSE
                    ABS([B].[CurrentWorkDayOffset] - [A].[CurrentWorkDayOffset])
            END AS DECIMAL(6, 3)) AS [InnerEventTime]
     , [A].[rowNumber]            AS [Row1]
     , [B].[rowNumber]            AS [Row2]
     , [A].[Daily_Revenue]
     , [B].[CurrentWorkDayOffset]
INTO [#AllButToday]
FROM [#IndexedData]     AS [A]
    JOIN [#IndexedData] AS [B]
        ON [A].[rowNumber] = [B].[rowNumber] + 1
           AND [B].[CustomerAddressKey] = [A].[CustomerAddressKey]
ORDER BY 3
       , 5;

--******************************************************************************************************************--

SELECT [CustomerAddressKey]
     , AVG([InnerEventTime]) AS [lamb_da]
INTO [#lamb_da]
FROM [#AllButToday]
GROUP BY [CustomerAddressKey]
;

--******************************************************************************************************************--

SELECT [A].[Date1]
     , [A].[Date2]
     , [A].[CustomerAddressKey]
     , [A].[InnerEventTime]
     , [A].[Daily_Revenue]
     , [B].[lamb_da]
FROM [#AllButToday]      AS [A]
    LEFT JOIN [#lamb_da] AS [B]
        ON [B].[CustomerAddressKey] = [A].[CustomerAddressKey]
UNION
SELECT A.[Date2]                  AS [Date1]
     , CAST(GETDATE() AS DATE)  AS [Date2]
     , [A].[CustomerAddressKey] AS [CustomerAddressKey]
     , CASE
           WHEN ABS([A].[CurrentWorkDayOffset]) = 0 THEN
               1
           ELSE
               ABS([A].[CurrentWorkDayOffset])
       END                      AS [InnerEventTime]
     , '0'                      AS [Daily_Revenue]
     , [B].[lamb_da]
FROM [#AllButToday]      AS [A]
    LEFT JOIN [#lamb_da] AS [B]
        ON [B].[CustomerAddressKey] = [A].[CustomerAddressKey]
WHERE [Row1] = 2
ORDER BY [CustomerAddressKey]
       , Date2 desc;

--******************************************************************************************************************--
DROP TABLE [#CustData]
         , [#IndexedData]
         , [#AllButToday]
         , [#lamb_da];
