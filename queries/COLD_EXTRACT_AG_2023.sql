/* --- 1. INVOICES (OINV) --- */
SELECT 
    '"CH"' AS "Entity", 
    T0."DocEntry", 
    T0."DocNum", 
    T0."DocDate",
    '"' + (CASE WHEN T0."ObjType" = '13' THEN 'Invoice' ELSE 'Credit Note' END) + '"' AS "DocType",
    T1."LineNum" AS "Line_ID", 
    '"' + CAST(T0."CardCode" AS NVARCHAR(50)) + '"' AS "CardCode", 
    '"' + CAST(T1."ItemCode" AS NVARCHAR(50)) + '"' AS "ItemCode", 
    '"' + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), '"', '""') + '"' AS "Description",
    CHAR(34) + CAST((T1."Quantity" * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Quantity",
    CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Net Revenue",
    T1."SlpCode", 
    T0."UpdateDate"
FROM [A20180_QMSCH_P01].[dbo].[OINV] T0 
INNER JOIN [A20180_QMSCH_P01].[dbo].[INV1] T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2023-01-01' AND '2023-12-31' 
  AND T0."CANCELED" = 'N'

UNION ALL

/* --- 2. CREDIT NOTES (ORIN) --- */
SELECT 
    '"CH"', T0."DocEntry", T0."DocNum", T0."DocDate", '"Credit Note"', 
    T1."LineNum", '"' + CAST(T0."CardCode" AS NVARCHAR(50)) + '"', '"' + CAST(T1."ItemCode" AS NVARCHAR(50)) + '"', 
    '"' + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), '"', '""') + '"',
    CHAR(34) + CAST((T1."Quantity" * -1) AS NVARCHAR(50)) + CHAR(34), 
    CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * -1) AS NVARCHAR(50)) + CHAR(34), 
    T1."SlpCode", 
    T0."UpdateDate"
FROM [A20180_QMSCH_P01].[dbo].[ORIN] T0 
INNER JOIN [A20180_QMSCH_P01].[dbo].[RIN1] T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2023-01-01' AND '2023-12-31' 
  AND T0."CANCELED" = 'N';