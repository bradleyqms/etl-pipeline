SELECT 
    CHAR(34) + 'GmbH' + CHAR(34) AS "Entity", 
    T0."DocEntry", 
    T0."DocNum", 
    T0."DocDate",
    CHAR(34) + (CASE WHEN T0."ObjType" = '13' THEN 'Invoice' ELSE 'Credit Note' END) + CHAR(34) AS "DocType",
    T1."LineNum" AS "Line_ID", 
    CHAR(34) + CAST(T0."CardCode" AS NVARCHAR(50)) + CHAR(34) AS "Card Code", 
    CHAR(34) + CAST(T1."ItemCode" AS NVARCHAR(50)) + CHAR(34) AS "Item Code", 
    CHAR(34) + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "Description",
    CHAR(34) + CAST((T1."Quantity" * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Quantity",
    CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Net Revenue",
    T1."SlpCode", 
    T0."UpdateDate"
FROM "OINV" T0 
INNER JOIN "INV1" T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2023-01-01' AND '2023-12-31' AND T0."CANCELED" = 'N'

UNION ALL

SELECT 
    CHAR(34) + 'GmbH' + CHAR(34), T0."DocEntry", T0."DocNum", T0."DocDate", CHAR(34) + 'Credit Note' + CHAR(34), 
    T1."LineNum", CHAR(34) + CAST(T0."CardCode" AS NVARCHAR(50)) + CHAR(34), CHAR(34) + CAST(T1."ItemCode" AS NVARCHAR(50)) + CHAR(34), 
    CHAR(34) + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34),
    CHAR(34) + CAST((T1."Quantity" * -1) AS NVARCHAR(50)) + CHAR(34), CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * -1) AS NVARCHAR(50)) + CHAR(34), T1."SlpCode", T0."UpdateDate"
FROM "ORIN" T0 
INNER JOIN "RIN1" T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2023-01-01' AND '2023-12-31' AND T0."CANCELED" = 'N';