SELECT 
    'GmbH' AS "Entity", T0."DocEntry", T0."DocNum", T0."DocDate",
    CASE WHEN T0."ObjType" = '13' THEN 'Invoice' ELSE 'Credit Note' END AS "DocType",
    T1."LineNum" AS "Line_ID", T0."CardCode", T1."ItemCode", CHAR(34) + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "Description",
    CHAR(34) + CAST((T1."Quantity" * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Quantity",
    CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * (CASE WHEN T0."ObjType" = '19' THEN -1 ELSE 1 END)) AS NVARCHAR(50)) + CHAR(34) AS "Net Revenue",
    T1."SlpCode", T0."UpdateDate"
FROM "OINV" T0 
INNER JOIN "INV1" T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2024-01-01' AND '2024-12-31' AND T0."CANCELED" = 'N'

UNION ALL

SELECT 
    'GmbH', T0."DocEntry", T0."DocNum", T0."DocDate", 'Credit Note', 
    T1."LineNum", T0."CardCode", T1."ItemCode", CHAR(34) + REPLACE(CAST(T1."Dscription" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34),
    CHAR(34) + CAST((T1."Quantity" * -1) AS NVARCHAR(50)) + CHAR(34), CHAR(34) + CAST((T1."TotalSumSy" * (1 - (ISNULL(T0."DiscPrcnt", 0) / 100.0)) * -1) AS NVARCHAR(50)) + CHAR(34), T1."SlpCode", T0."UpdateDate"
FROM "ORIN" T0 
INNER JOIN "RIN1" T1 ON T0."DocEntry" = T1."DocEntry"
WHERE T0."DocDate" BETWEEN '2024-01-01' AND '2024-12-31' AND T0."CANCELED" = 'N';