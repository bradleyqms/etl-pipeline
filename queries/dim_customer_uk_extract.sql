/* --- 3. UK --- */
SELECT 
    CHAR(34) + 'UK' + CHAR(34) AS "Entity",
    CHAR(34) + CAST(T0."CardCode" AS NVARCHAR(50)) + CHAR(34) AS "CardCode", 
    CHAR(34) + REPLACE(CAST(T0."CardName" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "CardName", 
    
    /* Cleaned Group Name Translation */
    CHAR(34) + REPLACE(CAST(T1."GroupName" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "GroupName",

    CHAR(34) + REPLACE(CAST(T0."Address" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "BillToStreet", 
    CHAR(34) + REPLACE(CAST(T0."City" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "BillToCity", 
    CHAR(34) + CAST(T0."ZipCode" AS NVARCHAR(50)) + CHAR(34) AS "BillToZip", 
    CHAR(34) + CAST(T0."Country" AS NVARCHAR(10)) + CHAR(34) AS "BillToCountry",
    CHAR(34) + REPLACE(CAST(T0."MailAddres" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "ShipToStreet", 
    CHAR(34) + REPLACE(CAST(T0."MailCity" AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "ShipToCity", 
    CHAR(34) + CAST(T0."MailZipCod" AS NVARCHAR(50)) + CHAR(34) AS "ShipToZip", 
    CHAR(34) + CAST(T0."MailCountr" AS NVARCHAR(10)) + CHAR(34) AS "ShipToCountry",
    
    /* System Metadata wrapped in quotes to match column order */
    CHAR(34) + CAST(T0."Territory" AS NVARCHAR(20)) + CHAR(34) AS "TerritoryID", 
    CHAR(34) + CAST(T0."SlpCode" AS NVARCHAR(20)) + CHAR(34) AS "SlpCode", 
    CHAR(34) + CONVERT(NVARCHAR(10), T0."CreateDate", 120) + CHAR(34) AS "CreateDate", 
    CHAR(34) + CONVERT(NVARCHAR(10), T0."UpdateDate", 120) + CHAR(34) AS "UpdateDate", 
    CHAR(34) + CAST(T0."validFor" AS NVARCHAR(1)) + CHAR(34) AS "IsActive"

FROM [A20180_DES_P01]."dbo"."OCRD" T0
LEFT JOIN [A20180_DES_P01]."dbo"."OCRG" T1 ON T0."GroupCode" = T1."GroupCode"
WHERE T0."CardType" = 'C' AND T0."CardCode" IS NOT NULL