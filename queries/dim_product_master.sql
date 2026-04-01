/* DIM_PRODUCTS: Multi-Entity Refresh */

/* --- 1. GMBH (Master) --- */
SELECT
    CHAR(34) + 'GmbH' + CHAR(34) AS "Entity",
    CHAR(34) + CAST(T0.[ItemCode] AS NVARCHAR(50)) + CHAR(34) AS "ItemCode", 
    CHAR(34) + REPLACE(CAST(T0.[ItemName] AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "Description", 
    
    /* Translated Item Group Name from OITB */
    CHAR(34) + REPLACE(CAST(T1.[ItmsGrpNam] AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "ItemGroup", 
    
    CHAR(34) + CAST(T0.[validFor] AS NVARCHAR(1)) + CHAR(34) AS "IsActive",

    /* 1. Heavy Hitters */
    CHAR(34) + CAST(ISNULL(T0.[U_WRE_webshopaktiv],'N') AS NVARCHAR(10)) + CHAR(34) AS "Webshop_Active",
    CHAR(34) + CAST(ISNULL(T0.[U_WRE_WS_Active],'N') AS NVARCHAR(10)) + CHAR(34) AS "WS_Active_Flag",
    CHAR(34) + CAST(ISNULL(T0.[U_WRE_IsProv],'N') AS NVARCHAR(10)) + CHAR(34) AS "Is_Prov",
    CHAR(34) + REPLACE(CAST(T0.[U_WRE_Status] AS NVARCHAR(50)), CHAR(34), '""') + CHAR(34) AS "Status",
    CHAR(34) + CAST(T0.[U_WRE_WS_ParentItem] AS NVARCHAR(50)) + CHAR(34) AS "Parent_Item",

    /* 2. Logistics & Packaging */
    CHAR(34) + CAST(T0.[U_WRE_VE_Weight] AS NVARCHAR(50)) + CHAR(34) AS "Weight_SU_kg",
    CHAR(34) + CAST(T0.[U_WRE_PrimWeight] AS NVARCHAR(50)) + CHAR(34) AS "Weight_Primary_g",
    CHAR(34) + CAST(T0.[U_WRE_SecWeight] AS NVARCHAR(50)) + CHAR(34) AS "Weight_Secondary_g",

    /* 3. Content & Conversions */
    CHAR(34) + CAST(T0.[U_WRE_Item_Content_ml] AS NVARCHAR(50)) + CHAR(34) AS "Content_ML",
    CHAR(34) + CAST(T0.[U_WRE_Item_Content_gr] AS NVARCHAR(50)) + CHAR(34) AS "Content_GR",

    /* 4. Categorization & Labels */
    CHAR(34) + REPLACE(CAST(T0.[U_Produktlinie] AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "ProductLine",
    CHAR(34) + REPLACE(CAST(T0.[U_OL_FrgnName] AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "Name_EN",
    CHAR(34) + REPLACE(CAST(T0.[U_WRE_WS_VariantDim1] AS NVARCHAR(MAX)), CHAR(34), '""') + CHAR(34) AS "Variant_Dim1",
    CHAR(34) + CONVERT(NVARCHAR(10), T0.[CreateDate], 120) + CHAR(34) AS "CreateDate"

FROM [OITM] T0 
LEFT JOIN [OITB] T1 ON T0.[ItmsGrpCod] = T1.[ItmsGrpCod] /* Joins the Item Group Table */
WHERE T0.[ItemCode] IS NOT NULL