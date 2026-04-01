/* DIM_SALESPERSON: Monthly Refresh */
SELECT CAST('GmbH' AS NVARCHAR(20)) AS [Entity], [SlpCode], [SlpName], [Active] 
FROM OSLP

UNION ALL

SELECT CAST('UK' AS NVARCHAR(20)), [SlpCode], [SlpName], [Active] 
FROM [A20180_DES_P01].[dbo].[OSLP]

UNION ALL

SELECT CAST('CH' AS NVARCHAR(20)), [SlpCode], [SlpName], [Active] 
FROM [A20180_QMSCH_P01].[dbo].[OSLP]

UNION ALL

SELECT CAST('US' AS NVARCHAR(20)), [SlpCode], [SlpName], [Active] 
FROM [A20180_QMSUSA_P01].[dbo].[OSLP];