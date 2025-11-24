/****** Object:  StoredProcedure [dbo].[usp_GenerateStageObject]    Script Date: 11/21/2025 8:01:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[usp_GenerateStageObject] @SourceSystem [VARCHAR](100) AS

BEGIN

--DECLARE @SourceName VARCHAR(1000) = 'CCS'

--select * from dbo.ObjectMerge
DECLARE @SourceSchema VARCHAR(100) = (
		SELECT DISTINCT CustomSchemaName
		FROM dbo.ObjectMerge
		
		)
--Print @SourceSchema
DECLARE @TableCount INT = (
		SELECT count(*)
		FROM dbo.ObjectMerge
		)

DECLARE @SchemaQuery NVARCHAR(100) = CONCAT('EXEC (','''','CREATE SCHEMA ' , QUOTENAME(@SourceSchema),'''',')')

--PRINT @SchemaQuery

IF NOT EXISTS (SELECT NAME FROM SYS.SCHEMAS WHERE NAME = @SourceSchema)
BEGIN
 EXEC (@SchemaQuery)
END

--Print @TableCount
IF OBJECT_ID('tempdb..#TableList', 'U') IS NOT NULL
BEGIN
	DROP TABLE #TableList
END

IF OBJECT_ID('tempdb..#ColumnMeta', 'U') IS NOT NULL
BEGIN
	DROP TABLE #ColumnMeta
END

CREATE TABLE #ColumnMeta (
	ColumnMetaId INT IDENTITY(1,1) NOT NULL,
	EtlControlId INT NOT NULL, 
	ColumnMeta VARCHAR(500) NULL
)

CREATE TABLE #TableList (
	ETLControlID VARCHAR(30) NULL,
	ColumnNames VARCHAR(8000) NULL
)

INSERT INTO #TableList
SELECT DISTINCT ETLControlID
	--Adding square brackets around the table and schema (Date: 9/10/2025)
	,'CREATE TABLE ' + QUOTENAME(CustomSchemaName) + '.' + QUOTENAME(REPLACE(TableName,'/','_'))
	+ '(' + STRING_AGG(CAST(ColumMedata AS VARCHAR(MAX)), ',') + + CHAR(10) + ',[ETLCreatedDate] [datetime2](3) NOT NULL,[ETLCreatedBatchID] [bigint] NOT NULL,[ETLModifiedDate] [datetime2](3) NOT NULL,[ETLModifiedBatchID] [bigint] NOT NULL,[HashValue] [char](40) NOT NULL' + CASE 
		WHEN 1 <> 3
			THEN ',[IsDeleted] Bit NOT NULL'
		ELSE ''
		END AS ColumnNames
FROM dbo.ObjectMerge
GROUP BY CustomSchemaName,TableName
	,ETLControlID 

--SELECT * FROM #TableList

------------------------Delete tables that already exist to prevent trying to create them twice with in same schema.(Date: 7/1/2025)
DELETE FROM T
FROM
    information_schema.tables i
JOIN dbo.ObjectMerge o
	ON o.TableName = i.TABLE_NAME
	AND o.CustomSchemaName = i.TABLE_SCHEMA
JOIN #TableList t
	ON t.ETLControlID = o.ETLControlID
------------------------------------------------------------------------

/*check if pk exist or not*/

IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
BEGIN
	DROP TABLE #temp
END

SELECT
          ETLControlID
		, CustomSchemaName
		, TableName
		,COUNT(DISTINCT ConstraintName) ISPKFlag
		INTO #temp
		FROM 
(
SELECT DISTINCT
         ETLControlID
		,CustomSchemaName
		, TableName
		, CASE WHEN IsPrimaryKey = 'Yes' THEN 
		   '[PK_'+ CustomSchemaName +'_' + REPLACE(TableName,'/','_') + ']'
		   ELSE NULL END AS ConstraintName
FROM dbo.ObjectMerge O
WHERE NOT EXISTS (SELECT 1 FROM
    information_schema.tables i
WHERE  o.TableName = i.TABLE_NAME AND o.CustomSchemaName = i.TABLE_SCHEMA)
)A
GROUP BY

          ETLControlID
		, CustomSchemaName
		,TableName
		

--select * from #temp
IF OBJECT_ID('tempdb..#PKList', 'U') IS NOT NULL
BEGIN
	DROP TABLE #PKList
END

CREATE TABLE #PKList (
	ETLControlID VARCHAR(30) NULL
	,SchemaName VARCHAR(4000) NULL
	,PKColumns VARCHAR(4000) NULL
	);

	


IF EXISTS (SELECT * FROM #temp  WHERE [ISPKFlag]  = 1) 

BEGIN

	INSERT INTO #PKList
	SELECT DISTINCT 
	    x1.ETLControlID
		,QUOTENAME(x1.CustomSchemaName) AS SchemaName
		,',CONSTRAINT ' + CONCAT(x1.CustomSchemaName,'_',REPLACE(x1.TableName,'/','_')) + '  
			PRIMARY KEY NONCLUSTERED ( ' + STRING_AGG(CAST(x1.ColumnName AS varchar(max)), ',') + ' ' + 'ASC)' + CHAR(10) + 'NOT ENFORCED' 
			AS PKColumns
	FROM dbo.ObjectMerge x1
		 JOIN #temp x2
	ON x1.ETLControlID = x2.ETLControlID
	AND x1.TableName = x2.TableName
	WHERE x2.[ISPKFlag]  = 1 
	AND x1.IsPrimaryKey = 'Yes'
	--and x1.TableName like '%BSEG%'
		GROUP BY x1.ETLControlID
		,x1.CustomSchemaName
		,x1.TableName
		--,x2.TableName
END

IF EXISTS (SELECT * FROM #temp  WHERE [ISPKFlag]  = 0)

BEGIN

	INSERT INTO #PkList
	SELECT DISTINCT 
	     x1.ETLControlID
		,x1.CustomSchemaName AS SchemaName
		,NULL AS PKColumns
	FROM dbo.ObjectMerge x1
	INNER JOIN #temp x2
	ON x1.ETLControlID = x2.ETLControlID
	WHERE x2.[ISPKFlag]  = 0
	GROUP BY x1.ETLControlID
		,x1.CustomSchemaName
		
END


--SELECT * from #PKList

--select * from #PKLIST_NOPK

IF OBJECT_ID('tempdb..#List', 'U') IS NOT NULL
BEGIN
	DROP TABLE #List
END

CREATE TABLE #List (
	Sql_Query VARCHAR(8000)
	);


IF EXISTS (SELECT 1 FROM  #PKList  WHERE PKColumns IS NOT NULL) 

BEGIN
	INSERT INTO #List
	SELECT DISTINCT ColumnNames + PKColumns +')' AS Sql_Query
	FROM #TableList C 
	JOIN #PKList p
		ON C.ETLControlID = p.ETLControlID
	 WHERE PKColumns IS NOT NULL
 END
 
 IF EXISTS (SELECT 1 FROM  #PKList  WHERE PKColumns IS NULL) 
BEGIN
	INSERT INTO #List
	SELECT DISTINCT ColumnNames +')'  AS Sql_Query
	--SUBSTRING(ColumnNames,1,LEN(ColumnNames)-1) +')' AS Sql_Query
	 FROM #TableList C 
	 JOIN #PKList p
	 ON C.ETLControlID=p.ETLControlID
	 WHERE PKColumns IS NULL
 END
 
 --select * from #list


 --New Table Creation 
 
IF OBJECT_ID('tempdb..#tbl', 'U') IS NOT NULL
BEGIN
	DROP TABLE #tbl
END

--If a table contains a VARCHAR(MAX) column, we create the table with HEAP (no index) explicitly to avoid errors, as columnstore indexes do not support MAX data types in Synapse. (Date: 7/1/2025)
IF EXISTS (SELECT DISTINCT 1 FROM dbo.ObjectMerge WHERE DataType = 'varchar(MAX)')
BEGIN
	CREATE TABLE #tbl
	WITH
	( DISTRIBUTION = ROUND_ROBIN
	)
	AS
	SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
	Sql_Query + ' WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)' AS Sql_Query FROM #LIST
END
ELSE
BEGIN
	CREATE TABLE #tbl
	WITH
	( DISTRIBUTION = ROUND_ROBIN
	)
	AS
	SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
	Sql_Query FROM #LIST
END

DECLARE @nbr_statements INT = (SELECT COUNT(*) FROM #tbl)
,       @i INT = 1

WHILE   @i <= @nbr_statements
BEGIN
    DECLARE @sql_code varchar(8000) = (SELECT Sql_Query FROM #tbl WHERE Seq = @i);
    EXEC    (@sql_code);
    SET     @i +=1;
END

--                      _             _                     _                                _                    
--                     | |           | |                   | |                              | |                   
--   ___ _ __ ___  __ _| |_ ___   ___| |_ ___  _ __ ___  __| |  _ __  _ __ ___   ___ ___  __| |_   _ _ __ ___ ___ 
--  / __| '__/ _ \/ _` | __/ _ \ / __| __/ _ \| '__/ _ \/ _` | | '_ \| '__/ _ \ / __/ _ \/ _` | | | | '__/ _ / __|
-- | (__| | |  __| (_| | ||  __/ \__ | || (_) | | |  __| (_| | | |_) | | | (_) | (_|  __| (_| | |_| | | |  __\__ \
--  \___|_|  \___|\__,_|\__\___| |___/\__\___/|_|  \___|\__,_| | .__/|_|  \___/ \___\___|\__,_|\__,_|_|  \___|___/
--                                                             | |                                                
--                                                             |_|                                                                                               

/* Generate the Load Stored Procedure for every new table that has primary key and not all columns are primary keys (Date: 7/1/2025)*/
DECLARE @PKCount INT = (SELECT COUNT(*) FROM dbo.ObjectMerge WHERE IsPrimaryKey = 'Yes')

IF (EXISTS (SELECT 1 FROM  #PKList  WHERE PKColumns IS NOT NULL) AND @TableCount <> @PKCount)
BEGIN
IF OBJECT_ID('tempdb..#SP', 'U') IS NOT NULL
BEGIN
	DROP TABLE #SP
END

CREATE TABLE #SP
WITH
( DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT DISTINCT
ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
QUOTENAME(A.SchemaName) AS SchemaName,
A.TargetTableName
FROM
(
SELECT DISTINCT
CustomSchemaName AS SchemaName,
REPLACE(TableName,'/','_') AS TargetTableName
FROM dbo.ObjectMerge
) A

--select * from #SP
--Creating Stored procedures

DECLARE @nbr_Count INT = (SELECT COUNT(*) FROM #SP)
,       @inc INT = 1


-- PRINT 'Before While = ' + CAST(@nbr_Count AS VARCHAR(10))

WHILE   @inc <= @nbr_Count
BEGIN
--PRINT 'Inside While = ' + CAST(@inc AS VARCHAR(10))

DECLARE @TargetTableName VARCHAR(100) = (SELECT DISTINCT REPLACE(REPLACE(TargetTableName, '[', ''), ']','') AS TargetTableName FROM #SP WHERE Seq = @inc)
       ,@TableSchema VARCHAR(100) = (SELECT DISTINCT REPLACE(REPLACE(SchemaName, '[', ''), ']','') AS SchemaName From #SP)

--Print @TargetTableName
--Print @TableSchema


    EXEC [dbo].[usp_GenerateLoadStoredProcedure] @TableSchema = @TableSchema , @TargetTableName = @TargetTableName
	
	SET     @inc +=1;
	--PRINT @inc
END
END

--Generation stored Proc for every new table that has no primary key or all columns are primary keys (Date: 7/1/2025)
IF (EXISTS (SELECT 1 FROM  #PKList  WHERE PKColumns IS  NULL) OR @TableCount = @PKCount)
BEGIN
IF OBJECT_ID('tempdb..#SP_NOPK', 'U') IS NOT NULL
BEGIN
	DROP TABLE #SP_NOPK
END

CREATE TABLE #SP_NOPK
WITH
( DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT DISTINCT
ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
QUOTENAME(A.SchemaName) AS SchemaName,
A.TargetTableName
FROM
(SELECT DISTINCT
CustomSchemaName AS SchemaName,
REPLACE(TableName,'/','_') AS TargetTableName
FROM dbo.ObjectMerge) A

--select * from #SP
--Creting Store procedures

DECLARE @count INT = (SELECT COUNT(*) FROM #SP_NOPK)
,       @cnt INT = 1


-- PRINT 'Before While = ' + CAST(@nbr_Count AS VARCHAR(10))

WHILE   @cnt <= @count
BEGIN
--PRINT 'Inside While = ' + CAST(@inc AS VARCHAR(10))

DECLARE @TargetTableName_NoPK VARCHAR(100) = (SELECT DISTINCT REPLACE(REPLACE(TargetTableName, '[', ''), ']','') AS TargetTableName FROM #SP_NOPK WHERE Seq = @cnt)
       ,@TableSchema_NOPK VARCHAR(100) = (SELECT DISTINCT REPLACE(REPLACE(SchemaName, '[', ''), ']','') AS SchemaName FROM #SP_NOPK)

--Print @TargetTableName
--Print @TableSchema


    EXEC [dbo].[usp_GenerateLoadStoredProcedure_noPK] @TableSchema = @TableSchema_NOPK , @TargetTableName = @TargetTableName_NOPK
	
	SET     @cnt +=1;
	--PRINT @inc
END
END
  IF @TableCount = 0
  BEGIN
    PRINT 'NO NEW TABLE AVAILABLE! '
  END


END
GO

