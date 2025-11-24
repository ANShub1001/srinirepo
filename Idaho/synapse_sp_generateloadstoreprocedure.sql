/****** Object:  StoredProcedure [dbo].[usp_GenerateLoadStoredProcedure]    Script Date: 11/21/2025 8:02:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[usp_GenerateLoadStoredProcedure] @TableSchema [varchar](500),@TargetTableName [varchar](500) AS

       --DECLARE  @TableSchema VARCHAR(500)            = 'AssetSuite'
       --                   ,@TargetTableName VARCHAR(500)    = 'TIDCAFAC';


DECLARE @object_name sysname = QUOTENAME(@TableSchema) + '.[usp_load_'+@TargetTableName+']'
DECLARE @UpdateTable sysname = CONCAT('dbo.tempUpd_',@TableSchema,'_'+@TargetTableName)
DECLARE @InsertTable sysname = CONCAT('dbo.tempIns_',@TableSchema,'_'+@TargetTableName)
DECLARE @SourceTable sysname = CONCAT('dbo.tempSrc_',@TableSchema,'_'+@TargetTableName)

/*
Print @UpdateTable 
Print @InsertTable 
Print @SourceTable 


*/
IF OBJECT_ID(@object_name) = 0  OR OBJECT_ID(@object_name) IS NULL

BEGIN 

       DECLARE
       @TargetName VARCHAR(1000)               = QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TargetTableName)
       ,@SourceName VARCHAR(1000)               = QUOTENAME('Prestage') + '.' + QUOTENAME(@TargetTableName)
       ,@SQL VARCHAR(max)
       ,@CRLF CHAR(2)                                         = CHAR(13) + CHAR(10)
       
       ,@DistributionType VARCHAR(1000)  = (SELECT ISNULL(a.distribution_policy_desc,'Unknown')
                                                                        FROM sys.pdw_table_distribution_properties a
                                                                        JOIN sys.tables b ON b.object_id = a.object_id
                                                                        JOIN sys.schemas c ON c.schema_id = b.schema_id
                                                                        WHERE c.name = @TableSchema
                                                                        AND b.name = @TargetTableName)


---------------- Table to hold all the custom columns and join conditions ----------------

IF OBJECT_ID('tempdb..#HashValue', 'U') IS NOT NULL
BEGIN
       DROP TABLE #HashValue
END

SELECT @TableSchema AS TableSchema
       ,@TargetTableName AS TableName
       ,'CONVERT(CHAR(40), HASHBYTES(''SHA1'',' + STUFF(STRING_AGG(CAST(ColName as varchar(8000)), ''), 1, 9, '') + '), 2)' AS HashValue
INTO #HashValue
FROM (
       SELECT TOP 8000 TEMP.[object_id]
              ,' + ''|'' + ' + 'ISNULL(' + CASE 
                     WHEN TY.[name] NOT IN (
                                  'varchar'
                                  ,'char'
                                  ,'nvarchar'
                                  ,'nchar'
                                  )
                           THEN 'CAST(src.[' + TEMP.[name] + '] AS VARCHAR(40))'
                     ELSE 'src.[' + TEMP.[name] + ']'
                     END + ', ''NA'')' AS ColName
       FROM sys.[all_columns] AS TEMP
       JOIN sys.[types] TY ON TEMP.[system_type_id] = TY.[system_type_id]
              AND TEMP.[user_type_id] = TY.[user_type_id]
       JOIN sys.[tables] T ON T.[object_id] = TEMP.[object_id]
       WHERE TEMP.[object_id] = T.[object_id]
              AND (
                     -- do not include ETL auditing columns and hashkey 
					 TEMP.[name] NOT LIKE '%HashValue%'
                     --TEMP.[name] NOT LIKE '%HashKey%'
                     AND TEMP.[name] <> 'ETLCreatedDate'
                     AND TEMP.[name] <> 'ETLCreatedBatchID'
                     AND TEMP.[name] <> 'ETLModifiedDate'
                     AND TEMP.[name] <> 'ETLModifiedBatchID'
                     AND TEMP.[name] <> 'ETLCurrentRow'
                     AND TEMP.[name] <> 'ETLEffectiveFromDate'
                     AND TEMP.[name] <> 'ETLEffectiveToDate'
					 AND TEMP.[name] <> 'IsDeleted'
                     )
              AND SCHEMA_NAME(T.schema_id) = @TableSchema
              AND T.name = @TargetTableName
       ORDER BY TEMP.column_id
       ) ColList
IF OBJECT_ID('tempdb..#DynamicFields','U') IS NOT NULL
BEGIN
       DROP TABLE #DynamicFields
END

CREATE TABLE #DynamicFields(
       IsDataType BIT
       ,TableSchema VARCHAR(100)
       ,TableName VARCHAR(500)
       ,ColumnName VARCHAR(500)
       ,ColumnNameTxt VARCHAR(500)
       ,JoinOn VARCHAR(1000)
       ,JoinOnTxt VARCHAR(1000)
       ,UpdateOn VARCHAR(1000)
       ,HashColumns VARCHAR(1000)
       ,InsertOn VARCHAR(1000)
       ,IsPK BIT DEFAULT 0
       ,OrdinalPosition INT
       )
          
INSERT INTO #DynamicFields (
       IsDataType
       ,TableSchema
       ,TableName
       ,ColumnName
       ,ColumnNameTxt
       ,JoinOn
       ,JoinOnTxt
       ,UpdateOn
       ,HashColumns
       ,InsertOn
       ,IsPK
       ,OrdinalPosition
       )
SELECT DISTINCT CASE 
              WHEN DATA_TYPE IN ('VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR', 'NTEXT')   
                     THEN 1
              ELSE 0
              END AS IsDataType
       ,C.TABLE_SCHEMA AS TableSchema
       ,C.TABLE_NAME AS TableName
       ,C.COLUMN_NAME AS ColumnName      
       ,'ISNULL(' + QUOTENAME(C.COLUMN_NAME) + ','''') AS ' + QUOTENAME(C.COLUMN_NAME) AS ColumnNameTxt
       ,'tgt.' + QUOTENAME(C.COLUMN_NAME) + ' = src.' + QUOTENAME(C.COLUMN_NAME) AS JoinOn
       ,'ISNULL(tgt.' + QUOTENAME(C.COLUMN_NAME) + ','''')' + ' = ' + 'ISNULL(src.' + QUOTENAME(C.COLUMN_NAME) + ','''')' AS JoinOnTxt
       ,'tgt.' + QUOTENAME(C.COLUMN_NAME) + ' = src.' + QUOTENAME(C.COLUMN_NAME) AS UpdateOn
       ,'tgt.' + QUOTENAME(C.COLUMN_NAME) + ' <' + '> src.' + QUOTENAME(C.COLUMN_NAME) AS HashColumns
       ,'src.' + QUOTENAME(C.COLUMN_NAME) AS InsertOn
       ,CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IsPrimaryKey
       ,ORDINAL_POSITION AS OrdinalPosition
FROM INFORMATION_SCHEMA.COLUMNS C
LEFT JOIN (
            SELECT DISTINCT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
         )   pk 
               ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
            AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
            AND c.TABLE_NAME = pk.TABLE_NAME
            AND c.COLUMN_NAME = pk.COLUMN_NAME
WHERE
       c.TABLE_SCHEMA = @TableSchema
       AND c.TABLE_NAME = @TargetTableName
       AND c.COLUMN_NAME NOT  IN ( 'ETLCreatedDate','ETLCreatedBatchID','ETLModifiedDate','ETLModifiedBatchID')
       AND c.COLUMN_NAME NOT LIKE '%HashValue%'	
	  -- AND c.COLUMN_NAME NOT LIKE '%HashKey%'			 
       AND c.COLUMN_NAME NOT LIKE '%IsDeleted%'			 
---------------- Create the Column list variable ----------------------------------------------------
DECLARE @ColumnList VARCHAR(MAX)

SET @ColumnList = (
              SELECT STRING_AGG(CAST(QUOTENAME(ColumnName) AS NVARCHAR(MAX)), @CRLF + '  ,' ) WITHIN GROUP (ORDER BY OrdinalPosition ASC)
              FROM #DynamicFields 
              );
--select @ColumnList

---------------- Formulate columns, If PK is text then add ISNULL--------------------------------------
DECLARE @Result VARCHAR(MAX);

SET @Result = (
              SELECT STRING_AGG(
                                  CAST( 
                                         CASE 
                                                WHEN IsPK = 0 THEN QUOTENAME(ColumnName)
                                                WHEN IsPK = 1 AND IsDataType = 1 THEN ColumnNameTxt
                                         ELSE QUOTENAME(ColumnName)
                                         END
                                  AS NVARCHAR(MAX)), @CRLF + '  ,') WITHIN GROUP (ORDER BY OrdinalPosition ASC)
              FROM #DynamicFields 
              );

---------------- Create the JoinOnSource variable - matching the unique key -------------------------------
DECLARE @JoinOn VARCHAR(MAX)

SET @JoinOn = (
              SELECT STRING_AGG(CAST(JoinOn AS NVARCHAR(MAX)), @CRLF + '  AND ') WITHIN GROUP (ORDER BY OrdinalPosition ASC)
              FROM #DynamicFields                             
              WHERE IsPK = 1
              );

-------------- Join condition ---------------------------------------------------------------------------------
DECLARE @JoinCols VARCHAR(MAX);


SET @JoinCols = (
       SELECT STRING_AGG(
                                  CAST( 
                                          CASE WHEN IsPK = 1 AND IsDataType = 1 THEN JoinOnTxt
                                                  WHEN IsPK = 1 AND IsDataType = 0 THEN JoinOn
                                                END
                                         AS NVARCHAR(MAX)), @CRLF + '  AND ') WITHIN GROUP (ORDER BY OrdinalPosition ASC)
       FROM #DynamicFields                                    
       );

---------------- Update conditions ----------------------------------------------------------------------------
DECLARE @Update VARCHAR(MAX)

SET @Update = (
              SELECT STRING_AGG(CAST(UpdateOn AS NVARCHAR(MAX)), @CRLF + '  ,') WITHIN GROUP (ORDER BY OrdinalPosition ASC)
       FROM #DynamicFields  
       WHERE IsPK = 0
       );

---------------- Hash columns ------------------------------------------------------------------------------
DECLARE @MHash VARCHAR(MAX)

SET @MHash = (
              SELECT DISTINCT hk.[HashValue]
              FROM #DynamicFields a
              INNER JOIN #HashValue hk ON a.TableName = hk.TableName
              AND a.TableSchema = hk.TableSchema
              WHERE IsPK = 0
       );

---------------- Insert Conditions ---------------------------------------------------------------------------
DECLARE @MInsert VARCHAR(MAX)

SET @MInsert = (
              SELECT STRING_AGG(CAST(InsertOn AS NVARCHAR(MAX)), @CRLF + '  ,') WITHIN GROUP (ORDER BY OrdinalPosition ASC)
       FROM #DynamicFields
       );

--/*------------------------------------------------------------------------------------------------------------
--BEGIN

SET @SQL = 'CREATE PROCEDURE ' + @object_name  + @CRLF +
'@ETLBatchID INT'  + @CRLF +

'AS

BEGIN
       SET NOCOUNT ON;
       SET XACT_ABORT ON;

	DECLARE
		 @RowsInserted BIGINT = 0
		,@RowsUpdated BIGINT = 0
		,@CurrentDateTime DATETIME = GETDATE()

BEGIN TRY


       IF OBJECT_ID(''' +  @UpdateTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @UpdateTable +'
       END

	   IF OBJECT_ID(''' +  @InsertTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @InsertTable +'
       END
	   
	   IF OBJECT_ID(''' +  @SourceTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @SourceTable +'
       END 
	   



CREATE TABLE ' + @SourceTable +'
	WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
	AS
	SELECT ' + @CRLF +
	+ @MInsert  + @CRLF +
	',0 AS [IsDeleted]' + @CRLF +
	',' +  @MHash + ' AS HashValue'
		+ @CRLF 
+ 'FROM ' + @SourceName  + ' src 


CREATE TABLE '  + @UpdateTable + '
	WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
	AS
	SELECT
		src.*'
		+ @CRLF + 
	'FROM ' + @SourceTable + ' AS  src
	 INNER JOIN ' + @TargetName + ' tgt
	 ON ' + @JoinCols + @CRLF +
	'AND  tgt.HashValue <> src.HashValue


CREATE TABLE ' + @InsertTable + ' 
	WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
	AS
	SELECT 
	src.*  '
		+ @CRLF
+  'FROM '+  @SourceTable + ' AS  src 
	WHERE NOT EXISTS (
							SELECT 1 FROM'  + @TargetName + ' tgt
							WHERE  ' + @JoinCols + @CRLF +
'						 ) 

UPDATE tgt
SET 
   ' + @Update + CHAR(13)+
	 ',tgt.[ETLModifiedDate] = @CurrentDateTime'+ CHAR(13)+
	 ',tgt.[ETLModifiedBatchID]    = @ETLBatchID'+ @CRLF +
'FROM ' + @TargetName  + ' tgt
INNER JOIN ' + @UpdateTable +' src
ON (
   ' + @JoinCols + @CRLF +
'   ) 


INSERT ' + @TargetName + @CRLF +
'(
   ' + @ColumnList+
      CHAR(13)+',[IsDeleted]'
     + CHAR(13)+',[HashValue]' 
	 + CHAR(13)+',[ETLCreatedDate]'
     + CHAR(13)+',[ETLCreatedBatchID]'
     + CHAR(13)+',[ETLModifiedDate]'
     + CHAR(13)+',[ETLModifiedBatchID]'
      + CHAR(13)+ @CRLF +
')
SELECT 
   ' + @MInsert + CHAR(13)
        +',src.[IsDeleted]'+ CHAR(13)
         +',src.[HashValue]'+ CHAR(13)
         +',@CurrentDateTime'+ CHAR(13)
         +',@ETLBatchID'+ CHAR(13)
         +',@CurrentDateTime'+ CHAR(13)
         +',@ETLBatchID'+ @CRLF +
'FROM ' + @InsertTable+ ' src 


SET @RowsUpdated = (SELECT COUNT_BIG(1) FROM '+ @UpdateTable +')
SET @RowsInserted = (SELECT COUNT_BIG(1) FROM '+ @InsertTable +')

SELECT @RowsUpdated AS RowsUpdated,@RowsInserted AS RowsInserted'

+ CASE WHEN @DistributionType = 'REPLICATE' THEN @CRLF + '/* Rebuild the replicate table after Insert and Update */' + @CRLF + 'SELECT TOP 1 * INTO #Replicate FROM ' + @TargetName + @CRLF ELSE '' END +
'

END TRY

       BEGIN CATCH
	   

       IF OBJECT_ID(''' +  @UpdateTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @UpdateTable +'
       END

	   IF OBJECT_ID(''' +  @InsertTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @InsertTable +'
       END
	   
	   IF OBJECT_ID(''' +  @SourceTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @SourceTable +'
       END ;
             
              THROW;

       END CATCH

BEGIN

       IF OBJECT_ID(''' +  @UpdateTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @UpdateTable +'
       END

	   IF OBJECT_ID(''' +  @InsertTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @InsertTable +'
       END
	   
	   IF OBJECT_ID(''' +  @SourceTable+''',''U'') IS NOT NULL
       BEGIN
       DROP TABLE ' + @SourceTable +'
       END 
END	   

END'


 + @CRLF 


------------- Final SQL ----------------------------------
BEGIN
       DECLARE @SQL1 VARCHAR(MAX) = ''

       SELECT @SQL1 = @SQL1 + @SQL

       EXEC (@SQL1)
END

END
GO

