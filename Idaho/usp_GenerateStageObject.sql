/****** Object:  StoredProcedure [dbo].[usp_GeneratestageObject]    Script Date: 10/17/2025 8:08:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[usp_GeneratestageObject] @SourceSystem [VARCHAR](500)
AS
BEGIN
    SET NOCOUNT ON;

   DECLARE @SourceSchema VARCHAR(100) = (
    SELECT TOP 1 CustomSchemaName AS CustomSchemaName
    FROM sqldbdataservicesgis.dbo.ObjectMerge
    );

	--Print @SourceSchema

    DECLARE @TableCount INT = (
        SELECT COUNT(*) 
        FROM sqldbdataservicesgis.dbo.ObjectMerge
    );

	--Print @TableCount

    DECLARE @SchemaQuery NVARCHAR(200) = CONCAT('CREATE SCHEMA ', QUOTENAME(@SourceSchema));

	--Print @SchemaQuery

    IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = @SourceSchema)
    BEGIN
        EXEC (@SchemaQuery);
    END;

    IF OBJECT_ID('tempdb..#TableList', 'U') IS NOT NULL DROP TABLE #TableList;
    IF OBJECT_ID('tempdb..#ColumnMeta', 'U') IS NOT NULL DROP TABLE #ColumnMeta;

	--print 'creating temp table ColumnMeta'

    CREATE TABLE #ColumnMeta (
        ColumnMetaId INT IDENTITY(1,1) NOT NULL,
        EtlControlId INT NOT NULL,
        ColumnMeta VARCHAR(500) NULL
    );

    CREATE TABLE #TableList (
        ETLControlID VARCHAR(30) NULL,
        ColumnNames VARCHAR(MAX) NULL
    );

    -- Build column definitions using FOR XML PATH instead of STRING_AGG
    INSERT INTO #TableList
    SELECT DISTINCT o.ETLControlID,
           'CREATE TABLE ' + QUOTENAME(o.CustomSchemaName) + '.' + QUOTENAME(REPLACE(o.TableName,'/','_'))
           + '(' 
           + STUFF((
                SELECT ',' + CAST(i.ColumMedata AS VARCHAR(MAX))
                FROM sqldbdataservicesgis.dbo.ObjectMerge i
                WHERE i.ETLControlID = o.ETLControlID
                  AND i.CustomSchemaName = o.CustomSchemaName
                  AND i.TableName = o.TableName
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,1,'')
           + CHAR(10)
		   + ',[ETLCreatedDate] [datetime2](3) NOT NULL'
           + ',[ETLCreatedBatchID] [bigint] NOT NULL'
		   + ',[ETLModifiedDate] [datetime2](3) NOT NULL'
           + ',[ETLModifiedBatchID] [bigint] NOT NULL'
		   + ',[HashValue] [char](40) NOT NULL'
           + CASE WHEN 1 <> 3 THEN ',[IsDeleted] BIT NOT NULL' ELSE '' END AS ColumnNames
    FROM sqldbdataservicesgis.dbo.ObjectMerge o
    GROUP BY o.CustomSchemaName, o.TableName, o.ETLControlID;

	--print 'printing table list'
	--select * from #TableList;

    -- Cleanup existing tables if already present
    DELETE t
    FROM #TableList t
    JOIN sqldbdataservicesgis.dbo.ObjectMerge o
        ON t.ETLControlID = o.ETLControlID
    JOIN information_schema.tables i
        ON o.TableName = i.TABLE_NAME
       AND o.CustomSchemaName = i.TABLE_SCHEMA;

    IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL DROP TABLE #temp;

    SELECT ETLControlID, CustomSchemaName, TableName, COUNT(DISTINCT ConstraintName) AS ISPKFlag
    INTO #temp
    FROM (
        SELECT DISTINCT ETLControlID, CustomSchemaName, TableName,
               CASE WHEN IsPrimaryKey = 'Yes' 
                    THEN '[PK_' + CustomSchemaName + '_' + REPLACE(TableName,'/','_') + ']'
                    ELSE NULL END AS ConstraintName
        FROM sqldbdataservicesgis.dbo.ObjectMerge o
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.tables i
            WHERE o.TableName = i.TABLE_NAME AND o.CustomSchemaName = i.TABLE_SCHEMA
        )
    ) a
    GROUP BY ETLControlID, CustomSchemaName, TableName;

	--print 'printing table #temp'
	--select * from #temp;

    IF OBJECT_ID('tempdb..#PKList', 'U') IS NOT NULL DROP TABLE #PKList;

    CREATE TABLE #PKList (
        ETLControlID VARCHAR(30) NULL,
        SchemaName VARCHAR(4000) NULL,
        PKColumns VARCHAR(MAX) NULL
    );

    -- Build PK definitions with FOR XML PATH
    IF EXISTS (SELECT * FROM #temp WHERE ISPKFlag = 1)
    BEGIN
        INSERT INTO #PKList
        SELECT DISTINCT x1.ETLControlID,
               QUOTENAME(x1.CustomSchemaName) AS SchemaName,
               ',CONSTRAINT ' + CONCAT(x1.CustomSchemaName,'_',REPLACE(x1.TableName,'/','_'))
               + ' PRIMARY KEY NONCLUSTERED ( ' 
               + STUFF((
                    SELECT ',' + CAST(i.ColumnName AS VARCHAR(MAX))
                    FROM sqldbdataservicesgis.dbo.ObjectMerge i
                    WHERE i.ETLControlID = x1.ETLControlID
                      AND i.TableName = x1.TableName
                      AND i.IsPrimaryKey = 'Yes'
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,1,'')
               + ' ASC)' AS PKColumns
        FROM sqldbdataservicesgis.dbo.ObjectMerge x1
        JOIN #temp x2 ON x1.ETLControlID = x2.ETLControlID AND x1.TableName = x2.TableName
        WHERE x2.ISPKFlag = 1 AND x1.IsPrimaryKey = 'Yes'
        GROUP BY x1.ETLControlID, x1.CustomSchemaName, x1.TableName;
    END;

    IF EXISTS (SELECT * FROM #temp WHERE ISPKFlag = 0)
    BEGIN
        INSERT INTO #PKList
        SELECT DISTINCT x1.ETLControlID,
               x1.CustomSchemaName AS SchemaName,
               NULL AS PKColumns
        FROM sqldbdataservicesgis.dbo.ObjectMerge x1
        JOIN #temp x2 ON x1.ETLControlID = x2.ETLControlID
        WHERE x2.ISPKFlag = 0
        GROUP BY x1.ETLControlID, x1.CustomSchemaName;
    END;

	--print 'printing table PKList'
	--select * from #PKList;

    IF OBJECT_ID('tempdb..#List', 'U') IS NOT NULL DROP TABLE #List;
    CREATE TABLE #List (Sql_Query VARCHAR(MAX));

    INSERT INTO #List
    SELECT DISTINCT ColumnNames + PKColumns + ')' AS Sql_Query
    FROM #TableList c JOIN #PKList p ON c.ETLControlID = p.ETLControlID
    WHERE PKColumns IS NOT NULL;

    INSERT INTO #List
    SELECT DISTINCT ColumnNames + ')' AS Sql_Query
    FROM #TableList c JOIN #PKList p ON c.ETLControlID = p.ETLControlID
    WHERE PKColumns IS NULL;

	--print 'printing table #List'
	--select * from #List;

    IF OBJECT_ID('tempdb..#tbltemp', 'U') IS NOT NULL DROP TABLE #tbltemp;
    CREATE TABLE #tbltemp (Seq INT, Sql_Query VARCHAR(MAX));

    INSERT INTO #tbltemp (Seq, Sql_Query)
    SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq, Sql_Query
    FROM #List;

    DECLARE @nbr_statements INT = (SELECT COUNT(*) FROM #tbltemp), @i INT = 1;

    WHILE @i <= @nbr_statements
    BEGIN
        DECLARE @sql_code NVARCHAR(MAX) = (SELECT Sql_Query FROM #tbltemp WHERE Seq = @i);
        PRINT @sql_code; -- Debug
        EXEC sp_executesql @sql_code;
        SET @i += 1;
    END;

	--print 'table created'

    -- Generate load SPs
	--print 'now started with stored proc creation'
    DECLARE @PKCount INT = (SELECT COUNT(*) FROM sqldbdataservicesgis.dbo.ObjectMerge WHERE IsPrimaryKey = 'Yes');
	--print 'pk count';
	--print @PKCount
    IF (EXISTS (SELECT 1 FROM #PKList WHERE PKColumns IS NOT NULL) AND @TableCount <> @PKCount)
    BEGIN
		print 'PKLIST table PKColumns is not null'
        IF OBJECT_ID('tempdb..#SP', 'U') IS NOT NULL DROP TABLE #SP;
        CREATE TABLE #SP (Seq INT, SchemaName VARCHAR(200), TargetTableName VARCHAR(200));

        INSERT INTO #SP
        SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
               CustomSchemaName, REPLACE(TableName,'/','_')
        FROM sqldbdataservicesgis.dbo.ObjectMerge
        GROUP BY CustomSchemaName, TableName;

		--print 'print #SP table'
		--select * from #SP;

        DECLARE @nbr_Count INT = (SELECT COUNT(*) FROM #SP), @inc INT = 1;
		--print '@nbr_Count'
		--print @nbr_Count
        WHILE @inc <= @nbr_Count
        BEGIN
            DECLARE @TargetTableName VARCHAR(100) = (SELECT TargetTableName FROM #SP WHERE Seq = @inc);
            DECLARE @TableSchema VARCHAR(100) = (SELECT SchemaName FROM #SP WHERE Seq = @inc);
			--print '@TableSchema and @TargetTableName'
			--print @TableSchema
			--print @TargetTableName
            EXEC [dbo].[usp_GenerateLoadStoredProcedure] @TableSchema = @TableSchema , @TargetTableName = @TargetTableName;

            SET @inc += 1;
        END;
    END;

    IF (EXISTS (SELECT 1 FROM #PKList WHERE PKColumns IS NULL) OR @TableCount = @PKCount)
    BEGIN
		print 'PKLIST table PKColumns is null'
        IF OBJECT_ID('tempdb..#SP_NOPK', 'U') IS NOT NULL DROP TABLE #SP_NOPK;
        CREATE TABLE #SP_NOPK (Seq INT, SchemaName VARCHAR(200), TargetTableName VARCHAR(200));

        INSERT INTO #SP_NOPK
        SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Seq,
               CustomSchemaName, REPLACE(TableName,'/','_')
        FROM sqldbdataservicesgis.dbo.ObjectMerge
        GROUP BY CustomSchemaName, TableName;

        DECLARE @count INT = (SELECT COUNT(*) FROM #SP_NOPK), @cnt INT = 1;

        WHILE @cnt <= @count
        BEGIN
            DECLARE @TargetTableName_NoPK VARCHAR(100) = (SELECT TargetTableName FROM #SP_NOPK WHERE Seq = @cnt);
            DECLARE @TableSchema_NOPK VARCHAR(100) = (SELECT SchemaName FROM #SP_NOPK WHERE Seq = @cnt);

            EXEC [dbo].[usp_GenerateLoadStoredProcedure_noPK] @TableSchema = @TableSchema_NOPK , @TargetTableName = @TargetTableName_NoPK;

            SET @cnt += 1;
        END;
    END;

    IF @TableCount = 0
    BEGIN
        PRINT 'NO NEW TABLE AVAILABLE!';
    END;
END;
GO


