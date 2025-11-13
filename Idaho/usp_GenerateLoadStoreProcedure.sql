/****** Object:  StoredProcedure [dbo].[usp_GenerateLoadStoredProcedure]    Script Date: 10/17/2025 8:09:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[usp_GenerateLoadStoredProcedure]			
   @TableSchema     VARCHAR(500),			
   @TargetTableName VARCHAR(500)			
AS			
BEGIN			
    SET NOCOUNT ON;			
			
    DECLARE @object_name SYSNAME = QUOTENAME(@TableSchema) + '.[usp_Load_' + @TargetTableName + ']';			
    DECLARE @UpdateTable SYSNAME = CONCAT('dbo.tempUpd_', @TableSchema, '_', @TargetTableName);			
    DECLARE @InsertTable SYSNAME = CONCAT('dbo.tempIns_', @TableSchema, '_', @TargetTableName);			
    DECLARE @SourceTable SYSNAME = CONCAT('dbo.tempSrc_', @TableSchema, '_', @TargetTableName);
	

	PRINT '@object_name is ' + @object_name;		
    -------------------------------------------------------------------------			
    -- Drop existing load SP if already there			
    -------------------------------------------------------------------------			
    IF OBJECT_ID(@object_name, 'P') IS NOT NULL			
    BEGIN			
        EXEC('DROP PROCEDURE ' + @object_name);			
    END;			
			
    -------------------------------------------------------------------------			
    -- Build metadata			
    -------------------------------------------------------------------------			
    DECLARE @TargetName NVARCHAR(500) = QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TargetTableName);			
    DECLARE @SourceName NVARCHAR(500) = QUOTENAME('Prestage') + '.' + QUOTENAME(@TargetTableName);			
    DECLARE @SQL        NVARCHAR(MAX);			
    --DECLARE @CRLF       CHAR(2) =  CHAR(13) + CHAR(10);			
	DECLARE @Sep		CHAR(3) = ',' + CHAR(13) + CHAR(10);
	print '@Sep is "' + @Sep + '"'		
    -------------------------------------------------------------------------			
    -- Build hash expression			
    -------------------------------------------------------------------------			
    IF OBJECT_ID('tempdb..#HashValue','U') IS NOT NULL DROP TABLE #HashValue;			
	PRINT '@TableSchema' + @TableSchema		
	PRINT '@TargetTableName' + @TargetTableName		

	DECLARE @TotalCols INT;
	DECLARE @colsPart1 NVARCHAR(MAX);
	DECLARE @colsPart2 NVARCHAR(MAX);
	DECLARE @colsPart3 NVARCHAR(MAX);
	DECLARE @cols NVARCHAR(MAX);
 
	SELECT @TotalCols = COUNT(*) 
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@TargetName)		
      AND name NOT IN (			
            'ETLCreatedDate','ETLCreatedBatchID',			
            'ETLModifiedDate','ETLModifiedBatchID',			
            'ETLCurrentRow','ETLEffectiveFromDate',			
            'ETLEffectiveToDate','IsDeleted','HashValue'			
      );

	PRINT(@TotalCols)
 
	-- Split roughly in half
	DECLARE @MidPoint INT = CEILING(@TotalCols / 3.0);

	SELECT @colsPart1 = STRING_AGG('ISNULL(CAST(src.' + QUOTENAME(name) + ' AS NVARCHAR(MAX)), ''NA'')',' + ''|'' + ')
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@TargetName)
	AND column_id <= @MidPoint
	AND name NOT IN (			
            'ETLCreatedDate','ETLCreatedBatchID',			
            'ETLModifiedDate','ETLModifiedBatchID',			
            'ETLCurrentRow','ETLEffectiveFromDate',			
            'ETLEffectiveToDate','IsDeleted','HashValue'			
      );

	PRINT(@colsPart1)
 
	SELECT @colsPart2 = STRING_AGG('ISNULL(CAST(src.' + QUOTENAME(name) + ' AS NVARCHAR(MAX)), ''NA'')',' + ''|'' + ')
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@TargetName)
	AND column_id > @MidPoint and column_id <= @MidPoint * 2
	AND name NOT IN (			
            'ETLCreatedDate','ETLCreatedBatchID',			
            'ETLModifiedDate','ETLModifiedBatchID',			
            'ETLCurrentRow','ETLEffectiveFromDate',			
            'ETLEffectiveToDate','IsDeleted','HashValue'			
      );;

	PRINT(@colsPart2)

	SELECT @colsPart3 = STRING_AGG('ISNULL(CAST(src.' + QUOTENAME(name) + ' AS NVARCHAR(MAX)), ''NA'')',' + ''|'' + ')
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@TargetName)
	AND column_id > @MidPoint * 2
	AND name NOT IN (			
            'ETLCreatedDate','ETLCreatedBatchID',			
            'ETLModifiedDate','ETLModifiedBatchID',			
            'ETLCurrentRow','ETLEffectiveFromDate',			
            'ETLEffectiveToDate','IsDeleted','HashValue'			
      );;

	PRINT(@colsPart3)

    --SELECT TOP 1			
    --       @TableSchema AS TableSchema,			
    --       @TargetTableName AS TableName,			
    --       'CONVERT(CHAR(40), HASHBYTES(''SHA1'',' +			
    --            STRING_AGG(			
    --                'ISNULL(CAST(src.' + QUOTENAME(C.name) + ' AS NVARCHAR(4000)),''NA'')',			
    --                ' + ''|'' + '		
    --            )			
    --        + '),2)' AS HashValue			
    --INTO #HashValue			
    --FROM sys.columns C			
    --JOIN sys.tables T ON C.object_id = T.object_id			
    --WHERE SCHEMA_NAME(T.schema_id) = @TableSchema			
    --  AND T.name = @TargetTableName			
    --  AND C.name NOT IN (			
    --        'ETLCreatedDate','ETLCreatedBatchID',			
    --        'ETLModifiedDate','ETLModifiedBatchID',			
    --        'ETLCurrentRow','ETLEffectiveFromDate',			
    --        'ETLEffectiveToDate','IsDeleted','HashValue'			
    --  );			
			
	  --PRINT '#HashValue table'		
	  --select * from #HashValue;		
			
    -------------------------------------------------------------------------			
    -- Build dynamic field list			
    -------------------------------------------------------------------------			
    IF OBJECT_ID('tempdb..#DynamicFields','U') IS NOT NULL DROP TABLE #DynamicFields;			
			
    CREATE TABLE #DynamicFields (			
        ColumnName NVARCHAR(500),			
        InsertOn   NVARCHAR(1000),			
        UpdateOn   NVARCHAR(MAX),			
        JoinOn     NVARCHAR(1000),			
        IsPK       BIT,			
        OrdinalPos INT			
    );			
			
    INSERT INTO #DynamicFields (ColumnName,InsertOn,UpdateOn,JoinOn,IsPK,OrdinalPos)			
    SELECT 			
        C.COLUMN_NAME,			
        'src.' + QUOTENAME(C.COLUMN_NAME),			
        'tgt.' + QUOTENAME(C.COLUMN_NAME) + ' = src.' + QUOTENAME(C.COLUMN_NAME),			
        'tgt.' + QUOTENAME(C.COLUMN_NAME) + ' = src.' + QUOTENAME(C.COLUMN_NAME),			
        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END,			
        C.ORDINAL_POSITION			
    FROM INFORMATION_SCHEMA.COLUMNS C			
    LEFT JOIN (			
        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME			
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc			
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku 			
            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME			
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'			
    ) pk ON C.TABLE_SCHEMA = pk.TABLE_SCHEMA			
         AND C.TABLE_NAME = pk.TABLE_NAME			
         AND C.COLUMN_NAME = pk.COLUMN_NAME			
    WHERE C.TABLE_SCHEMA = @TableSchema			
      AND C.TABLE_NAME = @TargetTableName			
      AND C.COLUMN_NAME NOT IN ('ETLCreatedDate','ETLCreatedBatchID','ETLModifiedDate','ETLModifiedBatchID')			
      AND C.COLUMN_NAME NOT LIKE '%HashValue%'			
      AND C.COLUMN_NAME NOT LIKE '%IsDeleted%';			
			
	--select * from #DynamicFields;
    -------------------------------------------------------------------------			
    -- Column lists			
    -------------------------------------------------------------------------			
    DECLARE @ColumnList NVARCHAR(MAX) = (			
        SELECT STRING_AGG(QUOTENAME(ColumnName), @Sep)			
        FROM #DynamicFields			
    );			
			
    DECLARE @MInsert NVARCHAR(MAX) = (			
        SELECT STRING_AGG(InsertOn, @Sep)			
        FROM #DynamicFields			
    );			
			
	DECLARE @AndSep NVARCHAR(10) = ' AND ' + CHAR(13) + CHAR(10);		
    DECLARE @JoinCols NVARCHAR(MAX) = (			
        SELECT STRING_AGG(JoinOn, @AndSep)			
        FROM #DynamicFields WHERE IsPK = 1			
    );			

    DECLARE @Update NVARCHAR(MAX) = (			
        SELECT STRING_AGG(UpdateOn, @Sep)			
        FROM #DynamicFields WHERE IsPK = 0			
    );	
	
			
  -- -- DECLARE @MHash NVARCHAR(MAX) = (SELECT TOP 1 HashValue FROM #HashValue);			
			
	--PRINT 'Building Stored PROC';		
	--PRINT @MHash;		
	--PRINT @MInsert;		
	--PRINT @SourceTable;		
	--PRINT @SourceName;		
			
    -------------------------------------------------------------------------			
    -- Build load procedure code			
    -------------------------------------------------------------------------			
    SET @SQL = N'			
    CREATE PROCEDURE ' + @object_name + N'			
        @ETLBatchID INT			
    AS			
    BEGIN			
        SET NOCOUNT ON;			
        SET XACT_ABORT ON;			
			
        DECLARE @RowsInserted BIGINT = 0,			
                @RowsUpdated BIGINT = 0,			
                @CurrentDateTime DATETIME = GETDATE();			
			
        BEGIN TRY			
             --Drop temp tables if exist			
            IF OBJECT_ID(''' + @UpdateTable + ''',''U'') IS NOT NULL DROP TABLE ' + @UpdateTable + ';			
            IF OBJECT_ID(''' + @InsertTable + ''',''U'') IS NOT NULL DROP TABLE ' + @InsertTable + ';			
            IF OBJECT_ID(''' + @SourceTable + ''',''U'') IS NOT NULL DROP TABLE ' + @SourceTable + ';			
			
            -- Snapshot source			
            SELECT ' + @MInsert + ',			
                   0 AS IsDeleted,			
                  CONVERT(CHAR(40), HASHBYTES(''SHA1'', ' +  @colsPart1 + ' + ''|'' + ' + @colsPart2 + ' + ''|'' + ' + @colsPart3 + '), 2) AS HashValue		
            INTO ' + @SourceTable + '			
            FROM ' + @SourceName + ' src;	
			
			PRINT(''Hello'')
			
            -- Rows to update			
            SELECT src.*			
            INTO ' + @UpdateTable + '			
            FROM ' + @SourceTable + ' src			
            INNER JOIN ' + @TargetName + ' tgt			
                ON ' + ISNULL(@JoinCols,'1=0') + '			
            WHERE tgt.HashValue <> src.HashValue;			
			
            -- Rows to insert			
            SELECT src.*			
            INTO ' + @InsertTable + '			
            FROM ' + @SourceTable + ' src			
            WHERE NOT EXISTS (			
                SELECT 1 FROM ' + @TargetName + ' tgt			
                WHERE ' + ISNULL(@JoinCols,'1=0') + '			
            );			
			
            -- Update			
            UPDATE tgt			
            SET ' + ISNULL(@Update,'/*no cols*/') + ',			
                tgt.ETLModifiedDate = @CurrentDateTime,			
                tgt.ETLModifiedBatchID = @ETLBatchID			
            FROM ' + @TargetName + ' tgt			
            INNER JOIN ' + @UpdateTable + ' src			
                ON ' + ISNULL(@JoinCols,'1=0') + ';			
			
            -- Insert			
            INSERT INTO ' + @TargetName + '			
            (' + @ColumnList + ',			
             IsDeleted,HashValue,			
             ETLCreatedDate,ETLCreatedBatchID,			
             ETLModifiedDate,ETLModifiedBatchID)			
            SELECT ' + @MInsert + ',			
                   src.IsDeleted,			
                   src.HashValue,			
                   @CurrentDateTime,			
                   @ETLBatchID,			
                   @CurrentDateTime,			
                   @ETLBatchID			
            FROM ' + @InsertTable + ' src;			
			
            SET @RowsUpdated  = (SELECT COUNT(*) FROM ' + @UpdateTable + ');			
            SET @RowsInserted = (SELECT COUNT(*) FROM ' + @InsertTable + ');			
			
            SELECT @RowsUpdated AS RowsUpdated, @RowsInserted AS RowsInserted;			
        END TRY			
        BEGIN CATCH			
            IF OBJECT_ID(''' + @UpdateTable + ''',''U'') IS NOT NULL DROP TABLE ' + @UpdateTable + ';			
            IF OBJECT_ID(''' + @InsertTable + ''',''U'') IS NOT NULL DROP TABLE ' + @InsertTable + ';			
            IF OBJECT_ID(''' + @SourceTable + ''',''U'') IS NOT NULL DROP TABLE ' + @SourceTable + ';			
            THROW;			
        END CATCH
		BEGIN			
            IF OBJECT_ID(''' + @UpdateTable + ''',''U'') IS NOT NULL DROP TABLE ' + @UpdateTable + ';			
            IF OBJECT_ID(''' + @InsertTable + ''',''U'') IS NOT NULL DROP TABLE ' + @InsertTable + ';			
            IF OBJECT_ID(''' + @SourceTable + ''',''U'') IS NOT NULL DROP TABLE ' + @SourceTable + ';						
        END
    END;';			
			
    -------------------------------------------------------------------------			
    -- Debug: print generated code (first 4000 chars)			
    -------------------------------------------------------------------------			
    --PRINT SUBSTRING(@SQL, 0, 4000);	
	DECLARE @Index INT = 1;
	DECLARE @ChunkSize INT = 4000;

	WHILE @Index <= LEN(@SQL)
	BEGIN
		PRINT SUBSTRING(@SQL, @Index, @ChunkSize);
		SET @Index += @ChunkSize;
	END
			
	print 'length of sql' 
	print len(@SQL)		
	--print (@SQL)		
    EXEC sp_executesql @SQL;			
END			
GO


