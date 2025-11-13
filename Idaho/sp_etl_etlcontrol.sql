/****** Object:  StoredProcedure [CTL].[usp_ETLControlSourceEntry]    Script Date: 11/13/2025 1:20:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE PROC [CTL].[usp_ETLControlSourceEntry]
(
	@SourceSystem VARCHAR(50) 

	 -- SQLSource
    ,@SourceServer VARCHAR(100)
	,@SourceDatabase VARCHAR(50)  	
	,@SourceSchema VARCHAR(50) 
	,@SourceTable VARCHAR(255) 
	,@SourceHost VARCHAR(255)
	,@SourceServiceName VARCHAR(255)
	,@IsDeletionRequired BIT = NULL
	,@SourceTableCompositeKey VARCHAR(1000) = NULL

	 -- FileSource
	,@FileServerHostName VARCHAR(100)
	,@SourceFileRoot VARCHAR(100) 
	,@SourceFilePath VARCHAR(255)
	,@SourceFileName VARCHAR(255)
	,@SourceFileDelimiter VARCHAR(1)
	,@SourceFileQuoteCharacter VARCHAR(1)
	,@EarliestFileAvailableDate VARCHAR(100) --Start date to load the files from filedrop
	,@SheetNames VARCHAR(1000) --For Excel Files

	 -- APISource
	,@APISourceHost VARCHAR(255) = null
	,@APISourceServiceName VARCHAR(255) = null 
	,@APISourcefileName VARCHAR(100) = null

	 -- SQLSink
	,@SinkServer VARCHAR(100)
	,@SinkDatabase VARCHAR(50)
	,@SinkSchema VARCHAR(50) 
	,@SinkTable VARCHAR(255) 	

	-- FileSink
	,@SinkFileRoot VARCHAR(100) 	
	,@SinkFilePath VARCHAR(255) 
	,@SinkFileName VARCHAR(255) 

	,@TableType VARCHAR(20) 	 --Source/Stage/Dimension/Fact (FK: CTL.ETLTableType)
	,@ColumnMetaData VARCHAR(MAX) 
	
	--DeltaDetection
	,@DeltaDetectType VARCHAR(100)	--Predicate/ChangeTracking
	,@QueryPredicate VARCHAR(MAX)
	,@QueryPredicateType VARCHAR(100)  --Date/ID

	,@SinkLoadProcedureName VARCHAR(255)
	,@SinkPreLoadScript VARCHAR(MAX)
	,@SinkWriteBatchSize INT
	,@SourceTypeID INT	--1/2 (FK: CTL.ETLSourceType)
	,@IsActive BIT = 1
	,@SourceQuery VARCHAR(MAX) = NULL
	,@TriggerType  VARCHAR(MAX)
	,@SourceTableControlID BIGINT
	
)
AS

/*
Altered 2/21/2024
By Megan Hutchinson
To Add SheetNames column and parameter for loading Excel files with multiple tabs
*/

BEGIN

DECLARE 		
	@LoadType VARCHAR(20) = CASE WHEN NULLIF(@DeltaDetectType,'') IS NULL THEN 'Full' ELSE 'Incremental' END
	,@SourceType VARCHAR(100) 
	,@SinkType VARCHAR(100) 
	,@MaxExpression VARCHAR(MAX) = CASE 
									WHEN @DeltaDetectType = 'Predicate' THEN CONCAT('MAX(',@QueryPredicate,')')	
									WHEN @DeltaDetectType = 'ChangeTracking' THEN 'CHANGE_TRACKING_CURRENT_VERSION()'
									WHEN NULLIF(@DeltaDetectType,'') IS NULL THEN '0'
								  END
	--For File source ONLY (SourceTypeID = 3): Assign, if provided EarliestFileAvailableDate else current date as the MinExpression, a date from which the files should start getting copied over from file drop to ADLS. This value will be used in CTL.usp_GetFolderPath.
	,@MinExpression VARCHAR(100) = CASE 
									WHEN @TableType = 'Source' AND @SourceTypeID = 3 THEN	
										CASE WHEN @EarliestFileAvailableDate <> '' OR @EarliestFileAvailableDate IS NOT NULL THEN FORMAT(CAST(@EarliestFileAvailableDate AS DATE), 'yyyy-MM-dd 00:00:00.000')
											 WHEN @EarliestFileAvailableDate = ''  OR @EarliestFileAvailableDate IS NULL	 THEN FORMAT(CAST(GETDATE() AS DATE), 'yyyy-MM-dd 00:00:00.000') END
									ELSE NULL
									END
	,@DIU TINYINT = 2
	,@Note VARCHAR(MAX) = CASE WHEN @TableType = 'Source' THEN 'Loading data from source to azure data lake storage'
	                           WHEN @TableType = 'Stage' THEN 'Loading data from azure data lake storage to staging tables'
				               WHEN @TableType = 'Dimension' THEN 'Transformaing and Loading data from staging tables to Dimesnion tables'
				               WHEN @TableType = 'Fact' THEN 'Transforming and Loading data from staging to fact tables' END
	--,@TriggerType VARCHAR(MAX) = 'DailyTrigger'

DECLARE @CusSourceTable VARCHAR(MAX)
SET @CusSourceTable  = CASE WHEN CHARINDEX('/',COALESCE(@SourceTable,@SourceFileName)) > 0 THEN SUBSTRING(REPLACE(COALESCE(@SourceTable,@SourceFileName),'/','_'),2,LEN((COALESCE(@SourceTable,@SourceFileName)))) ELSE COALESCE(@SourceTable,@SourceFileName) END

IF @TableType = 'Source' --AND @SourceTypeID IN (1,4,5)
BEGIN
	SET @SinkFilePath = CASE WHEN @SinkFilePath IS NOT NULL THEN @SinkFilePath ELSE CONCAT(@CusSourceTable,'\YYYY\MM\DD\') END
	-- CASE WHEN @SinkFilePath IS NOT NULL THEN @SinkFilePath ELSE CONCAT(@SourceTable,'\YYYY\MM\DD\') END
	--CASE WHEN @SinkFilePath IS NOT NULL THEN @SinkFilePath ELSE CONCAT(@SourceDatabase,'\',@SourceSchema,'\',@SourceTable,'\YYYY\MM\DD\') END	
	SET @SinkFileName = CASE WHEN @SinkFileName IS NOT NULL THEN @SinkFileName ELSE CONCAT(@CusSourceTable,'_*.parquet') END
	--CASE WHEN @SinkFileName IS NOT NULL THEN @SinkFileName ELSE CONCAT(@SourceSchema,'_',@SourceTable,'_*.parquet') END
END


IF (@TableType = 'DW')
BEGIN
	SET @SourceSystem = 'Stage' 
END

MERGE [CTL].[ETLControl] AS Tgt
USING (
		SELECT 
			 @SourceSystem	AS SourceSystem 
			,(	
				SELECT 
					--SQLSource
					@SourceServer AS 'SQLSource.ServerName',@SourceDatabase AS 'SQLSource.DatabaseName', @SourceSchema AS 'SQLSource.SchemaName', @SourceTable AS 'SQLSource.TableName'
					,@SourceHost AS 'SQLSource.Host', @SourceServiceName AS 'SQLSource.ServiceName'
					--FileSource
					,@FileServerHostName AS 'FileSource.FileServerHostName',@SourceFileRoot AS 'FileSource.FileRoot',	@SourceFilePath  AS 'FileSource.FilePath',	@SourceFileName AS 'FileSource.FileName', @SourceFileDelimiter AS 'FileSource.FileDelimiter', (CASE WHEN @TableType = 'Source' AND @SourceTypeID = 3 AND @SourceFileQuoteCharacter IS NULL THEN '' ELSE @SourceFileQuoteCharacter END) AS 'FileSource.FileQuoteCharacter'				
					--,@SourceFileRoot AS 'FileSource.FileRoot',	@SourceFilePath  AS 'FileSource.FilePath',	@SourceFileName AS 'FileSource.FileName', @SourceFileDelimiter AS 'FileSource.FileDelimiter', (CASE WHEN @TableType = 'Source' AND @SourceTypeID = 3 AND @SourceFileQuoteCharacter IS NULL THEN '' ELSE @SourceFileQuoteCharacter END) AS 'FileSource.FileQuoteCharacter'				
					--APISource
					,@APISourceHost AS 'APISource.HostName', @APISourceServiceName AS 'APISource.ServiceName', @APISourceFileName AS 'APISource.SourceFileName'
				FOR JSON PATH
			) AS Source
			,( 
				SELECT 
					--SQLSink
					@SinkServer AS 'SQLSink.ServerName',@SinkDatabase  AS 'SQLSink.DatabaseName', @SinkSchema AS 'SQLSink.SinkSchema', @SinkTable AS 'SQLSink.SinkTable'
					--FileSink
					,@SinkFileRoot AS 'FileSink.FileRoot',  @SinkFilePath AS 'FileSink.FilePath', @SinkFileName AS 'FileSink.FileName'
				FOR JSON PATH
			 ) AS Sink		

		  ,@ColumnMetaData AS ColumnMetadata--replace(@ColumnMetaData,'&quot;','"')  AS ColumnMetaData			
			,( 
				SELECT 
					--predicate based
					CASE WHEN @DeltaDetectType = 'Predicate' THEN @QueryPredicate END  AS 'QueryPredicate', CASE WHEN @DeltaDetectType = 'Predicate' THEN @QueryPredicateType END AS 'QueryPredicateType'
					--change tracking based
					,CASE WHEN @DeltaDetectType = 'ChangeTracking' THEN 'CHANGE_TRACKING_CURRENT_VERSION()' END AS 'CurrentVersion'
					,CASE WHEN @DeltaDetectType IS NULL THEN '' END AS 'No delta detect mechanism at source'
				FOR JSON PATH
			 ) AS DeltaDetect
			,@DeltaDetectType AS DeltaDetectType					
			,@SinkLoadProcedureName AS SinkLoadProcedureName
			,NULLIF(@SinkPreLoadScript,'') AS SinkPreLoadScript	
			,NULLIF(@SinkWriteBatchSize,0) AS SinkWriteBatchSize
			,@DIU  AS DIU
			,@MinExpression AS MinExpression
			,@MaxExpression AS MaxExpression
			,@LoadType AS LoadType 
			,@TableType AS TableType 
			,@SourceTypeID AS SourceTypeID
			,@IsActive AS IsActive
			,@Note AS Note
			,@TriggerType AS TriggerType
			,@SourceQuery AS SourceQuery
			,@IsDeletionRequired AS IsDeletionRequired
			,@SourceTableCompositeKey AS SourceTableCompositeKey
			,@SheetNames AS SheetNames
			,@SourceTableControlID AS ETLSourceTableID
	) AS Src
	ON Tgt.SourceSystem	= Src.SourceSystem
		AND Tgt.Source	= Src.Source	
		AND Tgt.Sink	= Src.Sink 	
		AND ISNULL(Tgt.SinkLoadProcedureName, '') = ISNULL(Src.SinkLoadProcedureName, '')
WHEN MATCHED THEN
	UPDATE 
		SET 
			 Tgt.ColumnMetaData				= Src.ColumnMetaData	
			,Tgt.DeltaDetect				= Src.DeltaDetect
			,Tgt.DeltaDetectType			= Src.DeltaDetectType
			,Tgt.MinExpression				= Src.MinExpression
			,Tgt.MaxExpression				= Src.MaxExpression	
			,Tgt.SinkPreLoadScript			= Src.SinkPreLoadScript
			,Tgt.SinkWriteBatchSize			= Src.SinkWriteBatchSize
			,Tgt.DIU						= Src.DIU
			,Tgt.SourceQuery				= Src.SourceQuery
			,Tgt.TriggerType				= Src.TriggerType
			,Tgt.SourceTypeID				= Src.SourceTypeID
			,Tgt.LoadType					= Src.LoadType
			,Tgt.UpdatedDate				= GETDATE()	
			,Tgt.IsDeletionRequired			= Src.IsDeletionRequired
			,Tgt.SourceTableCompositeKey	= Src.SourceTableCompositeKey
			,Tgt.SheetNames					= Src.SheetNames
			,Tgt.ETLSourceTableID			= Src.ETLSourceTableID
WHEN NOT MATCHED BY TARGET THEN
	INSERT (
			 SourceSystem		
			,Source			
			,Sink
			,ColumnMetaData
			,DeltaDetect
			,DeltaDetectType			
			,SinkLoadProcedureName	
			,SinkPreLoadScript
			,SinkWriteBatchSize	
			,[TriggerType]
			,SourceQuery
			,DIU
			,MinExpression
			,MaxExpression
			,LoadType
			,TableType
			,SourceTypeID
			,IsActive
			,Note
			,UpdatedDate
			,IsDeletionRequired
			,SourceTableCompositeKey
			,SheetNames
			,ETLSourceTableID
			) 
	VALUES (				 
			SourceSystem			
			,Source			
			,Sink
			,ColumnMetaData
			,DeltaDetect
			,DeltaDetectType				
			,SinkLoadProcedureName
			,SinkPreLoadScript
			,SinkWriteBatchSize
			,[TriggerType]
			,SourceQuery
			,DIU
			,MinExpression 		
			,MaxExpression
			,LoadType
			,TableType
			,SourceTypeID
			,IsActive 	
			,Note
			,GETDATE()
			,IsDeletionRequired
			,SourceTableCompositeKey
			,SheetNames
			,ETLSourceTableID
		);  
END
GO


