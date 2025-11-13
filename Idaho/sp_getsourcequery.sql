/****** Object:  StoredProcedure [CTL].[usp_GetSourceQuery]    Script Date: 11/13/2025 1:20:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [CTL].[usp_GetSourceQuery](	
	@ETLAuditLogID BIGINT 
	,@ETLStartDateTime DATETIME2(3)
)
AS 

/*
Altered 04/24/2025 by Syed Zaidi 
Added line below to read SHAPE column as null from source GIS views only
WHEN @SourceType = 'oracledbvw' AND (@DeltaDetectType = 'Unknown' OR @LoadType='Full') AND @SourceSystem = 'GIS' THEN CONCAT('SELECT ', REPLACE(REPLACE(REPLACE(@QueryColumn,'[' ,''),']',''),'SHAPE', ''''' as SHAPE' ), ' FROM ', Src.SourceSchema, '.',Src.SourceTable)
*/
BEGIN

	--DECLARE  @ETLAuditLogID BIGINT = 24751
	--DECLARE @ETLStartDateTime DATETIME2(3) = GETDATE()

	DECLARE @QueryColumn VARCHAR(MAX)	
	DECLARE @DeltaDetectType VARCHAR(100)	
	DECLARE @SourceSystem VARCHAR(100)
	DECLARE @TableType VARCHAR(100) 
	DECLARE @SourceType VARCHAR(100) 
	DECLARE @LoadType VARCHAR(100)
	SELECT @QueryColumn = STRING_AGG( CAST(QUOTENAME(Cmd.QueryColumns) AS VARCHAR(MAX)), ', ')		
		,@DeltaDetectType = ISNULL(NULLIF(MAX(A.DeltaDetectType),''),'Unknown')
		,@LoadType = ISNULL(NULLIF(MAX(A.LoadType),''),'Unknown')
		,@SourceSystem = A.SourceSystem
		,@SourceType= ISNULL(NULLIF(MAX(STY.SourceType),''),'Unknown')
	FROM CTL.ETLAuditLog A
	JOIN CTL.ETLControl C
		ON A.ETLControlID = C.ETLControlID
    JOIN CTL.ETLSourceType STY
	ON STY.SourceTypeID = C.SourceTypeID
	CROSS APPLY OPENJSON(ISNULL(C.ColumnMetaData,'[{"Not Available":""}]'))
	WITH 
	(
		--ColumnMetaData
		QueryColumns VARCHAR(100) '$.ColumnName'		
	) AS Cmd
	WHERE A.ETLAuditLogID = @ETLAuditLogID  
	 GROUP BY  A.SourceSystem
	-- Generate Source Query
	SELECT 
		A.ETLAuditLogID
		,A.ETLControlID        
		,CASE
		    --Added to support reading data from GIS by skipping ST_Geometry datatype.
			WHEN @SourceType = 'oracledbvw' AND (@DeltaDetectType = 'Unknown' OR @LoadType='Full') AND @SourceSystem = 'GIS' THEN CONCAT('SELECT ', REPLACE(REPLACE(REPLACE(@QueryColumn,'[' ,''),']',''),'SHAPE', ''''' as SHAPE' ), ' FROM ', Src.SourceSchema, '.',Src.SourceTable)
		    WHEN @SourceType = 'oracledb' AND (@DeltaDetectType = 'Unknown' OR @LoadType='Full')  THEN CONCAT('SELECT ', '* ', ' FROM ', Src.SourceSchema, '.',Src.SourceTable)
			WHEN @SourceType = 'oracledb' AND (@DeltaDetectType = 'Predicate' AND @LoadType='Incremental') THEN CONCAT('SELECT ', '* ', ' FROM ', Src.SourceSchema, '.',Src.SourceTable,' WHERE To_TIMESTAMP( ',  Delta.QueryPredicate , ', ''YYYY-MM-DD HH24:MI:SS.FF'')' ,' > To_TIMESTAMP(' ,'''',A.ETLMinValue,'''',',','''','YYYY-MM-DD HH24:MI:SS.FF','''',')', ' AND To_TIMESTAMP(' , Delta.QueryPredicate , ', ''YYYY-MM-DD HH24:MI:SS.FF'')' , ' <= To_TIMESTAMP( ' ,'''', SUBSTRING(A.ETLMaxvalue,1,19) ,'''',',','''','YYYY-MM-DD  HH24:MI:SS.FF','''',')')
		    --Added to support reading data from VegMgmt by skipping ST_Geometry datatype and Image data. (Date : 7/1/2025)
			WHEN @SourceType = 'sql' AND (@DeltaDetectType = 'Unknown' OR @LoadType='Full') THEN CONCAT('SELECT ', REPLACE(REPLACE(@QueryColumn, '[SIGNATURE]', ''''' as SIGNATURE'),'[Shape]', ''''' as SHAPE'),' FROM ', '[' + Src.SourceSchema + ']', '.', '[' + Src.SourceTable + ']') 
			--THEN CONCAT('SELECT ', @QueryColumn +  ' FROM ', Src.SourceSchema, '.', '['+Src.SourceTable+']', ' s')
			WHEN @SourceType = 'sql' AND (@DeltaDetectType = 'Predicate' AND @LoadType='Incremental') THEN CONCAT('SELECT ', REPLACE(REPLACE(@QueryColumn, '[SIGNATURE]', ''''' as SIGNATURE'),'[Shape]', ''''' as SHAPE'), ' FROM ', '[' + Src.SourceSchema + ']', '.', '[' + Src.SourceTable + ']', '  WHERE ',  Delta.QueryPredicate , ' > ' ,'''', A.ETLMinValue,'''', ' AND ' , Delta.QueryPredicate , ' <= ' ,'''', A.ETLMaxValue,'''')
			--THEN CONCAT('SELECT ',  @QueryColumn, ' FROM ',  Src.SourceSchema, '.', Src.SourceTable, '  WHERE ',  Delta.QueryPredicate , ' > ' ,'''', A.ETLMinValue,'''', ' AND ' , Delta.QueryPredicate , ' <= ' ,'''', A.ETLMaxValue,'''') 								 
			WHEN @SourceType = 'sql' AND (@DeltaDetectType = 'ChangeTracking' AND @LoadType='Incremental') THEN (SELECT [CTL].[fn_getChangeTrackQuery] (@ETLAuditLogID,A.ETLMinValue,A.ETLMaxValue )) 			
			WHEN @SourceType = 'file' THEN  NULL
			--Added @SourceType = 'api' lines to support API source type (BBrandini)
			WHEN @SourceType = 'api' AND A.Source LIKE '%APISource%' THEN  CONCAT(ASrc.APIHostName, ASrc.APIServiceName)
			WHEN @SourceType = 'api' AND A.Source LIKE '%APIJSONSource%' THEN REPLACE(REPLACE(ASrc.APIJSONQuery, 'dls/', CONCAT(Snk.SinkFileRoot, '/',
				REPLACE(REPLACE(REPLACE(Snk.SinkFilePath,'YYYY',DATEPART(YY,@ETLStartDateTime)), 'MM', FORMAT(@ETLStartDateTime,'MM')), 'DD', FORMAT(@ETLStartDateTime,'dd')),
					REPLACE(REPLACE(Snk.SinkFileName,'*', CONCAT(CONVERT(CHAR(8), @ETLStartDateTime, 112),'_', FORMAT(@ETLStartDateTime,'HH'), FORMAT(@ETLStartDateTime,'mm'), FORMAT(@ETLStartDateTime,'ss'))), 'parquet', 'json'))
					), '\', '/')
			ELSE CONCAT('SELECT ', '* ', ' FROM ', Src.SourceSchema, '.',Src.SourceTable)
			--WHEN @DeltaDetectType = 'Unknown' THEN CONCAT('SELECT ', @QueryColumn +  ' FROM ', Src.SourceSchema, '.', '['+Src.SourceTable+']', ' s') 
		 END AS ETLQuery  	  
		,ISNULL(Snk.SinkFileRoot,'raw\') AS SinkFileRoot
		,REPLACE(REPLACE(REPLACE(Snk.SinkFilePath,'YYYY',DATEPART(YY,@ETLStartDateTime)),'\MM\', CONCAT('\',FORMAT(@ETLStartDateTime,'MM'),'\')),'\DD\',CONCAT('\',FORMAT(@ETLStartDateTime,'dd'),'\')) AS SinkFilePath	
		,REPLACE(Snk.SinkFileName,'*', CONCAT(CONVERT(CHAR(8), @ETLStartDateTime, 112),'_', FORMAT(@ETLStartDateTime,'HH'), FORMAT(@ETLStartDateTime,'mm'), FORMAT(@ETLStartDateTime,'ss'))) AS SinkFileName	     
		,ISNULL(A.ETLMinValue,'1900-01-01') AS ETLMinValue
		,ISNULL(A.ETLMaxvalue,'9999-12-31') AS ETLMaxValue
	FROM CTL.ETLAuditLog A		
	CROSS APPLY OPENJSON(A.Source)
		WITH
		(
			--SQLSource	
			SourceDBName VARCHAR(100) '$.SQLSource.DatabaseName',	
			SourceSchema VARCHAR(100) '$.SQLSource.SchemaName',
			SourceTable  VARCHAR(100) '$.SQLSource.TableName'
		) AS Src
	CROSS APPLY OPENJSON(A.Sink)
		WITH
		(	--FileSink
			SinkFileRoot VARCHAR(100) '$.FileSink.FileRoot',
			SinkFilePath VARCHAR(100) '$.FileSink.FilePath',
			SinkFileName VARCHAR(100) '$.FileSink.FileName'  
		) AS Snk

	--Added to support API source type (BBrandini)
	CROSS APPLY OPENJSON(A.Source)
		WITH
		(	
			-- APISource
			 APIHostName VARCHAR(100) '$.APISource.HostName'
			,APIServiceName VARCHAR(255) '$.APISource.ServiceName'
			,APISourceFileName VARCHAR(100) '$.APISource.SourceFileName'
			,APIJSONQuery VARCHAR(MAX) '$.APIJSONSource.JSONQuery'
		) AS ASrc

	CROSS APPLY OPENJSON(A.DeltaDetect)
	WITH 
	(
		--DeltaDetection
		QueryPredicate VARCHAR(100) '$.QueryPredicate',
		QueryPredicateType VARCHAR(100) '$.QueryPredicateType',
		CurrentVersion VARCHAR(100) '$.CurrentVersion'	
	) AS Delta
	WHERE A.ETLAuditLogID = @ETLAuditLogID 	
END
GO


