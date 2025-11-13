/****** Object:  StoredProcedure [CTL].[usp_ETLAuditLog]    Script Date: 11/13/2025 1:19:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [CTL].[usp_ETLAuditLog] 
	@ETLAuditLogID BIGINT = NULL 
	,@ETLBatchID BIGINT
	,@ETLControlID INT
	,@SourceSystem VARCHAR(50)
	,@PipelineName VARCHAR(50)
	,@PipelineTriggerType VARCHAR(50)
	,@LogType VARCHAR(50)
	,@Source varchar(max)		
	,@Sink varchar(max)
	,@DeltaDetect varchar(max)		
	,@DeltaDetectType varchar(100)
	,@ETLMinValueAdhoc varchar(50) = NULL
	,@ETLMaxValueAdhoc varchar(50) = NULL
	,@ETLMaxValue varchar(50)
	,@SourceQuery varchar(max) = NULL
	,@ETLStartTime varchar(50)
	,@RowsInserted BIGINT
	,@RowsUpdated BIGINT
	,@RowsMarkedAsDeleted BIGINT
	,@FileSizeInBytes BIGINT
	,@ThroughputInKbps DECIMAL(20,2)		
	,@ErrorMsg NVARCHAR(4000) 
	,@LoadStatus VARCHAR(50)
	,@ActivityID NVARCHAR(30) 
	,@ActivityName NVARCHAR(200)	
	,@MaxExpression VARCHAR(MAX)
	,@LoadType VARCHAR(20)
	,@AuditLogID BIGINT OUTPUT
AS	

SET NOCOUNT ON;

DECLARE @CurrentDate DATETIME2(3) = GETDATE()
DECLARE @IsStageLoaded BIT = 0
DECLARE @TableType VARCHAR(20) 
DECLARE @ETLMinValue VARCHAR(50) 
DECLARE @AdhocLoad BIT = 0				--new variable for adhoc load part
DECLARE @OffsetDays INT

--SET @ETLMaxValue= CASE WHEN @DeltaDetectType = 'ChangeTracking' THEN @ETLMaxValue
--                          WHEN @DeltaDetectType = 'Predicate'      THEN CONVERT(VARCHAR,GETDATE(),121)
--						  ELSE CONVERT(VARCHAR,GETDATE(),121) END

SET @AdhocLoad = CASE WHEN @LoadType = 'Adhoc' THEN 1 ELSE 0 END

IF @LoadStatus = 'Started'  
BEGIN	

	-- SET @TableType = (SELECT TOP 1 TableType FROM [CTL].[ETLControl] WHERE ETLControlID = @ETLControlID)
	
	SELECT @DeltaDetect = DeltaDetect
       ,@DeltaDetectType =  DeltaDetectType
	   ,@LoadType = LoadType
	   ,@TableType = TableType
	   ,@OffsetDays = 0--ISNULL(OffsetDays, 0)
	   FROM [CTL].[ETLControl] 
	   WHERE ETLControlID = @ETLControlID		 

	   SET @ETLMaxValue = CASE WHEN @TableType <> 'Source' THEN  CONVERT(VARCHAR,GETDATE(),121) ELSE @ETLMaxValue END
   
	IF @LoadType = 'Full'
	BEGIN			
			IF @DeltaDetectType = 'ChangeTracking'
			BEGIN
				SET @ETLMinValue = 0
			END

			IF @DeltaDetectType = 'Predicate'
			BEGIN				
				SET @ETLMinValue = CASE WHEN @DeltaDetect LIKE '%DATE%' THEN '1900-01-01 00:00:00.000' ELSE '0' END
			END	 						
	END
	ELSE IF @AdhocLoad = 1		--new condition for adhoc load part
	BEGIN			
			SET @ETLMinValueAdhoc = CAST(REPLACE(@ETLMinValueAdhoc,'''','') as DATETIME2(3))
			SET @ETLMaxValueAdhoc = CAST(REPLACE(@ETLMaxValueAdhoc,'''','') as DATETIME2(3))
			
	END
	ELSE
	--Incremental
	BEGIN
			IF @DeltaDetectType = 'ChangeTracking'
			BEGIN				
				SET @ETLMinValue = (
									--Get the max value of last successful run
									SELECT ISNULL(MAX(CAST(elog.ETLMaxValue AS BIGINT)), 0) 
									FROM CTL.ETLAuditLog elog								
									WHERE elog.ETLControlID = @ETLControlID
										AND elog.IsLoadSuccess = 1
										AND elog.RowsInserted > 0 
										and elog.ETLStartTime >= (Select MAX(ETLStartTime) from CTL.ETLAuditLog elog2
																	where elog2.ETLControlID = elog.ETLControlID
																	and elog2.LoadType = CASE WHEN (SELECT top 1 1 from CTL.ETLAuditLog c where c.ETLControlID = elog2.ETLControlID and LoadType = 'Full') IS NULL then 'Incremental' else 'Full' end)
					
								)				
			END

			IF @DeltaDetectType = 'Predicate'		
			BEGIN			
				SET @ETLMinValue = (
									--Get the max value of last successful run
									--SELECT ISNULL(MAX(elog.ETLMaxValue), CASE WHEN @DeltaDetect LIKE '%DATE%' THEN '1900-01-01 00:00:00.000' ELSE '0' END) 
                                    SELECT CASE 
                                        -- if the predicate is DATE, then use a date expression
                                        WHEN @DeltaDetect LIKE '%DATE%' AND @OffsetDays = 0
											-- force a data expression if the last successful incremental predicate wasn't date based with 1900-01-01 as the starting point
											THEN CONVERT(VARCHAR(50), ISNULL(TRY_CAST(MAX(elog.ETLMaxValue) AS DATETIME2(0)), '1900-01-01 00:00:00.000'), 121)
										----if the predicate is DATE and there is offset days mentioned in Control table
										WHEN @DeltaDetect LIKE '%DATE%' AND @OffsetDays <> 0
											-- look back for the offset days to do the incremental loads
											THEN CONVERT(VARCHAR(50), DATEADD(dd, @OffsetDays, MAX(elog.ETLMaxValue)), 121)
                                        -- otherwise use the max value, set as 0 if no value present
                                        ELSE ISNULL(MAX(elog.ETLMaxValue), '0')
                                    END 
									FROM CTL.ETLAuditLog elog								
									WHERE elog.ETLControlID = @ETLControlID
										AND elog.IsLoadSuccess = 1
										AND elog.RowsInserted > 0
								)							
			END 			
	END	

	;WITH LogData AS
	(
	SELECT @ETLBatchID AS ETLBatchID
		  ,@ETLControlID AS ETLControlID 		
		  ,@SourceSystem AS SourceSystem
		  ,@PipelineName AS PipelineName
		  ,@PipelineTriggerType AS PipelineTriggerType
		  ,@LogType	AS LogType	  
		  ,@Source AS Source		 
		  ,@Sink AS Sink
		  ,@DeltaDetect AS DeltaDetect
		  ,@DeltaDetectType	AS DeltaDetectType		 		 
		  ,CASE WHEN @ETLMinValue >= NULLIF(@ETLMaxValue,'-1') then '1900-01-01' else @ETLMinValue end AS ETLMinValue	
		  ,NULLIF(@ETLMaxValue,'-1') AS ETLMaxValue
		  ,@SourceQuery AS SourceQuery
		  ,NULLIF(@MaxExpression,'-1') AS MaxExpression
		  ,@RowsInserted AS RowsInserted
		  ,@RowsUpdated AS RowsUpdated	
          ,@RowsMarkedAsDeleted AS RowsMarkedAsDeleted	  
		  ,CAST(REPLACE(@ETLStartTime,'''','') as DATETIME2(3)) AS ETLStartTime	
		  ,@TableType AS TableType		
		  ,CASE WHEN @AdhocLoad = 1 THEN 'Adhoc' ELSE @LoadType END AS LoadType		
		  ,@LoadStatus AS LoadStatus
		  ,0 AS IsLoadSuccess
		  ,@ActivityID AS ActivityID
		  ,@ActivityName AS ActivityName	
		)		
 
	INSERT INTO CTL.ETLAuditLog
		(
		   ETLBatchID
		  ,ETLControlID
		  ,SourceSystem
		  ,PipelineName
		  ,PipelineTriggerType
		  ,LogType		
		  ,Source		
		  ,Sink
		  ,DeltaDetect
		  ,DeltaDetectType		
		  ,ETLMinValue 
		  ,ETLMaxValue	
		  ,SourceQuery
		  ,MaxExpression
		  ,RowsInserted
		  ,RowsUpdated
		  ,RowsMarkedAsDeleted
		  ,ETLStartTime	
		  ,TableType		
		  ,LoadType
		  ,LoadStatus
		  ,IsLoadSuccess
		  ,ActivityID
		  ,ActivityName 
		) 
	SELECT  ETLBatchID
		  ,ETLControlID
		  ,SourceSystem
		  ,PipelineName
		  ,PipelineTriggerType
		  ,LogType		
		  ,Source		 
		  ,Sink	 
		  ,DeltaDetect
		  ,DeltaDetectType	 	 
		  ,CASE WHEN @AdhocLoad = 1  THEN @ETLMinValueAdhoc ELSE ETLMinValue END AS ETLMinValue 
		  ,CASE WHEN @AdhocLoad = 1  THEN @ETLMaxValueAdhoc ELSE ETLMaxValue END AS ETLMaxValue
		  ,SourceQuery
		  ,MaxExpression
		  ,RowsInserted
		  ,RowsUpdated
		  ,RowsMarkedAsDeleted
		  ,ETLStartTime	
		  ,TableType	 
		  ,LoadType
		  ,LoadStatus
		  ,IsLoadSuccess
		  ,ActivityID
		  ,ActivityName 
	FROM LogData		  	

	SET @AuditLogID =  SCOPE_IDENTITY()   
	SELECT @AuditLogID AS AuditLogID 			
END

IF @LoadStatus = 'Completed'
BEGIN
		UPDATE CTL.ETLAuditLog
		SET ETLEndTime = @CurrentDate			
			,RunTimeInSecond = DATEDIFF(ss,ETLStartTime,@CurrentDate)
			--,ETLMaxValue = NULLIF(@ETLMaxValue,'-1')
			,SourceQuery = @SourceQuery
			,RowsInserted =  @RowsInserted 
			,RowsUpdated =  @RowsUpdated
			,RowsMarkedAsDeleted =  @RowsMarkedAsDeleted
			,FileSizeInBytes = @FileSizeInBytes
			,ThroughputInKbps = @ThroughputInKbps
			,LoadStatus = @LoadStatus		
			,ActivityID = @ActivityID
			,ActivityName  = @ActivityName
			,Sink = COALESCE(@Sink,REPLACE(REPLACE(REPLACE(REPLACE(Sink,'*', CONCAT(CONVERT(CHAR(8), ETLStartTime, 112),'_', FORMAT(ETLStartTime,'HH'), FORMAT(ETLStartTime,'mm'), FORMAT(ETLStartTime,'ss'))),'YYYY', DATEPART(YY,ETLStartTime)),'\MM\',CONCAT('\',FORMAT(ETLStartTime,'MM'),'\')),'\DD\',CONCAT('\',FORMAT(ETLStartTime,'dd'),'\')))			
			,IsStageLoaded = CASE WHEN TableType = 'Source' THEN @IsStageLoaded ELSE 1 END
			,IsLoadSuccess = 1
		WHERE ETLAuditLogID = 	@ETLAuditLogID		
END


IF @LoadStatus = 'Failed' 
BEGIN
		UPDATE CTL.ETLAuditLog
		SET ETLEndTime = @CurrentDate	
			,RunTimeInSecond = DATEDIFF(ss,ETLStartTime,@CurrentDate)
			,SourceQuery = @SourceQuery		
			,LoadStatus = @LoadStatus
			,ActivityID = @ActivityID
			,ActivityName  = @ActivityName			
			,ErrorMsg = @ErrorMsg
			,IsLoadSuccess = 0
		WHERE ETLAuditLogID = 	@ETLAuditLogID;			
END




GO


