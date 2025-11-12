USE [PHX_ME_MART]
GO
/****** Object:  StoredProcedure [dbo].[usp_LoadMart_Task_Dim]    Script Date: 11/3/2023 3:07:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
*****************************************************************************************************************************
	Name:			[dbo].[usp_LoadMart_Task_Dim]
	Developer:		Dinesh Puvvala
	Description:	Load dbo.usp_LoadMart_Task_Dim from ME GOld database
	Usage:			
					exec [dbo].[usp_LoadMart_Task_Dim] ;
					exec [dbo].[usp_LoadMart_Task_Dim] @GroupID= Y;
					exec [dbo].[usp_LoadMart_Task_Dim] @GroupID= Y, @BatchID =XXX;  (XXX should exist in ETLBatch Table) use @BatchID=4 for testing
    Revision:
				10/06/2023  initial 
*******************************************************************************************************************************
*/

ALTER   PROC [dbo].[usp_LoadMart_Task_Dim]
(
    @GroupID INT = 4,
    @BatchID int = 4
)
AS
BEGIN
    DECLARE @TableName VARCHAR(100),
            @ETLProgram SYSNAME,
            @SourceTable VARCHAR(100),
            @ETLPhaseName VARCHAR(100),
            @ETLPhaseID INT,
            @Comment VARCHAR(1000),
            @ProcessLogID INT,
            @StartTime DATETIME,
            @EndTime DATETIME,
            @StatusID INT,
            @ProcessCount INT = 0,
            @UpdateCount INT = 0,
            @InsertCount INT = 0,
            @ErrorCount int = 0,
            @CurrentTime DATETIME = Getdate(),
            @ExecutionBatchId INT;
    DECLARE @SummaryOfChanges TABLE (Change VARCHAR(20))
    BEGIN TRY
        SET @TableName = 'ME{dbo.Tbl_Task_Dim}'
        SET @ETLProgram = OBJECT_NAME(@@PROCID)
        SET @SourceTable = 'dbo.Tbl_Task'
        SET @ETLPhaseName = 'LoadMart'
        EXEC [dbo].[usp_ETLGetETLPhase] @ETLPhaseName, @ETLPhaseID OUTPUT
        IF @BatchID is null
            SET @ExecutionBatchId = 4
        ELSE
            SET @ExecutionBatchId = @BatchID
        EXEC dbo.usp_ETLCreateProcessLog @BatchId = @ExecutionBatchId,
                                         @ObjectName = @TableName,
                                         @ETLProgram = @ETLProgram,
                                         @SourceTable = @SourceTable,
                                         @ETLPhaseId = @ETLPhaseID,
                                         @GroupID = @GroupID,
                                         @ProcessLogId = @ProcessLogID OUTPUT
        SET @starttime =
        (
            SELECT max(starttime)
            FROM [Phoenix_Audit].[dbo].[Tbl_ETLProcess_Log]
            WHERE ProcessLogId = @ProcessLogID
        )
        --------logic start-----------
        ------------------------------

        /*load incremental data into temptable*/
        DECLARE @lastsuccessrun DATETIME = (
                                               SELECT Last_Load_Successful_Date
                                               FROM [Phoenix_Audit].[dbo].[Tbl_ETL_Load_Control]
                                               WHERE ETL_Program_Name = 'usp_LoadMart_Task_Dim'
                                           ),
                @maxloaddatetime DATETIME = (
                                                SELECT ISNULL(MAX(loaddatetime), '1900-01-01')
                                                FROM [dbo].[Tbl_Task]
                                            );

        --SELECT @lastsuccessrun, @maxloaddatetime;

        /*load incremental data into a temp table*/
        IF object_Id('tempdb..#TaskDim') is NOT NULL
            DROP TABLE #TaskDim 

        SELECT TaskID,
			   src.Description,
			   tt.Description as [Desc]
        INTO #TaskDim 
        FROM [dbo].[Tbl_Task] src WITH (NOLOCK)
		LEFT JOIN [dbo].[Tbl_TaskType] tt
			ON src.TaskTypeID = tt.TaskTypeID
        WHERE src.loaddatetime > @lastsuccessrun
              AND src.loaddatetime <= @maxloaddatetime;

        SET @ProcessCount =
        (
            SELECT COUNT(*) FROM #TaskDim
        )

        /* Update existing records  */
        UPDATE dim 
        SET 
			dim.Task		= src.Description,
			dim.Task_Type	= src.[Desc]
        FROM PHX_ME_MART.dbo.Tbl_Task_Dim dim WITH (nolock)
            INNER JOIN #TaskDim src
                ON dim.Task_ID = src.TaskID

        SET @UpdateCount = @@ROWCOUNT

        /* Insert new records  */
        INSERT INTO PHX_ME_MART.dbo.Tbl_Task_Dim
        (
            Task_ID,
            Task,
			Task_Type
        )
        SELECT src.TaskID,
               src.Description,
			    src.[Desc]
        FROM #TaskDim src
            LEFT JOIN PHX_ME_MART.dbo.Tbl_Task_Dim dim
                ON src.TaskID = dim.Task_ID
        WHERE dim.Task_ID IS NULL;

        SET @InsertCount = @@ROWCOUNT
        ----set @UpdateCount = 0  
        ----set @InsertCount = 0 
        SET @StatusID = [dbo].[udf_ETLGetStatusID]('Success');
        SET @Comment = 'dbo.Tbl_Task_Dim Table was Loaded successfully'
 
         /**************** update control table last successful run************************/

        UPDATE [Phoenix_Audit].[dbo].[Tbl_ETL_Load_Control]
        SET Last_Load_Successful_Date = @maxloaddatetime,
            RowsProcessed = @ProcessCount,
            Rowsinserted = @InsertCount,
            RowsUpdated = @UpdateCount,
            Load_End_Time = @EndTime,
            Load_Start_Time = @StartTime,
            job_run_ID = @ProcessLogID
        WHERE ETL_Program_Name = @ETLProgram

        ------drop existing temp table
        IF object_Id('tempdb..#TaskDim') is NOT NULL
            DROP TABLE #TaskDim

        ---------- logic end -----------
        SET @EndTime = getdate()
        EXEC dbo.usp_ETLUpdateProcessLog @ProcessLogId = @ProcessLogID,
                                         @EndTime = @EndTime,
                                         @StatusID = @StatusID,
                                         @RecordProcessed = @ProcessCount,
                                         @RecordInserted = @InsertCount,
                                         @RecordUpdated = @UpdateCount,
                                         @RecordWithError = @ErrorCount,
                                         @Comment = @Comment

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage VARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;
        DECLARE @ErrorComment VARCHAR(200);
        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();
        DECLARE @ProcedureName SYSNAME;
        SET @ErrorComment = 'Load [dbo].[Tbl_Task_Dim] Failed with one or more data exception'
        SET @TableName = 'ME{dbo.Tbl_Task_Dim}'
        SET @ProcedureName = OBJECT_NAME(@@PROCID);
        DECLARE @SeverityId INT;
        SET @SeverityId = dbo.udf_GetSeverityId('High');
        EXEC dbo.usp_ETLLogError @ETLProgram = @ProcedureName,
                                 @ObjectName = @TableName,
                                 @ProcessLogId = @ProcessLogID,
                                 @Severity = @SeverityId,
                                 @Comment = @ErrorComment,
                                 @ExceptionMessage = @ErrorMessage
        SET @EndTime = getdate()
        SET @StatusID = [dbo].[udf_ETLGetStatusID]('Failure');
        EXEC dbo.usp_ETLUpdateProcessLog @ProcessLogId = @ProcessLogID,
                                         @EndTime = @EndTime,
                                         @StatusId = @Statusid,
                                         @RecordProcessed = @ProcessCount,
                                         @Comment = @ErrorComment
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END

