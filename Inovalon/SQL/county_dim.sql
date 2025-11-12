--============County_Dim==============================

USE [PHX_ME_MART]
GO
/****** Object:  StoredProcedure [dbo].[usp_LoadMart_County_Dim]    Script Date: 11/3/2023 2:52:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
*****************************************************************************************************************************
	Name:			[dbo].[usp_LoadMart_County_Dim]
	Developer:		Dinesh Puvvala
	Description:	Load dbo.Tbl_County_Dim from ME GOld database
	Usage:			
					exec [dbo].[usp_LoadMart_County_Dim] ;
					exec [dbo].[usp_LoadMart_County_Dim] @GroupID= Y;
					exec [dbo].[usp_LoadMart_County_Dim] @GroupID= Y, @BatchID =XXX;  (XXX should exist in ETLBatch Table) use @BatchID=4 for testing
    Revision:
				10/04/2023  initial 
*******************************************************************************************************************************
*/
ALTER   PROC [dbo].[usp_LoadMart_County_Dim]
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
        SET @TableName = 'ME{dbo.Tbl_County_Dim}'
        SET @ETLProgram = OBJECT_NAME(@@PROCID)
        SET @SourceTable = 'dbo.tbCounties'
        SET @ETLPhaseName = 'LoadMart'

        EXEC [dbo].[usp_ETLGetETLPhase] @ETLPhaseName, @ETLPhaseID OUTPUT

        if @BatchID is null
            set @ExecutionBatchId = 4
        Else
            set @ExecutionBatchId = @BatchID


        EXEC dbo.usp_ETLCreateProcessLog @BatchId = @ExecutionBatchId,
                                         @ObjectName = @TableName,
                                         @ETLProgram = @ETLProgram,
                                         @SourceTable = @SourceTable,
                                         @ETLPhaseId = @ETLPhaseID,
                                         @GroupID = @GroupID,
                                         @ProcessLogId = @ProcessLogID OUTPUT;

		SET @starttime =
			(
			    SELECT max(starttime)
			    FROM [Phoenix_Audit].[dbo].[Tbl_ETLProcess_Log]
			    WHERE ProcessLogId = @ProcessLogID
			)

        --------logic start-----------
        DECLARE @lastsuccessrun DATETIME = (
                                               SELECT TOP 1
                                                   Last_Load_Successful_Date
                                               FROM [Phoenix_Audit].[dbo].[Tbl_ETL_Load_Control]
                                               WHERE ETL_Program_Name = 'usp_LoadMart_County_Dim'
                                               ORDER BY Last_Load_Successful_Date DESC
                                           ),
                @maxloaddatetime DATETIME = (
                                                SELECT ISNULL(MAX(loaddatetime), '1900-01-01')
                                                FROM PHX_GOLD_ME.dbo.tbCounties
                                            );
        --SELECT @lastsuccessrun, @maxloaddatetime;

        /*load incremental data into a temp table*/

        IF object_Id('tempdb..#CountiesDIM') is NOT NULL
            DROP TABLE #CountiesDIM


        SELECT src.cnt_id, 
			   src.cnt_name,
			   st.st_name,
			   st.st_code 
        INTO #CountiesDIM
        FROM PHX_GOLD_ME.[dbo].[tbCounties] src WITH (NOLOCK)
			LEFT JOIN PHX_GOLD_ME.dbo.tbStates st
				ON src.st_code = st.st_code
			WHERE src.loaddatetime >= @lastsuccessrun
			AND src.loaddatetime <= @maxloaddatetime


        set @ProcessCount =
        (
            select count(*) from #CountiesDIM
        )
        /************************************************************************************************/
        /* Update existing records  */
        UPDATE dim
        SET 
            dim.County = src.cnt_name,
            dim.State = src.st_name,
            dim.State_Code = src.st_code
        FROM PHX_ME_MART.dbo.Tbl_County_Dim dim WITH (NOLOCK)
            INNER JOIN #CountiesDIM src
                ON dim.cnt_id = src.cnt_id

        SET @UpdateCount = @@ROWCOUNT

        /* Insert new records  */
        INSERT INTO PHX_ME_MART.dbo.Tbl_County_Dim
        (
			   cnt_id,
               County,
               State,
               State_Code
        )
        SELECT  src.cnt_id,
				src.cnt_name,
				src.st_name,
				src.st_code 
        FROM #CountiesDIM src
            LEFT JOIN PHX_ME_MART.dbo.Tbl_County_Dim dim
                ON src.cnt_id = dim.cnt_id
         WHERE dim.cnt_id IS NULL;
        /********************************************************************************************/
        set @InsertCount = @@ROWCOUNT
        ----set @UpdateCount = 0  
        ----set @InsertCount = 0
        SET @StatusID = [dbo].[udf_ETLGetStatusID]('Success');
        SET @Comment = 'dbo.Tbl_County_Dim Table was Loaded successfully'
        -- set @ProcessCount=(select count(*) from dbo.Tbl_County_Dim)


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

	    IF object_Id('tempdb..#CountiesDIM') is NOT NULL
            DROP TABLE #CountiesDIM

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
        Set @ErrorComment = 'Load [dbo].[Tbl_County_Dim] Failed with one or more data exception'
        set @TableName = 'ME{Tbl_County_Dim}'
        SET @ProcedureName = OBJECT_NAME(@@PROCID);
        DECLARE @SeverityId INT;
        Set @SeverityId = dbo.udf_GetSeverityId('High');
        EXEC dbo.usp_ETLLogError @ETLProgram = @ProcedureName,
                                 @ObjectName = @TableName,
                                 @ProcessLogId = @ProcessLogID,
                                 @Severity = @SeverityId,
                                 @Comment = @ErrorComment,
                                 @ExceptionMessage = @ErrorMessage

        SET @EndTime = getdate()

        set @StatusID = [dbo].[udf_ETLGetStatusID]('Failure');


        EXEC dbo.usp_ETLUpdateProcessLog @ProcessLogId = @ProcessLogID,
                                         @EndTime = @EndTime,
                                         @StatusId = @Statusid,
                                         @RecordProcessed = @ProcessCount,
                                         @Comment = @ErrorComment

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END


