ALTER PROCEDURE sp_merge_Emp_details_changes
AS
BEGIN
    -- Optional: Silence row count messages
    SET NOCOUNT ON;

    MERGE INTO dbo.Stage_Emp_details_CT AS target
    USING (
        SELECT *
        FROM dbo.Stage_Emp_details_CT
        WHERE __$start_lsn IS NOT NULL   -- ⛔ Filter out problematic rows
    ) AS source
    ON target.Emp_id = source.Emp_id

    -- Perform update if __$operation = 3 (Update)
    WHEN MATCHED AND source.__$operation = 3 THEN
        UPDATE SET 
            Emp_name = source.Emp_name,
            Emp_salary = source.Emp_salary,
            Emp_loc = source.Emp_loc

    -- Perform insert if __$operation = 2 (Insert)
    WHEN NOT MATCHED BY TARGET AND source.__$operation = 2 THEN
        INSERT (Emp_id, Emp_name, Emp_salary, Emp_loc, __$start_lsn, __$end_lsn, __$seqval, __$operation, __$update_mask, __$command_id)
        VALUES (source.Emp_id, source.Emp_name, source.Emp_salary, source.Emp_loc, 
                source.__$start_lsn, source.__$end_lsn, source.__$seqval, source.__$operation, source.__$update_mask, source.__$command_id);
END

drop PROCEDURE sp_merge_Emp_details_changes

CREATE TABLE dbo.stage_Emp_details_CT
(
    [__$start_lsn]     BINARY(10)      NOT NULL,
    [__$end_lsn]       BINARY(10)      NULL,
    [__$seqval]        BINARY(10)      NOT NULL,
    [__$operation]     INT             NOT NULL,
    [__$update_mask]   VARBINARY(128)  NULL,
    [Emp_id]           INT             NOT NULL,
    [Emp_name]         VARCHAR(50)     NULL,
    [Emp_salary]       INT             NULL,
    [Emp_loc]          VARCHAR(50)     NULL,
    [__$command_id]    INT             NULL
)
WITH
(
    DISTRIBUTION = HASH(Emp_id),              -- Use ROUND_ROBIN if unsure or Emp_id is not optimal
    CLUSTERED COLUMNSTORE INDEX
);
