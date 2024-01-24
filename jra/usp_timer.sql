/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Prints time elapsed during processes.

Parameters:
- @Start (datetime): Input process start time.
- @Process varchar(128): Name of process for display. Defaults to 'Process'.
- @End (datetime): End time of process.

Returns:
Prints time elapsed during processes.

Usage:
DECLARE @start datetime
SET @start = GETDATE()
<script>
EXECUTE [jra].[usp_timer] @start, 'Script'
*/

DECLARE @process varchar(512) = NULL,
	@start datetime = NULL,
	@record bit = 1,
	@print bit = 1,
	@display bit = 0

DECLARE @instance int

WAITFOR DELAY '00:00:00'

IF @record = 1 AND (OBJECT_ID('tempdb..#timer', 'U') IS NULL)
BEGIN
	SELECT [r].[session_id],
		@process AS [process],
		1 AS [instance_id], 
		[r].[start_time] AS [task_start],
		[r].[start_time] AS [instance_start],
		GETDATE() AS [instance_end],
		[s].[text] AS [batch_text],
		[r].[sql_handle] AS [batch_id], 
		[r].[plan_handle] AS [plan_id]
	INTO #timer
	FROM sys.dm_exec_requests AS [r]
		CROSS APPLY sys.dm_exec_sql_text([r].[sql_handle]) AS [s]
	WHERE [r].[session_id] = @@SPID
	SET @instance = 1
END
ELSE IF @record = 1
BEGIN
	SET @instance = (SELECT COUNT([instance_id]) + 1 FROM #timer)
	INSERT INTO #timer([session_id], [process], [instance_id], [task_start], [instance_start], [instance_end], [batch_text], [batch_id], [plan_id])
	SELECT [r].[session_id], 
		@process,
		@instance,		
		[r].[start_time],
		COALESCE(@start, (SELECT [instance_end] FROM #timer WHERE [task_start] = [r].[start_time] AND [instance_id] = @instance - 1), [r].[start_time]),
		GETDATE(),
		[s].[text],
		[r].[sql_handle], 
		[r].[plan_handle]
	FROM sys.dm_exec_requests AS [r]
		CROSS APPLY sys.dm_exec_sql_text([r].[sql_handle]) AS [s]
	WHERE [r].[session_id] = @@SPID
END

IF @print = 1
BEGIN
	DECLARE @statement nvarchar(max),
		@duration bigint = IIF(@record = 0, GETDATE(), (SELECT DATEDIFF(millisecond, [instance_start], [instance_end]) FROM #timer WHERE [instance_id] = @instance))
	SET @statement = CONCAT(@process, ' (Instance ', @instance, ') - ', FORMAT((@duration/360000), '###00'), ':', FORMAT((@duration/60000 % 60), 'D2'), ':', FORMAT((@duration/1000 % 60), 'D2'), '.', FORMAT((@duration % 1000), 'D3'))
	PRINT(LTRIM(@statement))
END

SELECT [session_id] AS [Session ID],
	[plan_id] AS [Plan ID],
	[batch_id] AS [Batch ID],
	ISNULL([process], '') AS [Process],
	ROW_NUMBER() OVER(PARTITION BY [task_start] ORDER BY [instance_start] ASC) AS [Process Instance],
	[instance_id] AS [Instance ID],
	[task_start] AS [Task Start],
	MAX([instance_end]) OVER(PARTITION BY [task_start]) AS [Task End],
	DATEDIFF(millisecond, [task_start], MAX([instance_end]) OVER(PARTITION BY [task_start])) AS [Task Duration],
	[instance_start] AS [Instance Start],
	[instance_end] AS [Instance End],
	DATEDIFF(millisecond, [instance_start], [instance_end]) AS [Instance Duration]
FROM #timer
ORDER BY [instance_id] ASC