CREATE OR ALTER PROCEDURE [jra].[usp_timer] (
	@Start datetime,
	@Process varchar(128) = 'Process',
	@End datetime = NULL
)
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
AS
BEGIN
SET @End = ISNULL(@End, GETDATE())
DECLARE @Length int = DATEDIFF(second, @Start, @End)
DECLARE @Hours int = @Length/3600,
	@Minutes int = (@Length % 3600)/60,
	@Seconds int = @Length % 60
PRINT(CONCAT(@Process, ' started at ', FORMAT(@Start, 'HH:mm:ss'), ' and finished at ', FORMAT(@End, 'HH:mm:ss'), '.'))
PRINT(CONCAT('This is a duration of ', 
		IIF(@Hours = 0, '', CONCAT(@Hours, ' hour', IIF(@Hours = 1, ', ', 's, '))), 
		IIF(@Minutes = 0, '', CONCAT(@Minutes, ' minute', IIF(@Minutes = 1, ' and ', 's and '))),
			@Seconds, ' second', IIF(@Seconds = 1, '.', 's.'), CHAR(10)))
END
GO