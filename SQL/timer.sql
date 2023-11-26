CREATE PROCEDURE [jra].[timer] (
	@Start datetime,
	@Process varchar(128) = 'Process',
	@End datetime = NULL
)
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