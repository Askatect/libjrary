CREATE OR ALTER PROCEDURE [jra].[usp_print] (@string nvarchar(max))
AS

SET @string = REPLACE(@string, CHAR(13), CHAR(10))

DECLARE @working_string nvarchar(max),
	@position int, 
	@newline tinyint

WHILE LEN(@string) > 0
BEGIN
	SET @working_string = SUBSTRING(@string, 1, 4000)
	SET @position = CHARINDEX(CHAR(10), REVERSE(@working_string))
	IF @position = 0
	BEGIN
		SET @position = LEN(@working_string)
		SET @newline = 1
	END
	ELSE
	BEGIN
		SET @position = LEN(@working_string) - @position
		SET @newline = 2
	END
	PRINT(SUBSTRING(@string, 1, @position))
	SET @string = SUBSTRING(@string, @newline + @position, LEN(@string))
END
