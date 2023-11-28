CREATE OR ALTER PROCEDURE [jra].[print] (@string nvarchar(max))
AS
BEGIN
	DECLARE @c int = 1
	WHILE @c <= LEN(@string)
	BEGIN
		PRINT(CAST(SUBSTRING(@string, @c, 16000) AS ntext))
		SET @c += 16000
	END
END
GO

-- DECLARE @working_string nvarchar(max)
-- DECLARE @position int, @newline tinyint

-- WHILE LEN(@string) > 0
-- BEGIN
-- 	SET @working_string = SUBSTRING(@string, 1, 4000)
-- 	SET @position = CHARINDEX(CHAR(10), REVERSE(@working_string))
-- 	IF @position = 0
-- 	BEGIN
-- 		SET @position = LEN(@working_string)
-- 		SET @newline = 1
-- 	END
-- 	ELSE
-- 	BEGIN
-- 		SET @position = LEN(@working_string) - @position
-- 		SET @newline = 2
-- 	END
-- 	PRINT(SUBSTRING(@string, 1, @position))
-- 	SET @string = SUBSTRING(@string, @newline + @position, LEN(@string))
-- END