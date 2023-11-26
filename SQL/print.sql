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