CREATE OR ALTER FUNCTION dbo.age(@Start date, 
	@End date = NULL)
RETURNS int
BEGIN
	RETURN (CAST(FORMAT(ISNULL(@End, GETDATE()), 'yyyyMMdd') AS int) - CAST(FORMAT(@Start, 'yyyyMMdd') AS int))/10000
END
GO