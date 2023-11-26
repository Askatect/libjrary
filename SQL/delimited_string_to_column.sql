CREATE OR ALTER FUNCTION [jra].[string_split] (
    @string nvarchar(max),
    @splitter nvarchar(max) = ','
)
RETURNS @array table ([value] nvarchar(max))
AS
BEGIN
    IF (SELECT [compatibility_level] FROM sys.databases WHERE [name] = DB_NAME()) <= 130
        INSERT INTO @array SELECT [value] FROM STRING_SPLIT(@string, @splitter)
    ELSE
    BEGIN
        DECLARE @c int = 1,
            @loc int
        SET @string += @splitter
        SET @loc = CHARINDEX(@splitter, @string, @c)
        WHILE @loc > 0
        BEGIN
            INSERT INTO @array
            VALUES (SUBSTRING(@string, @c, @loc - @c));
            SET @c = @loc + LEN(@splitter)
            SET @loc = CHARINDEX(@splitter, @string, @c)
        END
    END
    RETURN;
END
GO