CREATE OR ALTER FUNCTION [utl].[extract_pattern] (
    @string nvarchar(max),
    @pattern nvarchar(max) = '[ABCDEFGHIJKLMNOPQRSTUVWXYZ,0-9]'
)
RETURNS nvarchar(max)
AS
BEGIN
    DECLARE @c int = 1
    WHILE @c <= LEN(@string)
    BEGIN  
        IF PATINDEX(@pattern, SUBSTRING(@string, @c, 1) COLLATE Latin1_General_CS_AI) = 0
        BEGIN
            SET @string = STUFF(@string, @c, 1, '')
            CONTINUE
        END
        SET @c += 1
    END
RETURN @string
END