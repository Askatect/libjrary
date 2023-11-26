CREATE OR ALTER PROCEDURE [jra].[delimited_string_to_table] (
    @string nvarchar(max),
    @separator nvarchar(max) = ',',
    @row_separator nvarchar(max) = ';',
    @header bit = 1,
    @print bit = 1,
    @display bit = 1
)
AS
IF @print = 0
    SET NOCOUNT ON

DECLARE @cmd nvarchar(max),
    @row nvarchar(max),
    @col_count int,
    @row_count int,
    @c int = 1

SET @row_count = LEN(@string) - LEN(REPLACE(@string, @row_separator, ''))

IF @row_count = 0
    SET @row = @string
ELSE
    SET @row = SUBSTRING(@string, 1, CHARINDEX(@row_separator, @string, 1) - 1)

SET @col_count = 1 + LEN(@row) - LEN(REPLACE(@row, @separator, ''))

IF @header = 0
BEGIN
    SET @row = ''
    WHILE @c <= @col_count
    BEGIN
        SET @row += CONCAT('Column', @c, @separator)
        SET @c += 1
    END
END
ELSE
    SET @row += @separator

SET @row = REPLACE(@row, @separator, ' nvarchar(max), ')
SET @row = LEFT(@row, LEN(@row) - 1)

DROP TABLE IF EXISTS ##output; SELECT '' AS [ ] INTO ##output -- Beat the intellisense.

SET @cmd = CONCAT('DROP TABLE IF EXISTS ##output; 
CREATE TABLE ##output (', @row, ')')
IF @print = 1
    PRINT(@cmd)
EXEC(@cmd)

SET @cmd = CONCAT('INSERT INTO ##output
VALUES ', IIF(@header = 1, '--', ''), '(''', REPLACE(REPLACE(@string, @separator, ''', '''), @row_separator, CONCAT('''), ', CHAR(10), CHAR(9), '(''')), ''')'
)
IF @print = 1
    PRINT(@cmd)
EXEC(@cmd)

IF @display = 1
    SELECT * FROM ##output