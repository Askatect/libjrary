CREATE OR ALTER FUNCTION [jra].[ufn_string_split] (
    @string nvarchar(max),
    @separator nvarchar(max) = ','
)
RETURNS @array table ([value] nvarchar(max))
/*
Version: 1.1
Author: JRA
Date: 2024-01-08
History:
- 1.1: Whitespace of values is trimmed.

Description:
Splits a string into a tabulated column, with column name [value].

Parameters:
- @string (nvarchar(max)): The delimited string of data to turn into an array.
- @separator (nvarchar(max)): The delimiter. Defaults to ','.

Returns:
- @array (table): Tabulated data from input string.

Usage:
[jra].[ufn_string_split]('Link,Zelda,Ganondorf', DEFAULT)
>>> #===========#
    | value     |
    #===========#
    | Link      |
    +-----------+
    | Zelda     |
    +-----------+
    | Ganondorf |
    +-----------+
*/
AS
BEGIN
    IF (SELECT [compatibility_level] FROM sys.databases WHERE [name] = DB_NAME()) <= 130
        INSERT INTO @array SELECT RTRIM(LTRIM([value])) FROM STRING_SPLIT(@string, @separator)
    ELSE
    BEGIN
        DECLARE @c int = 1,
            @loc int
        SET @string += @separator
        SET @loc = CHARINDEX(@separator, @string, @c)
        WHILE @loc > 0
        BEGIN
            INSERT INTO @array
            VALUES (RTRIM(LTRIM(SUBSTRING(@string, @c, @loc - @c))));
            SET @c = @loc + LEN(@separator)
            SET @loc = CHARINDEX(@separator, @string, @c)
        END
    END
    RETURN;
END
GO