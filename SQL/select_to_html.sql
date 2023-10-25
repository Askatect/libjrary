CREATE OR ALTER PROCEDURE [dbo].[select_to_html] (
    @query varchar(max),
    @order_by varchar(max) = NULL,
	@out varchar(max) = '' OUTPUT
)
AS
BEGIN
    DECLARE @main char(7) = '#181848',
        @black char(7) = '#000000',
        @white char(7) = '#ffffff',
        @dark_accent char(7) = '#541b8c',
        @light_accent char(7) = '#72abe3',
        @grey char(7) = '#cfcfcf',
        @negative char(7) = '#8c1b1b',
        @null char(7) = '#8c8c1b',
        @positive char(7) = '#1b8c1b'

    DECLARE @cmd nvarchar(max) = CONCAT('
        DROP TABLE IF EXISTS [dbo].[temp];
        
        SELECT * 
        INTO [dbo].[temp] 
        FROM (', @query, ') AS [T]
    ')
    EXEC(@cmd)

    DROP TABLE IF EXISTS #columns;

    SELECT [column_id] AS [c],
        [name] AS [col],
        CASE WHEN [system_type_id] IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127, 40, 41, 42, 43, 61, 189) THEN 1
            ELSE 0
            END AS [numeric],
        CONVERT(float, 0.0) AS [min],
        CONVERT(float, 0.0) AS [max]
    INTO #columns
    FROM [sys].[columns]
    WHERE [object_id] = OBJECT_ID('[dbo].[temp]')
    ORDER BY [column_id]

    ALTER TABLE [dbo].[temp]
    ADD [ID] int IDENTITY(0, 1) NOT NULL, [R] int;

    SET @cmd = CONCAT('
        UPDATE [dbo].[temp]
        SET [R] = [T].[R]
        FROM (SELECT ROW_NUMBER() OVER(ORDER BY ', ISNULL(@order_by, (SELECT TOP(1) [col] FROM #columns)), ') - 1 AS [R], [ID] FROM [dbo].[temp]) AS [T]
        WHERE [dbo].[temp].[ID] = [T].[ID]
    ')
    EXEC(@cmd)

    ALTER TABLE [dbo].[temp]
    DROP COLUMN [ID];

    DECLARE @c int,
        @col nvarchar(max),
        @numeric bit,
        @min float,
        @max float,
        @R int = 0,
        @value_float float,
        @value_char varchar(max),
        @background char(7),
        @fontcolour char(7),
		@text_align varchar(6)
    DECLARE row_cursor cursor DYNAMIC SCROLL FOR
        SELECT [c], [col], [numeric], [min], [max]
        FROM #columns WITH (NOLOCK)

    OPEN row_cursor

    FETCH FIRST FROM row_cursor
    INTO @c, @col, @numeric, @min, @max

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @numeric = 1
        BEGIN
            SET @cmd = CONCAT('
                UPDATE #columns
                SET [min] = CONVERT(float, (SELECT MIN([', @col, ']) FROM [dbo].[temp])),
                    [max] = CONVERT(float, (SELECT MAX([', @col, ']) FROM [dbo].[temp]))
                WHERE [col] = ''', @col, '''
            ')
            EXEC(@cmd)
        END

        FETCH NEXT FROM row_cursor
        INTO @c, @col, @numeric, @min, @max
    END

    DECLARE @separator char(70) = CONCAT('</th>', CHAR(10), CHAR(9), CHAR(9), '<th style="background-color:', @main, ';border:2px solid ', @black, '">')
	DECLARE @html varchar(max) = ''
    SET @html += '<table style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid' + @black + ';border-collapse:collapse">' + CHAR(10)
    SET @html += (SELECT 
        CONCAT(
            CHAR(9), '<tr style="color:' + @white + '">', CHAR(10), 
            CHAR(9), CHAR(9), '<th style="background-color:' + @main + ';border:2px solid ' + @black + '">', STRING_AGG([col], @separator), '</th>', CHAR(10), 
            CHAR(9), '</tr>', CHAR(10)
        ) 
        FROM #columns
    )

	WHILE @R <= (SELECT MAX([R]) FROM [dbo].[temp])
    BEGIN
	    FETCH FIRST FROM row_cursor
        INTO @c, @col, @numeric, @min, @max

        SET @html += CONCAT(CHAR(9), '<tr>', CHAR(10))

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @cmd = CONCAT('
                SELECT @value_char = REPLACE(CONVERT(varchar(max), [', @col, '], 21), CHAR(10), ''''),
                    @value_float = ', IIF(@numeric = 1, CONCAT('CONVERT(float, [', @col, '])'), 'NULL'), '
                FROM [dbo].[temp]
                WHERE [R] = ', @R, '
            ')
            EXEC sp_executesql @cmd, N'@value_char varchar(max) OUTPUT, @value_float float OUTPUT', @value_char OUTPUT, @value_float OUTPUT

            IF @c = 1
                SET @html += CONCAT(CHAR(9), CHAR(9), '<td style="border:2px solid ', @black,';background-color:', @dark_accent, ';color:', @white, '">', @value_char, '</td>', CHAR(10))
            ELSE
            BEGIN
				IF @numeric = 1
					SET @text_align = 'right'
				ELSE
					SET @text_align = 'center'
                IF @numeric = 1 AND @min < 0
                BEGIN
                    IF @value_float < 0
                        SET @background = [dbo].[gradient_hex](@value_float, @min, 0, @negative, @null)
                    ELSE
                        SET @background = [dbo].[gradient_hex](@value_float, @max, 0, @positive, @null)
                    SET @fontcolour = @white
                END
                ELSE
                BEGIN
                    IF @numeric = 1
                        SET @background = [dbo].[gradient_hex](@value_float, @min, @max, @white, @light_accent)
                    ELSE IF @R % 2 = 0
                        SET @background = @white
                    ELSE
                        SET @background = @grey
                    SET @fontcolour = @black
                END
                SET @html += CONCAT(CHAR(9), CHAR(9), '<td style="border:2px solid ', @black,';background-color:', @background, ';color:', @fontcolour, ';text-align:', @text_align, '">', @value_char, '</td>', CHAR(10))
            END

            FETCH NEXT FROM row_cursor
            INTO @c, @col, @numeric, @min, @max
        END

        SET @html += CONCAT(CHAR(9), '</tr>', CHAR(10))
        SET @R += 1
    END

    SET @html += '</table>'

    CLOSE row_cursor
    DEALLOCATE row_cursor

    DROP TABLE IF EXISTS [dbo].[temp]
    DROP TABLE IF EXISTS #columns

	SET @out = @html

	RETURN;
END
GO