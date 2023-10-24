CREATE OR ALTER FUNCTION [dbo].[select_to_html] (
    @query varchar(max),
    @order_by varchar(max) = NULL
)
RETURNS varchar(max)
AS
BEGIN
    DECLARE @black char(7) = '#000000',
        @white char(7) = '#ffffff'
        @dark_accent char(7) = '#541b8c'
    
    DECLARE @cmd varchar(max) = CONCAT('
        SELECT ROW_NUMBER() OVER(', ISNULL(@order_by, 'ORDER BY 0'), ') AS [R], 
            * 
        INTO [dbo].[temp] 
        FROM (', @query, ')')
    EXEC(@cmd)
    SET @cmd = ''

    SELECT [column_id],
        [name],
        [system_type_id]
    INTO #columns
    FROM [sys].[columns]
    WHERE [object_id] = OBJECT_ID('[dbo].[temp]')
    ORDER BY [column_id]

    DECLARE @html varchar(max) = ''
    SET @html += '<table style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid ' + @black + ';border-collapse:collapse">' + CHAR(10)
    SET @html += CONCAT(CHAR(9), '<tr style="color:', @white, '">', CHAR(10), CHAR(9), CHAR(9), '<th style="background-color:', @dark_accent, ';border:2px solid ', @black, '">', (SELECT [name] FROM #columns WHERE [column_id] = 1), '</th>', CHAR(10)

    DROP TABLE IF EXISTS [dbo].[temp]