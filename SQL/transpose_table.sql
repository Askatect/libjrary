CREATE OR ALTER PROCEDURE [dbo].[transpose_table] (@query varchar(max),
	@output varchar(max) = '[dbo].[output]'
)
AS
BEGIN
	SET NOCOUNT ON

	DROP TABLE IF EXISTS [dbo].[temp]
	DROP TABLE IF EXISTS #columns
	DROP TABLE IF EXISTS ##insert

	DECLARE @cmd varchar(max)
	SET @cmd = CONCAT('
		SELECT *
		INTO [dbo].[temp]
		FROM (', @query, ') AS [T]
	')
	--PRINT(@cmd)
	EXEC(@cmd)

	DECLARE @c int, @col varchar(max)
	DECLARE cols_cursor SCROLL cursor FOR
	SELECT [column_id] AS [c],
		[name] AS [col]
	FROM sys.columns
	WHERE [object_id] = OBJECT_ID('[dbo].[temp]')
	ORDER BY [column_id]

	OPEN cols_cursor

	FETCH FIRST FROM cols_cursor
	INTO @c, @col

	SET @cmd = CONCAT('
		SELECT ', @c, ' AS [c], CONCAT(''(['', STRING_AGG(ISNULL(CONVERT(varchar, [', @col, '], 21), ''NULL''), ''] varchar(max), [''), ''] varchar(max))'') AS [values]
		INTO ##insert
		FROM [dbo].[temp]
	')

	WHILE @@FETCH_STATUS = 0
	BEGIN
		FETCH NEXT FROM cols_cursor
		INTO @c, @col

		SET @cmd += CONCAT('
			UNION
			SELECT ', @c, ', CONCAT(''('''''', STRING_AGG(ISNULL(CONVERT(varchar, [', @col, '], 21), ''NULL''), '''''', ''''''), '''''')'')
			FROM [dbo].[temp]
		')
	END
	--PRINT(@cmd)
	EXEC(@cmd)

	SET @cmd = CONCAT('
		CREATE TABLE ', @output, '
		', (SELECT [values] FROM ##insert WHERE [c] = 1)
	)
	--PRINT(@cmd)
	EXEC(@cmd)

	SET @cmd = CONCAT('
		INSERT INTO ', @output, '
		--OUTPUT inserted.*
		VALUES ', (SELECT STRING_AGG([values], ', ' + CHAR(10)) FROM ##insert WHERE [c] > 1)
	)
	--PRINT(@cmd)
	EXEC(@cmd)

	CLOSE cols_cursor
	DEALLOCATE cols_cursor

	DROP TABLE IF EXISTS [dbo].[temp]
	DROP TABLE IF EXISTS #columns
	DROP TABLE IF EXISTS ##insert
END
GO