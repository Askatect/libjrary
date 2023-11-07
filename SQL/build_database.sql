SET NOCOUNT ON
GO

CREATE OR ALTER PROCEDURE [dbo].[print] (@string nvarchar(max))
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

DECLARE @sql nvarchar(max) = '',
	@schema nvarchar(max),
	@table nvarchar(max),
	@definition nvarchar(max),
	@detail nvarchar(max)

SET @sql = CONCAT('
CREATE OR ALTER PROCEDURE [dbo].[build_', LOWER(DB_NAME()), '] (
	@print bit = 1,
	@replace bit = 0
)
AS
BEGIN
DECLARE @cmd nvarchar(max) = ''''
')

--============================================================--
/* Schemata */

SET @sql += '
IF @print = 1
	PRINT(''Building schemata...'')

BEGIN TRY'

DECLARE schemata_cursor cursor FAST_FORWARD FOR
SELECT [name] AS [schema]
FROM sys.schemas
WHERE [name] NOT IN (
      'public',
      'guest',
      'INFORMATION_SCHEMA',
      'sys',
      'db_owner',
      'db_accessadmin',
      'db_securityadmin',
      'db_ddladmin',
      'db_backupoperator',
      'db_datareader',
      'db_datawriter',
      'db_denydatareader',
      'db_denydatawriter'
)

OPEN schemata_cursor

FETCH NEXT FROM schemata_cursor
INTO @schema

WHILE @@FETCH_STATUS = 0
BEGIN
      SET @sql += CONCAT('
IF (SCHEMA_ID(''', @schema, ''') IS NULL)
BEGIN
	SET @sql = ''CREATE SCHEMA ', @schema, '''
	IF @print = 1
		PRINT(@sql)
	EXEC(@sql)
END
')

      FETCH NEXT FROM schemata_cursor
      INTO @schema
END

CLOSE schemata_cursor
DEALLOCATE schemata_cursor

SET @sql += 'END TRY
BEGIN CATCH
	PRINT(CONCAT(''Error creating schemata. '', ERROR_NUMBER(), '': '', ERROR_MESSAGE()))
END CATCH
'

--============================================================--
/* Tables, Columns, Primary Keys, Unique Constraints, Check 
Constraints and Default Constraints */

SET @sql += '
IF @print = 1
	PRINT(''Building tables...'')
'

DECLARE tables_cursor cursor FAST_FORWARD FOR
SELECT SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	STUFF(
		(SELECT CONCAT(', ', CHAR(10), CHAR(9), '[', [c].[name], '] ', [d].[name], CASE WHEN [d].[system_type_id] IN (106, 108) THEN CONCAT('(', [d].[precision], ', ', [d].[scale], ')') WHEN [d].[system_type_id] IN (165, 167, 173, 175, 231, 239) THEN CONCAT('(', IIF([c].[max_length] = -1, 'max', CONVERT(varchar, IIF([d].[system_type_id] IN (231, 239), [c].[max_length]/2, [c].[max_length]))), ')') ELSE '' END, IIF([c].[is_nullable] = 1, ' NULL', ' NOT NULL'), IIF([dc].[object_id] IS NOT NULL, CONCAT(' CONSTRAINT ', [dc].[name], ' DEFAULT ', REPLACE([dc].[definition], '''', '''''')), ''), IIF([cc].[is_disabled] = 0, CONCAT(' CONSTRAINT ', [cc].[name], ' CHECK ', REPLACE([cc].[definition], '''', '''''')), ''))
		FROM sys.columns AS [c]
			INNER JOIN sys.types AS [d]
				ON [d].[system_type_id] = [c].[system_type_id]
				AND [d].[user_type_id] = [c].[user_type_id]
			LEFT JOIN sys.default_constraints AS [dc]
				ON [dc].[parent_object_id] = [c].[object_id]
				AND [dc].[parent_column_id] = [c].[column_id]
			LEFT JOIN sys.check_constraints AS [cc]
				ON [cc].[parent_object_id] = [c].[object_id]
				AND [cc].[parent_column_id] = [c].[column_id]
		WHERE [c].[object_id] = [t].[object_id]
		FOR XML PATH('')
	), 1, 4, '') AS [cols]
FROM sys.tables AS [t]
GROUP BY [t].[schema_id],
	[t].[object_id],
	[t].[name]
ORDER BY [t].[name]

OPEN tables_cursor

FETCH NEXT FROM tables_cursor
INTO @schema,
	@table, 
	@definition

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @detail = (
		SELECT CONCAT(', ', CHAR(10), CHAR(9), 'CONSTRAINT ', [constraint], IIF([is_primary_key] = 1, ' PRIMARY KEY ', ' UNIQUE '), IIF([is_primary_key] = 1, [type] COLLATE database_default, ''), '(', [cols], ')')
		FROM (
			SELECT SCHEMA_NAME([t].[schema_id]) AS [schema],
				[t].[name] AS [table],
				STUFF((
					SELECT CONCAT(', [', [c].[name], ']')
					FROM sys.index_columns AS [ic]
						INNER JOIN sys.columns AS [c]
							ON [ic].[object_id] = [c].[object_id]
							AND [ic].[column_id] = [c].[column_id]
					WHERE [ic].[object_id] = [t].[object_id]
						AND [ic].[index_id] = [i].[index_id]
					FOR XML PATH('')
				), 1, 2, '') AS [cols],
				[i].[name] AS [constraint],
				[i].[is_primary_key],
				[i].[is_unique_constraint],
				[i].[type_desc] AS [type]
			FROM sys.tables AS [t]
				INNER JOIN sys.indexes AS [i]
					ON [i].[object_id] = [t].[object_id]
			WHERE ([is_primary_key] = 1
					OR [is_unique_constraint] = 1)
				AND SCHEMA_NAME([t].[schema_id]) = @schema
				AND [t].[name] = @table
		) AS [T]
		FOR XML PATH('')
	)

	SET @sql += CONCAT('
IF @replace = 1
BEGIN
	SET @cmd = ''DROP TABLE IF EXISTS [', @schema, '].[', @table, ']''
	IF @print = 1
		PRINT(@cmd)
	EXEC(@cmd)
END
IF (OBJECT_ID(''[', @schema, '].[', @table, ']'') IS NULL)
BEGIN
	SET @cmd = ''
CREATE TABLE [', @schema, '].[', @table, '] (
	', REPLACE(REPLACE(REPLACE(REPLACE(@definition, '&quot;', '"'), '&amp;', '&'), '&lt;', '<'), '&gt;', '>'), @detail, '
)
''
	IF @print = 1
		PRINT(@cmd)
	EXEC(@cmd)
END
')

	FETCH NEXT FROM tables_cursor
	INTO @schema,
		@table, 
		@definition
END

CLOSE tables_cursor
DEALLOCATE tables_cursor

--============================================================--
/* Indexes */

DECLARE index_cursor cursor FAST_FORWARD FOR
SELECT [schema],
	[table],
	[index],
	CONCAT('CREATE ', IIF([is_unique] = 1, 'UNIQUE ', ''), [type] COLLATE database_default, ' INDEX ', [index], ' ON [', [schema], '].[', [table], '] (', [cols], ')')
FROM (
	SELECT SCHEMA_NAME([t].[schema_id]) AS [schema],
		[t].[name] AS [table],
		STUFF((
			SELECT CONCAT(', [', [c].[name], ']', IIF([ic].[is_descending_key] = 0, ' ASC', ' DESC'))
			FROM sys.index_columns AS [ic]
				INNER JOIN sys.columns AS [c]
					ON [ic].[object_id] = [c].[object_id]
					AND [ic].[column_id] = [c].[column_id]
			WHERE [ic].[object_id] = [t].[object_id]
				AND [ic].[index_id] = [i].[index_id]
			FOR XML PATH('')
		), 1, 2, '') AS [cols],
		[i].[name] AS [index],
		[i].[is_unique],
		[i].[type_desc] AS [type]
	FROM sys.tables AS [t]
		INNER JOIN sys.indexes AS [i]
			ON [i].[object_id] = [t].[object_id]
	WHERE [i].[is_primary_key] = 0
		AND [i].[is_unique_constraint] = 0
		AND [i].[index_id] > 0
) AS [T]

OPEN index_cursor

FETCH NEXT FROM index_cursor
INTO @schema,
	@table,
	@detail,
	@definition

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql += CONCAT('
IF @replace = 1
BEGIN
	SET @cmd = ''DROP INDEX IF EXISTS ', @detail, ' ON [', @schema, '].[', @table, ']''
	IF @print = 1
		PRINT(@cmd)
	EXEC(@cmd)
END
SET @cmd = ''', @definition, '''
IF @print = 1
	PRINT(@cmd)
EXEC(@cmd)
')
	
	FETCH NEXT FROM index_cursor
	INTO @schema,
		@table,
		@detail,
		@definition
END

CLOSE index_cursor
DEALLOCATE index_cursor

--============================================================--
/* Foreign Keys */

--============================================================--
/* Triggers */

--============================================================--
/* Views */

--============================================================--

SET @sql += 'END'

--============================================================--

EXECUTE [dbo].[print] @sql

DROP PROCEDURE IF EXISTS [dbo].[print]
