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

CREATE OR ALTER PROCEDURE [dbo].[database_builder_builder] (
	@print bit = 1,
	@exec bit = 0
)
AS

IF @print = 0
	SET NOCOUNT ON

DECLARE @sql nvarchar(max) = '',
	@schema nvarchar(max),
	@table nvarchar(max),
	@definition nvarchar(max),
	@detail nvarchar(max)

SET @sql = CONCAT('
CREATE OR ALTER PROCEDURE [dbo].[build_', LOWER(DB_NAME()), '] (
	@print bit = 1,
	@replace bit = 0,
	@exec bit = 0
)
AS
BEGIN
DECLARE @cmd nvarchar(max) = ''''
')

--============================================================--
/* Schemata */

SET @sql += '
--============================================================--
/* Schemata */

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
	SET @cmd = ''CREATE SCHEMA ', @schema, '''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
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
--============================================================--
/* Tables, Columns, Primary Keys, Unique Constraints, Check 
Constraints and Default Constraints */
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
	IF @exec = 1
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
	IF @exec = 1
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
/* Foreign Keys */

SET @sql += '
--============================================================--
/* Foreign Keys */
'

DECLARE foreignkey_cursor cursor FAST_FORWARD FOR
SELECT [T].[schema],
	[T].[table],
	[T].[foreign_key],
	CONCAT('ALTER TABLE [', [T].[schema], '].[', [T].[table], '] ADD CONSTRAINT ', [T].[foreign_key], ' REFERENCES (',  [T].[columns], ') REFERENCES [', [T].[schema_ref], '].[', [T].[table_ref], '](', [T].[columns_ref], ')')
FROM (
	SELECT [fk].[name] AS [foreign_key],
		SCHEMA_NAME([t].[schema_id]) AS [schema],
		[t].[name] AS [table],
		STUFF((
			SELECT CONCAT(', [', [c].[name], ']')
			FROM sys.foreign_key_columns AS [fkc]
				INNER JOIN sys.columns AS [c]
					ON [c].[object_id] = [fkc].[parent_object_id]
					AND [c].[column_id] = [fkc].[parent_column_id]
			WHERE [fkc].[parent_object_id] = [t].[object_id]
				AND [fkc].[constraint_object_id] = [fk].[object_id]
			FOR XML PATH('')
		), 1, 2, '') AS [columns],
		SCHEMA_NAME([s].[schema_id]) AS [schema_ref],
		[s].[name] AS [table_ref],
		STUFF((
			SELECT CONCAT(', [', [c].[name], ']')
			FROM sys.foreign_key_columns AS [fkc]
				INNER JOIN sys.columns AS [c]
					ON [c].[object_id] = [fkc].[referenced_object_id]
					AND [c].[column_id] = [fkc].[referenced_column_id]
			WHERE [fkc].[parent_object_id] = [t].[object_id]
				AND [fkc].[constraint_object_id] = [fk].[object_id]
			FOR XML PATH('')
		), 1, 2, '') AS [columns_ref]
	FROM sys.tables AS [t]
		INNER JOIN sys.foreign_keys AS [fk]
			ON [fk].[parent_object_id] = [t].[object_id]
		INNER JOIN sys.tables AS [s]
			ON [fk].[referenced_object_id] = [s].[object_id]
) AS [T]

OPEN foreignkey_cursor

FETCH NEXT FROM foreignkey_cursor
INTO @schema,
	@table,
	@detail,
	@definition

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql += CONCAT('
IF @replace = 1
BEGIN
	SET @cmd = ''DROP CONSTRAINT IF EXISTS ', @detail, '''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
END
IF (OBJECT_ID(''', @detail, ''') IS NULL)
BEGIN
	SET @cmd = ''', @definition, '''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
END
')
	
	FETCH NEXT FROM foreignkey_cursor
	INTO @schema,
		@table, 
		@detail,
		@definition
END

CLOSE foreignkey_cursor
DEALLOCATE foreignkey_cursor

--============================================================--
/* Views and Triggers */

SET @sql += '
--============================================================--
/* Views and Triggers */
'

DECLARE view_trigger_cursor cursor FAST_FORWARD FOR
SELECT SCHEMA_NAME([o].[schema_id]) AS [schema],
	[o].[name], 
	[type],
	[sc].[text]
FROM sys.objects AS [o]
	INNER JOIN sys.syscomments AS [sc]
		ON [sc].[id] = [o].[object_id]
WHERE [o].[type] IN ('TR', 'V')

OPEN view_trigger_cursor

FETCH NEXT FROM view_trigger_cursor
INTO @schema,
	@table,
	@detail,
	@definition

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql += CONCAT('
IF @replace = 1
BEGIN
	SET @cmd = ''DROP ', IIF(@detail = 'V', 'VIEW', 'TRIGGER'), ' IF EXISTS [', @schema, '].[', @table, ']''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
END
IF (OBJECT_ID(''[', @schema, '].[', @table, ']'', ''', @detail, ''') IS NULL)
BEGIN
	SET @cmd = ''', REPLACE(@definition, '''', ''''''), '''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
END
')

	FETCH NEXT FROM view_trigger_cursor
	INTO @schema, 
		@table,
		@detail,
		@definition
END

CLOSE view_trigger_cursor
DEALLOCATE view_trigger_cursor

--============================================================--
/* Indexes */

SET @sql += '
--============================================================--
/* Indexes */
'

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
	FROM sys.objects AS [t]
		INNER JOIN sys.indexes AS [i]
			ON [i].[object_id] = [t].[object_id]
	WHERE [t].[type] IN ('U', 'V')
		AND [i].[is_primary_key] = 0
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
	IF @exec = 1
		EXEC(@cmd)
END
IF EXISTS (
	SELECT 1 
	FROM sys.indexes AS [i] 
		INNER JOIN sys.objects AS [t] 
			ON [t].[object_id] = [i].[parent_object_id] 
	WHERE SCHEMA_NAME([schema_id]) = ''', @schema, ''' 
		AND [t].[name] = ''', @table, '''
		AND [i].[name] = ''', @detail, '''
)
BEGIN
	SET @cmd = ''', @definition, '''
	IF @print = 1
		PRINT(@cmd)
	IF @exec = 1
		EXEC(@cmd)
END
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

SET @sql += CHAR(10) + 'END'

IF @print = 1
BEGIN
	IF LEN(@sql) >= 4000
	BEGIN
		BEGIN TRY
			EXECUTE [dbo].[print] @sql
		END TRY
		BEGIN CATCH
			PRINT('Command is too large to print.')
		END CATCH
	END
	ELSE
		PRINT(@sql)
END

IF @exec = 1
BEGIN
	EXEC(@sql)
END
GO

--DROP PROCEDURE IF EXISTS [dbo].[print]

EXEC [dbo].[database_builder_builder] 1, 1
