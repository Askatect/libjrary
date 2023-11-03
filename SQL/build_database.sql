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
      @schema nvarchar(max)

SET @sql = CONCAT('
CREATE OR ALTER PROCEDURE [dbo].[build_', DB_NAME(), ']
AS
DECLARE @cmd nvarchar(max) = ''''
')

--============================================================--
/* Schemata */

SET @sql += '
IF @print = 1
	PRINT(''Building schemata...'')
BEGIN TRY
'

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
IF (SCHEMA_ID([', @schema, ']) IS NULL)
BEGIN
	IF @print = 1
		PRINT(''CREATE SCHEMA [', @schema, ']'')
	EXEC(''CREATE SCHEMA [', @schema, ']'')
END
      ')

      FETCH NEXT FROM schemata_cursor
      INTO @schema
END

CLOSE schemata_cursor
DEALLOCATE schemata_cursor

SET @sql += '
END TRY
BEGIN CATCH
	PRINT(''Error creating schemata.'')
	PRINT(CONCAT(ERROR_NUMBER(), '': '', ERROR_MESSAGE())
END CATCH
'

--============================================================--
/* Tables */

SET @sql += '
IF @print = 1
	PRINT(''Building tables...'')
'

DECLARE @table nvarchar(max),
	@cols nvarchar(max)

DECLARE tables_cursor cursor FAST_FORWARD FOR
SELECT SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	(SELECT CONCAT('[', [c].[name], '] ', [d].[name], CASE WHEN [d].[system_type_id] IN (106, 108) THEN CONCAT('(', [d].[precision], ', ', [d].[scale], ')') WHEN [d].[system_type_id] IN (165, 167, 173, 175, 231, 239) THEN CONCAT('(', IIF([d].[max_length] = -1, 'max', [d].[max_length]), ')') ELSE '' END, IIF([c].[is_nullable] = 1, ' NULL', ' NOT NULL'), ', ', CHAR(10), CHAR(9))
	FROM sys.columns AS [c]
		INNER JOIN sys.types AS [d]
			ON [d].[system_type_id] = [c].[system_type_id]
			AND [d].[user_type_id] = [c].[user_type_id]
	WHERE [c].[object_id] = [t].[object_id]
	FOR XML PATH('')) AS [cols]
FROM sys.tables AS [t]
GROUP BY [t].[schema_id],
	[t].[object_id],
	[t].[name]
ORDER BY [t].[name]

OPEN tables_cursor

FETCH NEXT FROM tables_cursor
INTO @schema,
	@table, 
	@cols

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql += CONCAT('
IF @replace = 1
	EXEC(''DROP TABLE IF EXISTS [', @schema, '].[', @table, ']'')
IF (OBJECT_ID(''[', @schema, '].[', @table, ']'') IS NULL)
BEGIN
	SET @cmd = ''
CREATE TABLE [', @schema, '].[', @table, '] (
	', LEFT(@cols, LEN(@cols) - 4), '
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
		@cols
END

CLOSE tables_cursor
DEALLOCATE tables_cursor

--============================================================--
/* Constraints */

SELECT SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	[i].[name] AS [primary_key],
	[i].*
FROM sys.tables AS [t]
	INNER JOIN sys.indexes AS [i]
		ON [i].[object_id] = [t].[object_id]
		AND [i].[is_primary_key] = 1
	INNER JOIN sys.index_columns AS [ic]
		ON [ic].[object_id] = [t].[object_id]
		AND [ic].[index_id] = [i].[index_id]
	INNER JOIN sys.columns AS [c]
		ON [c].[object_id] = [t].[object_id]
		AND [c].[column_id] = [ic].[column_id]

--============================================================--

EXECUTE [dbo].[print] @sql

DROP PROCEDURE IF EXISTS [dbo].[print]