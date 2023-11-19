SET NOCOUNT ON

DECLARE @sql nvarchar(max), 
	@R int, 
	@O int,
	@type varchar(4),
	@description varchar(64), 
	@schema varchar(128),
	@table varchar(128),
	@name varchar(128), 
	@definition nvarchar(max)

DECLARE @types table (
	[type] varchar(4),
	[description] varchar(64),
	[order] tinyint
)
INSERT INTO @types
VALUES ('SC', 'Schemata', 1),
	('U', 'Tables', 2),
	('D', 'Default Constraints', 3),
	('C', 'Check Constraints', 4),
	('PK', 'Primary Keys', 5),
	('UQ', 'Unique Constraints', 6),
	('F', 'Foreign Keys', 7),
	('TR', 'Triggers', 8),
	('P', 'Stored Procedures', 9),
	('FN', 'Scalar Functions', 10),
	('TF', 'Table-Valued Functions', 11),
	('V', 'Views', 12)

DECLARE @definitions table (
	[type] varchar(4),
	[schema] varchar(128),
	[table] varchar(128),
	[name] varchar(128),
	[definition] nvarchar(max)
)

--============================================================--
/* Schemata */

INSERT INTO @definitions
SELECT 'SC' AS [type],
	[name] AS [schema],
	NULL AS [table],
	NULL AS [name],
	CONCAT('IF SCHEMA_ID(''', [name], ''') IS NULL', CHAR(10), CHAR(9), 'EXEC(''CREATE SCHEMA [', [name], ']'')') AS [definition]
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

--============================================================--
/* Tables */

INSERT INTO @definitions
SELECT [t].[type], 
	SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	NULL AS [name],
	CONCAT('CREATE TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], '] (',
	STUFF((
		SELECT CONCAT(',', CHAR(10), CHAR(9), 
			'[', [c].[name] + '] ', 
			[d].[name], 
			CASE WHEN [d].[system_type_id] IN (106, 108) THEN CONCAT('(', [d].[precision], ', ', [d].[scale], ')')
				WHEN [d].[system_type_id] IN (165, 167, 173, 175) THEN CONCAT('(', IIF([c].[max_length] = -1, 'max', CONVERT(varchar, [c].[max_length])), ')') 
				WHEN [d].[system_type_id] IN (231, 239) THEN CONCAT('(', IIF([c].[max_length] = -1, 'max', CONVERT(varchar, [c].[max_length]/2)), ')') 
				ELSE '' END,
			IIF([c].[is_nullable] = 1, ' NULL', ' NOT NULL')
		)
		FROM sys.columns AS [c]
			INNER JOIN sys.types AS [d]
				ON [d].[system_type_id] = [c].[system_type_id]
				AND [d].[user_type_id] = [c].[user_type_id]
		WHERE [c].[object_id] = [t].[object_id]
		FOR XML PATH(''), TYPE
	).value('./text()[1]', 'nvarchar(max)'), 1, 1, ''),
	CHAR(10), ')') AS [definition]
FROM sys.tables AS [t]
WHERE [is_ms_shipped] = 0

--============================================================--
/* Default Constraints */

INSERT INTO @definitions
SELECT [dc].[type],
	SCHEMA_NAME([dc].[schema_id]) AS [schema],
	OBJECT_NAME([dc].[parent_object_id]) AS [table],
	[dc].[name],
	CONCAT('ALTER TABLE [', SCHEMA_NAME([dc].[schema_id]), '].[', OBJECT_NAME([dc].[parent_object_id]), ']', 
		CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [dc].[name],
		CHAR(10), CHAR(9), 'DEFAULT ', [dc].[definition], ' FOR [', [c].[name], ']') AS [definition]
FROM sys.default_constraints AS [dc]
	INNER JOIN sys.columns AS [c]
		ON [c].[object_id] = [dc].[parent_object_id]
		AND [c].[column_id] = [dc].[parent_column_id]
WHERE [dc].[is_ms_shipped] = 0

--============================================================--
/* Check Constaints */

INSERT INTO @definitions
SELECT [cc].[type],
	SCHEMA_NAME([cc].[schema_id]) AS [schema],
	OBJECT_NAME([cc].[parent_object_id]) AS [table],
	[cc].[name],
	CONCAT('ALTER TABLE [', SCHEMA_NAME([cc].[schema_id]), '].[', OBJECT_NAME([cc].[parent_object_id]), ']', 
		CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [cc].[name],
		CHAR(10), CHAR(9), 'CHECK ', [cc].[definition]) AS [definition]
FROM sys.check_constraints AS [cc]
	INNER JOIN sys.columns AS [c]
		ON [c].[object_id] = [cc].[parent_object_id]
		AND [c].[column_id] = [cc].[parent_column_id]
WHERE [cc].[is_ms_shipped] = 0

--============================================================--
/* Primary Keys and Unique Constraints */

INSERT INTO @definitions
SELECT IIF([i].[is_primary_key] = 1, 'PK', 'UQ') AS [type],
	SCHEMA_NAME([schema_id]) AS [schema],
	[t].[name] AS [table],
	[i].[name],
	CONCAT('ALTER TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], ']',
		CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [i].[name],
		CHAR(10), CHAR(9), IIF([i].[is_primary_key] = 1, 'PRIMARY KEY ', 'UNIQUE '), [i].[type_desc] COLLATE database_default, '(', 
		STUFF((
			SELECT CONCAT(', [', [c].[name], ']')
			FROM sys.index_columns AS [ic]
				INNER JOIN sys.columns AS [c]
					ON [c].[object_id] = [ic].[object_id]
					AND [c].[column_id] = [ic].[column_id]
			WHERE [ic].[object_id] = [i].[object_id]
				AND [ic].[index_id] = [i].[index_id]
			FOR XML PATH(''), TYPE
		).value('./text()[1]', 'nvarchar(max)'), 1, 2, ''), 
		')') AS [definition]
FROM sys.indexes AS [i]
	INNER JOIN sys.tables AS [t]
		ON [t].[object_id] = [i].[object_id]
WHERE [i].[is_primary_key] = 1
	OR [i].[is_unique_constraint] = 1

--============================================================--
/* Foreign Keys */

INSERT INTO @definitions
SELECT [fk].[type],
	SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	[fk].[name],
	CONCAT('ALTER TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], ']',
		CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [fk].[name], ' FOREIGN KEY ([', [c].[name], '])',
		CHAR(10), CHAR(9), 'REFERENCES [', SCHEMA_NAME([s].[schema_id]), '].[', [s].[name], '] ([', [b].[name], '])',
		CHAR(10), CHAR(9), 'ON DELETE ', REPLACE([fk].[delete_referential_action_desc], '_', ' ') COLLATE database_default,
		CHAR(10), CHAR(9), 'ON UPDATE ', REPLACE([fk].[update_referential_action_desc], '_', ' ') COLLATE database_default) AS [definition]
FROM sys.foreign_keys AS [fk]
	INNER JOIN sys.tables AS [t]
		ON [t].[object_id] = [fk].[parent_object_id]
	INNER JOIN sys.foreign_key_columns AS [fkc]
		ON [fkc].[parent_object_id] = [fk].[parent_object_id]
		AND [fkc].[constraint_object_id] = [fk].[object_id]
	INNER JOIN sys.columns AS [c]
		ON [c].[object_id] = [fkc].[parent_object_id]
		AND [c].[column_id] = [fkc].[parent_column_id]
	INNER JOIN sys.tables AS [s]
		ON [s].[object_id] = [fk].[referenced_object_id]
	INNER JOIN sys.foreign_key_columns AS [fkb]
		ON [fkb].[referenced_object_id] = [fk].[referenced_object_id]
		AND [fkb].[constraint_object_id] = [fk].[object_id]
	INNER JOIN sys.columns AS [b]
		ON [b].[object_id] = [fkb].[referenced_object_id]
		AND [b].[column_id] = [fkb].[referenced_column_id]
WHERE [fk].[is_ms_shipped] = 0

--============================================================--
/* Functions, Stored Procedures and Views */

INSERT INTO @definitions
SELECT REPLACE([o].[type], 'IF', 'TF') AS [type],
	SCHEMA_NAME([o].[schema_id]) AS [schema],
	NULL AS [table],
	[o].[name],
	STUFF((
		SELECT [sc].[text]
		FROM sys.syscomments AS [sc]
		WHERE [sc].[id] = [o].[object_id]
		FOR XML PATH(''), TYPE
	).value('./text[1]', 'nvarchar(max)'), 1, 0, '') AS [definition]
FROM sys.objects AS [o]
WHERE [o].[type] IN ('FN', 'IF', 'TF', 'V', 'P')
	AND [o].[is_ms_shipped] = 0

--============================================================--
/* Triggers */

INSERT INTO @definitions
SELECT [tr].[type],
	SCHEMA_NAME([o].[schema_id]) AS [schema],
	[o].[name] AS [table],
	[tr].[name],
	STUFF((
		SELECT [sc].[text]
		FROM sys.syscomments AS [sc]
		WHERE [sc].[id] = [tr].[object_id]
		FOR XML PATH(''), TYPE
	).value('./text[1]', 'nvarchar(max)'), 1, 0, '') AS [definition]
FROM sys.triggers AS [tr]
	INNER JOIN sys.objects AS [o]
		ON [o].[object_id] = [tr].[parent_id]
WHERE [tr].[is_ms_shipped] = 0

--============================================================--
/* Indexes */

INSERT INTO @definitions
SELECT 'I' AS [type],
	SCHEMA_NAME([o].[schema_id]) AS [schema],
	[o].[name] AS [table],
	[i].[name],
	CONCAT('CREATE ', IIF([i].[is_unique] = 1, 'UNIQUE ', ''), [i].[type_desc] COLLATE database_default, 
		CHAR(10), CHAR(9), 'INDEX ', [i].[name],
		CHAR(10), CHAR(9), 'ON (',
		STUFF((
			SELECT CONCAT(', [', [c].[name], ']')
			FROM sys.index_columns AS [ic]
				INNER JOIN sys.columns AS [c]
					ON [c].[object_id] = [ic].[object_id]
					AND [c].[column_id] = [ic].[column_id]
			WHERE [ic].[object_id] = [i].[object_id]
				AND [ic].[index_id] = [i].[index_id]
			FOR XML PATH(''), TYPE
		).value('./type[1]', 'nvarchar(max)'), 1, 2, ''),
		')') AS [definition]
FROM sys.indexes AS [i]
	INNER JOIN sys.objects AS [o]
		ON [o].[object_id] = [i].[object_id]
WHERE [i].[is_primary_key] = 0
	AND [i].[is_unique_constraint] = 0
	AND [i].[index_id] > 0
	AND [o].[type] IN ('U', 'V')

--============================================================--
/* Definitions Cursor */

SET @sql = CONCAT('CREATE PROCEDURE [usp_drop_and_create_', DB_NAME(), ']', CHAR(10), 'AS')

DECLARE definitions_cursor cursor STATIC SCROLL FOR
SELECT ROW_NUMBER() OVER(PARTITION BY [defs].[type] ORDER BY [schema], [table], [name]) AS [R],
	ROW_NUMBER() OVER(PARTITION BY [defs].[type], [schema], [table] ORDER BY [name]) AS [O],
	[types].[type],
	[types].[description],
	[defs].[schema],
	[defs].[table],
	[defs].[name],
	[defs].[definition]
FROM @definitions AS [defs]
	INNER JOIN @types AS [types]
		ON [types].[type] = [defs].[type]
ORDER BY [types].[order], [R], [O]

OPEN definitions_cursor

FETCH FIRST FROM definitions_cursor
INTO @R, @O, @type, @description, @schema, @table, @name, @definition

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @type = 'F'
	BEGIN
		IF @R = 1
			SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* Dropping ', @description, ' */', CHAR(10))
		IF @O = 1 and @name IS NOT NULL AND @table IS NOT NULL
			SET @sql += CONCAT(CHAR(10), '/* [', @schema, '].[', @table, '] */')
		SET @sql += CONCAT(CHAR(10), '-- ', COALESCE(@name, @table, @schema), CHAR(10), 'IF (OBJECT_ID(''', @name, ''', ''F'') IS NOT NULL)',
			CHAR(10), CHAR(9), 'ALTER TABLE [', @schema, '].[', @table, '] DROP CONSTRAINT ', @name,
			CHAR(10), 'GO', CHAR(10))
	END
	ELSE IF @type IN ('FN', 'TF', 'V')
	BEGIN
		IF @R = 1
			SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* Dropping ', @description, ' */', CHAR(10))
		SET @sql += CONCAT('DROP ', IIF(@type = 'V', 'VIEW', 'FUNCTION'), ' IF EXISTS [', @schema, '].[', @name, '] GO;', CHAR(10))
	END

	FETCH NEXT FROM definitions_cursor
	INTO @R, @O, @type, @description, @schema, @table, @name, @definition
END

FETCH FIRST FROM definitions_cursor
INTO @R, @O, @type, @description, @schema, @table, @name, @definition

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @type = 'U'
	BEGIN
		IF @R = 1
			SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* Dropping ', @description, ' */', CHAR(10))
		SET @sql += CONCAT('DROP TABLE IF EXISTS [', @schema, '].[', @table, '] GO;', CHAR(10))
	END

	FETCH NEXT FROM definitions_cursor
	INTO @R, @O, @type, @description, @schema, @table, @name, @definition
END

FETCH FIRST FROM definitions_cursor
INTO @R, @O, @type, @description, @schema, @table, @name, @definition

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @R = 1
		SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* ', @description, ' */', CHAR(10))
	IF @O = 1 and @name IS NOT NULL AND @table IS NOT NULL
		SET @sql += CONCAT(CHAR(10), '/* [', @schema, '].[', @table, '] */')
	SET @sql += CONCAT(CHAR(10), '-- ', COALESCE(@name, @table, @schema), CHAR(10), @definition, CHAR(10), 'GO', CHAR(10))

	FETCH NEXT FROM definitions_cursor
	INTO @R, @O, @type, @description, @schema, @table, @name, @definition
END

CLOSE definitions_cursor
DEALLOCATE definitions_cursor

EXECUTE [dbo].[print] @sql