CREATE OR ALTER PROCEDURE [jra].[usp_build_db_creator] (
--DECLARE
	@replace bit = 0,
	@data bit = 0,
	@schemata varchar(max) = NULL,
	@tables bit = 1,
	@default_constraints bit = 1,
	@check_constraints bit = 1,
	@primary_keys bit = 1,
	@unique_constraints bit = 1,
	@foreign_keys bit = 1,
	@triggers bit = 1,
	@stored_procedures bit = 1,
	@scalar_functions bit = 1,
	@table_valued_functions bit = 1,
	@views bit = 1,
	@indexes bit = 1,
	@print bit = 1,
	@display bit = 1
)
AS

SET NOCOUNT ON

DECLARE @cmd nvarchar(max), 
	@sql nvarchar(max), 
	@R int, 
	@O int,
	@type varchar(4),
	@description varchar(128), 
	@schema varchar(128),
	@table varchar(128),
	@name varchar(128), 
	@modified datetime2,
	@definition nvarchar(max)

DECLARE @types table (
	[type] varchar(4),
	[description] varchar(64),
	[order] tinyint,
	[include] bit
)
INSERT INTO @types
VALUES ('SC', 'Schemata', 1, 1),
	('U', 'Tables', 2, @tables),
	('DT', 'Data', 3, @data),
	('D', 'Default Constraints', 4, @default_constraints),
	('C', 'Check Constraints', 5, @check_constraints),
	('PK', 'Primary Keys', 6, @primary_keys),
	('UQ', 'Unique Constraints', 7, @unique_constraints),
	('F', 'Foreign Keys', 8, @foreign_keys),
	('TR', 'Triggers', 9, @triggers),
	('P', 'Stored Procedures', 10, @stored_procedures),
	('FN', 'Scalar Functions', 11, @scalar_functions),
	('TF', 'Table-Valued Functions', 12, @table_valued_functions),
	('V', 'Views', 13, @views),
	('I', 'Indexes', 14, @indexes)

DELETE FROM @types
WHERE [include] = 0

DECLARE @definitions table (
	[type] varchar(4),
	[schema] varchar(128),
	[table] varchar(128),
	[name] varchar(128),
	[object_id] int,
	[definition] nvarchar(max)
)

--============================================================--
/* Schemata */

IF @schemata IS NULL
	SET @schemata = (
		SELECT STRING_AGG([name], ',') 
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
	)

DECLARE @schemata_array table (
	[schema_id] int,
	[schema] varchar(128)
)
INSERT INTO @schemata_array([schema])
SELECT DISTINCT [value] FROM STRING_SPLIT(@schemata + ',jra', ',')

UPDATE @schemata_array
SET [schema_id] = [s].[schema_id]
FROM sys.schemas AS [s]
WHERE [schema] = [s].[name]

INSERT INTO @definitions
SELECT 'SC' AS [type],
	[s].[schema] AS [schema],
	NULL AS [table],
	NULL AS [name],
	NULL AS [object_id],
	CONCAT('IF SCHEMA_ID(''', [s].[schema], ''') IS NULL', CHAR(10), CHAR(9), 'EXEC(''CREATE SCHEMA [', [s].[schema], ']'')') AS [definition]
FROM @schemata_array AS [s]

--============================================================--
/* Tables */

IF @tables = 1
BEGIN
	INSERT INTO @definitions
	SELECT [t].[type], 
		SCHEMA_NAME([t].[schema_id]) AS [schema],
		[t].[name] AS [table],
		NULL AS [name],
		[t].[object_id],
		CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], ']'', ''U'') IS NULL)', 
		CHAR(10), 'BEGIN', 
		CHAR(10), 'CREATE TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], '] (',
		STUFF((
			SELECT CONCAT(',', CHAR(10), CHAR(9), 
				'[', [c].[name] + '] ', 
				[d].[name], 
				CASE WHEN [d].[system_type_id] IN (106, 108) THEN CONCAT('(', [d].[precision], ', ', [d].[scale], ')')
					WHEN [d].[system_type_id] IN (165, 167, 173, 175) THEN CONCAT('(', IIF([c].[max_length] = -1, 'max', CONVERT(varchar, [c].[max_length])), ')') 
					WHEN [d].[system_type_id] IN (231, 239) THEN CONCAT('(', IIF([c].[max_length] = -1, 'max', CONVERT(varchar, [c].[max_length]/2)), ')') 
					ELSE '' END,
				IIF([c].[is_nullable] = 1, ' NULL', ' NOT NULL'),
				IIF([ic].[is_identity] = 1, CONCAT(' IDENTITY(', CONVERT(int, [ic].[last_value]) + 1, ', ', CONVERT(int, [ic].[increment_value]), ')'), '')
			)
			FROM sys.columns AS [c]
				INNER JOIN sys.types AS [d]
					ON [d].[system_type_id] = [c].[system_type_id]
					AND [d].[user_type_id] = [c].[user_type_id]
				LEFT JOIN sys.identity_columns AS [ic]
					ON [ic].[object_id] = [c].[object_id]
					AND [ic].[column_id] = [c].[column_id]
			WHERE [c].[object_id] = [t].[object_id]
			FOR XML PATH(''), TYPE
		).value('./text()[1]', 'nvarchar(max)'), 1, 1, ''),
		CHAR(10), ')',
		CHAR(10), 'END') AS [definition]
	FROM sys.tables AS [t]
	WHERE [t].[is_ms_shipped] = 0
		AND [t].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
END

--============================================================--
/* Data */

IF @data = 1
BEGIN
	DECLARE data_cursor cursor FAST_FORWARD FOR
	SELECT [defs].[schema],
		[defs].[table],
		STUFF((
			SELECT CONCAT(' + '', '',', CHAR(10), 'IIF([', [c].[name], '] IS NULL, ''NULL'', CONCAT('''''''', REPLACE([', [c].[name], '], '''''''', ''''''''''''), ''''''''))')
			FROM sys.columns AS [c]
			WHERE [c].[object_id] = OBJECT_ID(CONCAT('[', [defs].[schema], '].[', [defs].[table], ']'), 'U')
			FOR XML PATH('')
		), 1, 9, '') AS [definition]
	FROM @definitions AS [defs]
		
	WHERE [defs].[type] = 'U'

	OPEN data_cursor

	FETCH NEXT FROM data_cursor
	INTO @schema, @table, @definition

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @cmd = CONCAT('SELECT @R = COUNT(*) FROM [', @schema, '].[', @table, ']')
		EXECUTE sp_executesql @cmd, N'@R int OUTPUT', @R OUTPUT
		IF @R = 0
		BEGIN
			INSERT INTO @definitions
			VALUES ('DT', @schema, @table, @table + '_data', OBJECT_ID(CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '['))), '/* Source table is empty. */')
		END
		ELSE IF @R > 1000
		BEGIN
			INSERT INTO @definitions
			VALUES ('DT', @schema, @table, @table + '_data', OBJECT_ID(CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '['))), '/* Source table has more than 1000 rows. */')
		END
		ELSE
		BEGIN
			SET @cmd = CONCAT('
				SELECT @definition = STUFF((
					SELECT CONCAT('','', CHAR(10), CHAR(9), ''('', ', CHAR(10), @definition, ',', CHAR(10), ''')'')
					FROM [', @schema, '].[', @table, ']
					FOR XML PATH(''''), TYPE
				).value(''./text()[1]'', ''nvarchar(max)''), 1, 3, '''')
			')
			EXECUTE sp_executesql @cmd, N'@definition nvarchar(max) OUTPUT', @definition OUTPUT
			SET @definition = CONCAT('INSERT INTO [', @schema, '].[', @table, '](', STUFF((SELECT ', ' + QUOTENAME([c].[name], '[') FROM sys.columns AS [c] WHERE [c].[object_id] = OBJECT_ID(CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '['))) FOR XML PATH('')), 1, 2, ''), ')', CHAR(10), 'VALUES ', @definition)
			IF EXISTS (SELECT 1 FROM sys.identity_columns WHERE [object_id] = OBJECT_ID(CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '['))))
				SET @definition = CONCAT('SET IDENTITY_INSERT ', CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '[')), ' ON', CHAR(10), @definition, CHAR(10), 'SET IDENTITY_INSERT ', CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '[')), ' OFF')
			INSERT INTO @definitions
			VALUES ('DT', @schema, @table, @table + '_data', OBJECT_ID(CONCAT(QUOTENAME(@schema, '['), '.', QUOTENAME(@table, '['))), @definition)
		END		

		FETCH NEXT FROM data_cursor
		INTO @schema, @table, @definition
	END

	CLOSE data_cursor
	DEALLOCATE data_cursor
END

--============================================================--
/* Default Constraints */

IF @default_constraints = 1
BEGIN
	INSERT INTO @definitions
	SELECT [dc].[type],
		SCHEMA_NAME([dc].[schema_id]) AS [schema],
		OBJECT_NAME([dc].[parent_object_id]) AS [table],
		[dc].[name],
		[dc].[object_id],
		CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([dc].[schema_id]), '].[', [dc].[name], ']'', ''D'') IS NULL)', 
			CHAR(10), 'BEGIN', 
			CHAR(10), 'ALTER TABLE [', SCHEMA_NAME([dc].[schema_id]), '].[', OBJECT_NAME([dc].[parent_object_id]), ']', 
			CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [dc].[name],
			CHAR(10), CHAR(9), 'DEFAULT ', [dc].[definition], ' FOR [', [c].[name], ']',
			CHAR(10), 'END') AS [definition]
	FROM sys.default_constraints AS [dc]
		INNER JOIN sys.columns AS [c]
			ON [c].[object_id] = [dc].[parent_object_id]
			AND [c].[column_id] = [dc].[parent_column_id]
	WHERE [dc].[is_ms_shipped] = 0
		AND [dc].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
END

--============================================================--
/* Check Constaints */

IF @check_constraints = 1
BEGIN
	INSERT INTO @definitions
	SELECT [cc].[type],
		SCHEMA_NAME([cc].[schema_id]) AS [schema],
		OBJECT_NAME([cc].[parent_object_id]) AS [table],
		[cc].[name],
		[cc].[object_id],
		CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([cc].[schema_id]), '].[', [cc].[name], ']'', ''C'') IS NULL)', 
			CHAR(10), 'BEGIN', 
			CHAR(10), 'ALTER TABLE [', SCHEMA_NAME([cc].[schema_id]), '].[', OBJECT_NAME([cc].[parent_object_id]), ']', 
			CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [cc].[name],
			CHAR(10), CHAR(9), 'CHECK ', [cc].[definition],
			CHAR(10), 'END') AS [definition]
	FROM sys.check_constraints AS [cc]
		INNER JOIN sys.columns AS [c]
			ON [c].[object_id] = [cc].[parent_object_id]
			AND [c].[column_id] = [cc].[parent_column_id]
	WHERE [cc].[is_ms_shipped] = 0
		AND [cc].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
END

--============================================================--
/* Primary Keys and Unique Constraints */

IF @primary_keys = 1 OR @unique_constraints = 1
BEGIN
	INSERT INTO @definitions
	SELECT IIF([i].[is_primary_key] = 1, 'PK', 'UQ') AS [type],
		SCHEMA_NAME([schema_id]) AS [schema],
		[t].[name] AS [table],
		[i].[name],
		[i].[object_id],
		CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([t].[schema_id]), '].[', [i].[name], ']'', ''', IIF([i].[is_primary_key] = 1, 'PK', 'UQ'), ''') IS NULL)', 
			CHAR(10), 'BEGIN', 
			CHAR(10), 'ALTER TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], ']',
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
			')',
			CHAR(10), 'END') AS [definition]
	FROM sys.indexes AS [i]
		INNER JOIN sys.tables AS [t]
			ON [t].[object_id] = [i].[object_id]
	WHERE ([i].[is_primary_key] = 1
			OR [i].[is_unique_constraint] = 1)
		AND [t].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
END

--============================================================--
/* Foreign Keys */

INSERT INTO @definitions
SELECT [fk].[type],
	SCHEMA_NAME([t].[schema_id]) AS [schema],
	[t].[name] AS [table],
	[fk].[name],
	[fk].[object_id],
	CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([t].[schema_id]), '].[', [fk].[name], ']'', ''F'') IS NULL)', 
		CHAR(10), 'BEGIN', 
		CHAR(10), 'ALTER TABLE [', SCHEMA_NAME([t].[schema_id]), '].[', [t].[name], ']',
		CHAR(10), CHAR(9), 'ADD CONSTRAINT ', [fk].[name], 
		CHAR(10), CHAR(9), 'FOREIGN KEY ([', [c].[name], '])',
		CHAR(10), CHAR(9), 'REFERENCES [', SCHEMA_NAME([s].[schema_id]), '].[', [s].[name], '] ([', [b].[name], '])',
		CHAR(10), CHAR(9), 'ON DELETE ', REPLACE([fk].[delete_referential_action_desc], '_', ' ') COLLATE database_default,
		CHAR(10), CHAR(9), 'ON UPDATE ', REPLACE([fk].[update_referential_action_desc], '_', ' ') COLLATE database_default,
		CHAR(10), 'END') AS [definition]
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
	AND ([t].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
		OR [s].[schema_id] IN (SELECT [schema_id] FROM @schemata_array))

--============================================================--
/* Functions, Stored Procedures, Triggers and Views */

IF 1 IN (@scalar_functions, @table_valued_functions, @stored_procedures, @views)
BEGIN
	INSERT INTO @definitions
	SELECT REPLACE([o].[type], 'IF', 'TF') AS [type],
		SCHEMA_NAME([o].[schema_id]) AS [schema],
		IIF([o].[type] = 'TR', OBJECT_NAME([o].[parent_object_id]), NULL) AS [table],
		[o].[name],
		[o].[object_id],
		CONCAT('IF (OBJECT_ID(''[', SCHEMA_NAME([o].[schema_id]), '].[', [o].[name], ']'', ''', RTRIM([o].[type]) COLLATE database_default, ''') IS NULL)', 
		CHAR(10), 'BEGIN', 
		CHAR(10), 'EXEC(''', REPLACE([m].[definition], '''', ''''''), ''')',
		CHAR(10), 'END') AS [definition]
	FROM sys.objects AS [o]
		INNER JOIN sys.sql_modules AS [m]
			ON [m].[object_id] = [o].[object_id]
	WHERE [o].[type] IN ('FN', 'IF', 'TF', 'V', 'P', 'TR')
		AND [o].[is_ms_shipped] = 0
		AND [o].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
		AND NOT ([o].[schema_id] = SCHEMA_ID('jra')
			AND ([o].[name] = 'usp_build_db_creator'
				OR [o].[name] LIKE 'usp_create_[[]%]'
				OR [o].[name] LIKE 'usp_drop_and_create_[[]%]'))
END

--============================================================--
/* Indexes */

IF @indexes = 1
BEGIN
	INSERT INTO @definitions
	SELECT 'I' AS [type],
		SCHEMA_NAME([o].[schema_id]) AS [schema],
		[o].[name] AS [table],
		[i].[name],
		[i].[object_id],
		CONCAT('IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name] = ''', [i].[name], ''' AND [object_id] = OBJECT_ID(''[', SCHEMA_NAME([o].[schema_id]), '].[', [o].[name], ']''))', 
		CHAR(10), 'BEGIN', 
		CHAR(10), 'CREATE ', IIF([i].[is_unique] = 1, 'UNIQUE ', ''), [i].[type_desc] COLLATE database_default, ' INDEX ', [i].[name], ' ON [', SCHEMA_NAME([o].[schema_id]), '].[', [o].[name], '](',
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
			');',
			CHAR(10), 'END') AS [definition]
	FROM sys.indexes AS [i]
		INNER JOIN sys.objects AS [o]
			ON [o].[object_id] = [i].[object_id]
	WHERE [i].[is_primary_key] = 0
		AND [i].[is_unique_constraint] = 0
		AND [i].[index_id] > 0
		AND [o].[type] IN ('U', 'V')
		AND [o].[schema_id] IN (SELECT [schema_id] FROM @schemata_array)
END

--============================================================--
/* Definitions Cursor */

SET @sql = 'CREATE OR ALTER PROCEDURE '
SET @description = CONCAT('[jra].[usp_', IIF(@replace = 1, 'drop_and_', ''), 'create_[', DB_NAME(), ']]_[', REPLACE(REPLACE(@schemata, ',', ']]_['), ' ', ''), ']]]')
IF LEN(@description) >= 128
	SET @sql = CONCAT(@sql, SUBSTRING(@description, 1, CHARINDEX(']]', @description)) + ']]')
ELSE
	SET @sql = CONCAT(@sql, @description)

SET @sql += CONCAT(CHAR(10), 'AS', CHAR(10), '/* Script Generation: ', GETDATE(), ' */', CHAR(10), 'BEGIN')

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
			CHAR(10), CHAR(9), 'ALTER TABLE [', @schema, '].[', @table, '] DROP CONSTRAINT ', @name, ';', CHAR(10))
	END
	ELSE IF @type IN ('FN', 'TF', 'V') AND @replace = 1
	BEGIN
		IF @R = 1
			SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* Dropping ', @description, ' */', CHAR(10))
		SET @sql += CONCAT('DROP ', IIF(@type = 'V', 'VIEW', 'FUNCTION'), ' IF EXISTS [', @schema, '].[', @name, '];', CHAR(10))
	END

	FETCH NEXT FROM definitions_cursor
	INTO @R, @O, @type, @description, @schema, @table, @name, @definition
END

FETCH FIRST FROM definitions_cursor
INTO @R, @O, @type, @description, @schema, @table, @name, @definition

IF @replace = 1
BEGIN
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @type = 'U'
		BEGIN
			IF @R = 1
				SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* Dropping ', @description, ' */', CHAR(10))
			SET @sql += CONCAT('DROP TABLE IF EXISTS [', @schema, '].[', @table, '];', CHAR(10))
		END

		FETCH NEXT FROM definitions_cursor
		INTO @R, @O, @type, @description, @schema, @table, @name, @definition
	END
END

FETCH FIRST FROM definitions_cursor
INTO @R, @O, @type, @description, @schema, @table, @name, @definition

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @R = 1
		SET @sql += CONCAT(CHAR(10), '--============================================================--', CHAR(10), '/* ', @description, ' */', CHAR(10))
	IF @O = 1 and @name IS NOT NULL AND @table IS NOT NULL
		SET @sql += CONCAT(CHAR(10), '/* [', @schema, '].[', @table, '] */')
	SET @sql += CONCAT(CHAR(10), '-- ', COALESCE(@name, @table, @schema), CHAR(10), @definition, ';', CHAR(10))

	FETCH NEXT FROM definitions_cursor
	INTO @R, @O, @type, @description, @schema, @table, @name, @definition
END

CLOSE definitions_cursor
DEALLOCATE definitions_cursor

SET @sql += CONCAT(CHAR(10), 'END')

IF @print = 1
	EXECUTE [jra].[usp_print] @sql
IF @display = 1
BEGIN
	SELECT [types].[type],
		[types].[description],
		[defs].[schema],
		[defs].[table],
		[defs].[name],
		[defs].[definition]
	FROM @definitions AS [defs]
		INNER JOIN @types AS [types]
			ON [types].[type] = [defs].[type]
	ORDER BY [types].[order]
END
EXEC(@sql)
GO
