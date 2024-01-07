USE [TRANSACTIONS]
DROP DATABASE [personal]
GO

CREATE DATABASE [personal]
GO

USE [personal]
GO

CREATE SCHEMA [jra]
GO

CREATE SCHEMA [finances]
GO

CREATE TABLE [finances].[budgets] (
	[id] int NOT NULL IDENTITY(1, 1),
	[index] int NOT NULL,
	[category] int NOT NULL,
	[amount] money NOT NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[budget_index] (
	[id] int NOT NULL IDENTITY(1, 1),
	[name] nvarchar(64) NOT NULL,
	[start_date] date NOT NULL,
	[end_date] date NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[categories] (
	[id] int NOT NULL IDENTITY(1, 1),
	[name] nvarchar(64) NOT NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[locations] (
	[id] int NOT NULL IDENTITY(1, 1),
	[type] int NULL,
	[name] nvarchar(64) NOT NULL,
	[initial] money NOT NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[load] (
	[id] int NOT NULL IDENTITY(1, 1),
	[transaction] int NULL,
	[currency] nchar(1) NULL,
	[date] date NULL,
	[amount] money NULL,
	[from_id] int NULL,
	[from_alpha] nvarchar(64) NULL,
	[to_id] int NULL,
	[to_alpha] nvarchar(64) NULL,
	[category_id] int NULL,
	[category_alpha] nvarchar(64) NULL,
	[description] nvarchar(256) NULL,
	[source_id] int NULL,
	[source_alpha] nvarchar(64) NULL,
	[load_date] datetime NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[location_types] (
	[id] int NOT NULL IDENTITY(1, 1),
	[name] nvarchar(64) NOT NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[sources] (
	[id] int NOT NULL IDENTITY(1, 1),
	[name] nvarchar(128) NOT NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

CREATE TABLE [finances].[transactions] (
	[id] int NOT NULL IDENTITY(1, 1),
	[currency] nchar(1) NOT NULL,
	[date] date NOT NULL,
	[amount] money NOT NULL,
	[from] int NULL,
	[to] int NULL,
	[category] int NOT NULL,
	[description] nvarchar(256) NOT NULL,
	[source] int NULL,
	[created_date] datetime NOT NULL,
	[modified_date] datetime NOT NULL
)

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_amount
	DEFAULT (0) FOR [amount]

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT D_finances_budget_index_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT D_finances_budget_index_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[categories]
	ADD CONSTRAINT D_finances_categories_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[categories]
	ADD CONSTRAINT D_finances_categories_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_initial
	DEFAULT (0) FOR [initial]

ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_currency
	DEFAULT ('£') FOR [currency]

ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT D_finances_location_types_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT D_finances_location_types_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]
	
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_amount
	DEFAULT (0) FOR [amount]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_category
	DEFAULT (0) FOR [category]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_currency
	DEFAULT ('£') FOR [currency]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_description
	DEFAULT ('') FOR [description]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_source
	DEFAULT (0) FOR [source]

ALTER TABLE [finances].[sources]
	ADD CONSTRAINT D_finances_sources_created_date
	DEFAULT (GETUTCDATE()) FOR [created_date]

ALTER TABLE [finances].[sources]
	ADD CONSTRAINT D_finances_sources_modified_date
	DEFAULT (GETUTCDATE()) FOR [modified_date]

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT C_finances_transactions_amount
	CHECK ([amount] >= 0)

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT C_finances_transactions_date
	CHECK ([date] >= '20170701')

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT PK_finances_budgets_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT PK_finances_budget_index_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[categories]
	ADD CONSTRAINT PK_finances_categories_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT PK_finances_locations_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[load]
	ADD CONSTRAINT PK_finances_load_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT PK_finances_location_types_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[sources]
	ADD CONSTRAINT PK_finances_sources_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT PK_finances_transactions_id
	PRIMARY KEY CLUSTERED([id])

ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT UQ_finances_budget_index_name
	UNIQUE NONCLUSTERED([name])

ALTER TABLE [finances].[categories]
	ADD CONSTRAINT UQ_finances_categories_name
	UNIQUE NONCLUSTERED([name])

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT UQ_finances_locations_name
	UNIQUE NONCLUSTERED([name])

ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT UQ_finances_location_types_name
	UNIQUE NONCLUSTERED([name])

DECLARE @seed int

SET IDENTITY_INSERT [finances].[budget_index] ON
INSERT INTO [finances].[budget_index]([id], [start_date], [end_date], [created_date], [name])
SELECT BI_ID, BI_StartDate, BI_EndDate, BI_CreatedDate, BI_Name
FROM [Finances].[dbo].[BIBudgetIndex]
SET IDENTITY_INSERT [finances].[budget_index] OFF
SET @seed = (SELECT MAX([BI_ID]) FROM [Finances].[dbo].[BIBudgetIndex])
DBCC CHECKIDENT ('[finances].[budget_index]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[budgets] ON
INSERT INTO [finances].[budgets]([id], [amount], [category], [created_date], [index])
SELECT BD_ID, BD_Amount, BD_Category, BD_CreatedDate, BD_Index
FROM [Finances].[dbo].[BDBudget]
SET IDENTITY_INSERT [finances].[budgets] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[budgets])
DBCC CHECKIDENT ('[finances].[budgets]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[categories] ON
INSERT INTO [finances].[categories]([created_date], [id], [name])
SELECT CT_CreatedDate, CT_ID, CT_Name
FROM [Finances].[dbo].[CTCategories]
SET IDENTITY_INSERT [finances].[categories] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[categories])
DBCC CHECKIDENT ('[finances].[categories]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[locations] ON
INSERT INTO [finances].[locations]([created_date], [id], [initial], [name], [type])
SELECT LC_CreatedDate, LC_ID, LC_Initial, LC_Name, LC_Type
FROM [Finances].[dbo].[LCLocations]
SET IDENTITY_INSERT [finances].[locations] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[locations])
DBCC CHECKIDENT ('[finances].[locations]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[location_types] ON
INSERT INTO [finances].[location_types]([created_date], [id], [name])
SELECT LT_CreatedDate, LT_ID, LT_Name
FROM [Finances].[dbo].[LTLocationTypes]
SET IDENTITY_INSERT [finances].[location_types] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[location_types])
DBCC CHECKIDENT ('[finances].[location_types]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[sources] ON
INSERT INTO [finances].[sources]([id], [name])
SELECT SR_ID, SR_Name
FROM [Finances].[dbo].[SRSources]
SET IDENTITY_INSERT [finances].[sources] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[sources])
DBCC CHECKIDENT ('[finances].[sources]', RESEED, @seed)

SET IDENTITY_INSERT [finances].[transactions] ON
INSERT INTO [finances].[transactions]([amount], [category], [created_date], [currency], [date], [description], [from], [id], [source], [to])
SELECT TR_Amount, TR_Category, TR_CreatedDate, TR_Currency, TR_Date, TR_Description, TR_From, TR_ID, TR_Source, TR_To
FROM [Finances].[dbo].[TRTransactions]
SET IDENTITY_INSERT [finances].[transactions] OFF
SET @seed = (SELECT MAX([id]) FROM [finances].[transactions])
DBCC CHECKIDENT ('[finances].[transactions]', RESEED, @seed)

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT F_finances_budgets_index
	FOREIGN KEY ([index])
	REFERENCES [finances].[budget_index] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT F_finances_budgets_category
	FOREIGN KEY ([category])
	REFERENCES [finances].[categories] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

ALTER TABLE [finances].[locations]
	ADD CONSTRAINT F_finances_locations_type
	FOREIGN KEY ([type])
	REFERENCES [finances].[location_types] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_from
	FOREIGN KEY ([from])
	REFERENCES [finances].[locations] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_source
	FOREIGN KEY ([source])
	REFERENCES [finances].[sources] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_to
	FOREIGN KEY ([to])
	REFERENCES [finances].[locations] ([id])
	ON DELETE NO ACTION
	ON UPDATE NO ACTION

ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_category
	FOREIGN KEY ([category])
	REFERENCES [finances].[categories] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE

EXEC('CREATE TRIGGER TR_finances_budgets_modified_date
ON [finances].[budgets]
AFTER UPDATE
AS
UPDATE [finances].[budgets]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_budget_index_modified_date
ON [finances].[budget_index]
AFTER UPDATE
AS
UPDATE [finances].[budget_index]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_categories_modified_date
ON [finances].[categories]
AFTER UPDATE
AS
UPDATE [finances].[categories]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_load_modified_date
ON [finances].[load]
AFTER UPDATE
AS
UPDATE [finances].[load]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_locations_modified_date
ON [finances].[locations]
AFTER UPDATE
AS
UPDATE [finances].[locations]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_location_types_modified_date
ON [finances].[location_types]
AFTER UPDATE
AS
UPDATE [finances].[location_types]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_sources_modified_date
ON [finances].[sources]
AFTER UPDATE
AS
UPDATE [finances].[sources]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')

EXEC('CREATE TRIGGER TR_finances_transactions_modified_date
ON [finances].[transactions]
AFTER UPDATE
AS
UPDATE [finances].[transactions]
SET [modified_date] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
GO

CREATE OR ALTER FUNCTION [finances].[ufn_budget_name](@id int)
RETURNS nvarchar(64)
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[budget_index] WHERE [id] = @id)
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_budget_id](@name nvarchar(64))
RETURNS int
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[budget_index] WHERE [name] = @name), (SELECT [id] FROM [finances].[budget_index] WHERE [id] = CONVERT(int, @name)))
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_category_name](@id int)
RETURNS nvarchar(64)
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[categories] WHERE [id] = @id)
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_category_id](@name nvarchar(64))
RETURNS int
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[categories] WHERE [name] = @name), (SELECT [id] FROM [finances].[categories] WHERE [id] = CONVERT(int, @name)))
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_location_name](@id int)
RETURNS nvarchar(64)
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[locations] WHERE [id] = @id)
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_location_id](@name nvarchar(64))
RETURNS int
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[locations] WHERE [name] = @name), (SELECT [id] FROM [finances].[locations] WHERE [id] = CONVERT(int, @name)))
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_location_type_name](@id int)
RETURNS nvarchar(64)
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[location_types] WHERE [id] = @id)
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_location_type_id](@name nvarchar(64))
RETURNS int
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[location_types] WHERE [name] = @name), (SELECT [id] FROM [finances].[location_types] WHERE [id] = CONVERT(int, @name)))
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_source_name](@id int)
RETURNS nvarchar(64)
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[sources] WHERE [id] = @id)
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_source_id](@name nvarchar(64))
RETURNS int
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[sources] WHERE [name] = @name), (SELECT [id] FROM [finances].[sources] WHERE [id] = CONVERT(int, @name)))
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_get_transactions] (
	@start_date date = '2017-07-01',
	@end_date date = NULL,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = NULL,
	@description nvarchar(max) = '%',
	@source nvarchar(64) = NULL,
	@created_start datetime = NULL,
	@created_end datetime = NULL,
	@modified_start datetime = NULL,
	@modified_end datetime = NULL
)
RETURNS table
AS
RETURN
SELECT [tr].[id] AS [ID],
	[tr].[currency] AS [ ],
	[tr].[amount] AS [Amount],
	[tr].[date] AS [Date],
	[ft].[id] AS [From Type ID],
	[ft].[name] AS [From Type Name],
	[fr].[id] AS [From ID],
	[fr].[name] AS [From Name],
	[tt].[id] AS [To Type ID],
	[tt].[name] AS [To Type Name],
	[to].[id] AS [To ID],
	[to].[name] AS [To Name],
	[ct].[id] AS [Category ID],
	[ct].[name] AS [Category Name],
	[tr].[description] AS [Description],
	[sr].[id] AS [Source ID],
	[sr].[name] AS [Source Name]
FROM [finances].[transactions] AS [tr]
	LEFT JOIN [finances].[locations] AS [fr]
		ON [fr].[id] = [tr].[from]
	LEFT JOIN [finances].[location_types] AS [ft]
		ON [ft].[id] = [fr].[type]
	LEFT JOIN [finances].[locations] AS [to]
		ON [to].[id] = [tr].[to]
	LEFT JOIN [finances].[location_types] AS [tt]
		ON [tt].[id] = [to].[type]
	LEFT JOIN [finances].[categories] AS [ct]
		ON [ct].[id] = [tr].[category]
	LEFT JOIN [finances].[sources] AS [sr]
		ON [sr].[id] = [tr].[source]
WHERE [tr].[date] BETWEEN @start_date AND ISNULL(@end_date, (SELECT MAX([date]) FROM [finances].[transactions]))
	AND COALESCE([tr].[from], 0) = COALESCE([finances].[ufn_location_id](@from), [tr].[from], 0)
	AND COALESCE([tr].[to], 0) = COALESCE([finances].[ufn_location_id](@to), [tr].[to], 0)
	AND COALESCE([tr].[category], 0) = COALESCE([finances].[ufn_category_id](@category), [tr].[category], 0)
	AND ISNULL([tr].[description], '') LIKE @description
	AND COALESCE([tr].[source], 0) = COALESCE([finances].[ufn_source_id](@source), [tr].[source], 0)
	AND [tr].[created_date] BETWEEN ISNULL(@created_start, (SELECT MIN([created_date]) FROM [finances].[transactions])) AND ISNULL(@created_end, (SELECT MAX([created_date]) FROM [finances].[transactions]))
	AND [tr].[modified_date] BETWEEN ISNULL(@modified_start, (SELECT MIN([modified_date]) FROM [finances].[transactions])) AND ISNULL(@modified_end, (SELECT MAX([modified_date]) FROM [finances].[transactions]))
GO

CREATE OR ALTER PROCEDURE [finances].[usp_load_transactions] (
	@commit bit = 0,
	@print bit = 1,
	@delete bit = 0,
	@id int = NULL
)
AS
IF @print = 1 AND @commit = 1
	PRINT('Committing transactions.')
ELSE IF @print = 1 AND @commit = 0
	PRINT('Not committing transactions.')
ELSE
	SET NOCOUNT ON

BEGIN TRANSACTION

DECLARE @now datetime
SET @now = GETUTCDATE()

IF @delete = 1
	IF @print = 1
		PRINT('Deleting loaded data from load table.')
	DELETE FROM [finances].[load] WHERE [load_date] IS NOT NULL

IF @print = 1
	PRINT('Updating load table item identities from alpha columns.')
UPDATE [finances].[load]
SET [from_id] = [finances].[ufn_location_id]([from_alpha]),
	[to_id] = [finances].[ufn_location_id]([to_alpha]),
	[category_id] = [finances].[ufn_category_id]([category_alpha]),
	[source_id] = [finances].[ufn_source_id]([source_alpha])
WHERE [load_date] IS NULL
	AND ISNULL(@id, [id]) = [id]
UPDATE [finances].[load]
SET [from_alpha] = [finances].[ufn_location_name]([from_id]),
	[to_alpha] = [finances].[ufn_location_name]([to_id]),
	[category_alpha] = [finances].[ufn_category_name]([category_id]),
	[source_alpha] = [finances].[ufn_source_name]([source_id])
WHERE [load_date] IS NULL
	AND ISNULL(@id, [id]) = [id]

IF @print = 1
BEGIN
	SELECT NULL AS [ID],
		[currency] AS [ ],
		[amount],
		[date],
		[from_alpha],
		[to_alpha],
		[category_alpha],
		[description],
		[source_alpha]
	FROM [finances].[load]
	WHERE [load_date] IS NULL
END

IF @print = 1
	PRINT('Inserting into transactions table.')
DECLARE @ids table ([tr_id] int, [ld_id] int)
MERGE INTO [finances].[transactions] AS [target]
USING (SELECT * FROM [finances].[load] WHERE [load_date] IS NULL AND ISNULL(@id, [id]) = [id]) AS [source] ON 1 = 0
WHEN NOT MATCHED BY TARGET THEN
INSERT (
	[currency],
	[amount],
	[date],
	[from],
	[to],
	[category],
	[description],
	[source],
	[created_date],
	[modified_date]
)
VALUES (
	[source].[currency],
	[source].[amount],
	[source].[date],
	[source].[from_id],
	[source].[to_id],
	[source].[category_id],
	[source].[description],
	[source].[source_id],
	@now, 
	@now
)
OUTPUT [inserted].[id], [source].[id] INTO @ids (tr_id, ld_id);

IF @print = 1
	PRINT('Updating load table with load date and transaction identity.')
UPDATE [finances].[load]
SET [transaction] = [tr_id],
	[load_date] = @now
FROM @ids
WHERE [ld_id] = [finances].[load].[id]

IF @print = 1
BEGIN
	SELECT [transaction] AS [ID],
		[currency] AS [ ],
		[amount] AS [Amount],
		[date] AS [Date],
		[from_alpha] AS [From Name],
		[to_alpha] AS [To Name],
		[category_alpha] AS [Category Name],
		[description] AS [Description],
		[source_alpha] AS [Source Name],
		[id] AS [Load ID]
	FROM [finances].[load]
	WHERE [load_date] = @now
	ORDER BY [date] DESC, [transaction] DESC

	SELECT [ID],
		[ ],
		[Amount],
		[Date],
		[From Name],
		[To Name],
		[Category Name],
		[Description],
		[Source Name]
	FROM [finances].[ufn_get_transactions]('2017-07-01', NULL, NULL, NULL, NULL, '%', NULL, @now, @now, @now, @now)
	WHERE [ID] IN (SELECT [transaction] FROM [finances].[load] WHERE [load_date] = @now)
	ORDER BY [Date] DESC, [ID] DESC
END

IF @print = 1
	SELECT * FROM [finances].[ufn_duplicate_transactions](8)

IF @commit = 1
	COMMIT TRANSACTION
ELSE
	ROLLBACK TRANSACTION
GO

CREATE OR ALTER FUNCTION [finances].[ufn_duplicate_transactions](@threshold tinyint = 8)
RETURNS TABLE
RETURN
SELECT [L].[id] AS [Potential],
	[R].[id] AS [Duplicates]
FROM [finances].[transactions] AS [L]
	CROSS JOIN [finances].[transactions] AS [R]
WHERE [L].[id] < [R].[id]
	AND IIF([L].[currency] = [R].[currency], 1, 0)
		+ IIF([L].[amount] = [R].[amount], 1, 0)
		+ IIF([L].[date] = [R].[date], 1, 0)
		+ IIF([L].[from] = [R].[from], 1, 0)
		+ IIF([L].[to] = [R].[to], 1, 0)
		+ IIF([L].[category] = [R].[category], 1, 0)
		+ IIF([L].[description] = [R].[description], 1, 0)
		+ IIF([L].[source] = [R].[source], 1, 0)
		>= @threshold
GO

CREATE OR ALTER PROCEDURE [finances].[usp_new_transaction] (
	@commit bit = 0,
	@print bit = 1,
	@currency nchar(1) = '£',
	@amount money,
	@date date,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = 0,
	@description nvarchar(256),
	@source nvarchar(64) = 0
)
AS
DECLARE @ids table ([id] int)
INSERT INTO [finances].[load]([currency], [amount], [date], [from_alpha], [to_alpha], [category_alpha], [description], [source_alpha])
OUTPUT [inserted].[id] INTO @ids([id])
VALUES (@currency, @amount, @date, @from, @to, @category, @description, @source)

DECLARE @id int
SET @id = (SELECT TOP(1) [id] FROM @ids)
EXECUTE [finances].[usp_load_transactions] @commit, @print, 0, @id

IF @commit = 0
	DELETE FROM [finances].[load]
	WHERE [id] IN (SELECT [id] FROM @ids)
GO

CREATE OR ALTER VIEW [finances].[v_master_statement]
AS
WITH [raw] AS (
	SELECT * FROM [finances].[ufn_get_transactions]('2017-07-01', NULL, NULL, NULL, NULL, '%', NULL, NULL, NULL, NULL, NULL)
), [data] AS (
	SELECT [out].[ID],
		[out].[Date],
		[out].[From Type ID] AS [Account Type ID],
		[out].[From Type Name] AS [Account Type Name],
		[out].[From ID] AS [Account ID],
		[out].[From Name] AS [Account Name],
		[out].[To Type ID] AS [Party Type ID],
		[out].[To Type Name] AS [Party Type Name],
		[out].[To ID] AS [Party ID],
		[out].[To Name] AS [Party Name],
		[out].[Category ID],
		[out].[Category Name],
		[out].[ ],
		CONVERT(money, NULL) AS [In],
		[out].[Amount] AS [Out],
		-[out].[Amount] AS [Change],
		[out].[Description],
		[out].[Source ID],
		[out].[Source Name]
	FROM [raw] AS [out]
	UNION
	SELECT [in].[ID],
		[in].[Date],
		[in].[To Type ID] AS [Account Type ID],
		[in].[To Type Name] AS [Account Type Name],
		[in].[To ID] AS [Account ID],
		[in].[To Name] AS [Account Name],
		[in].[From Type ID] AS [Party Type ID],
		[in].[From Type Name] AS [Party Type Name],
		[in].[From ID] AS [Party ID],
		[in].[From Name] AS [Party Name],
		[in].[Category ID],
		[in].[Category Name],
		[in].[ ],
		[in].[Amount] AS [In],
		NULL AS [Out],
		[in].[Amount] AS [Change],
		[in].[Description],
		[in].[Source ID],
		[in].[Source Name]
	FROM [raw] AS [in]
)
SELECT *,
	(SELECT [initial] FROM [finances].[locations] WHERE [id] = [Account ID]) + SUM([data].[Change]) OVER(PARTITION BY [data].[Account ID] ORDER BY [data].[Date] ASC, [data].[ID] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Balance]
FROM [data]
GO

CREATE OR ALTER FUNCTION [finances].[ufn_location_statement](@location nvarchar(64), @start date = '2017-07-01', @end date = NULL)
RETURNS table
RETURN
SELECT *
FROM [finances].[v_master_statement]
WHERE [Account ID] = [finances].[ufn_location_id](@location)
	AND [Date] BETWEEN @start AND ISNULL(@end, GETDATE())
GO

CREATE OR ALTER FUNCTION [finances].[ufn_get_budget](@budget_name nvarchar(64))
RETURNS @output table (
	[Budget ID] [int] NULL,
	[Budget Name] [varchar](64) NULL,
	[Category ID] [int] NOT NULL,
	[Category] [nvarchar](32) NOT NULL,
	[Remaining] [money] NULL,
	[Current Actual] [money] NULL,
	[Current Budget] [money] NULL,
	[Current Difference] [money] NULL,
	[Projected Actual] [money] NULL,
	[Projected Budget] [money] NULL,
	[Projected Difference] [money] NULL,
	[Annual Actual] [money] NULL,
	[Annual Budget] [money] NULL,
	[Annual Difference] [money] NULL,
	[Monthly Actual] [money] NULL,
	[Monthly Budget] [money] NULL,
	[Monthly Difference] [money] NULL,
	[Weekly Actual] [money] NULL,
	[Weekly Budget] [money] NULL,
	[Weekly Difference] [money] NULL,
	[Daily Actual] [money] NULL,
	[Daily Budget] [money] NULL,
	[Daily Difference] [money] NULL
)
AS
BEGIN
DECLARE @budget_id int,
	@start date,
	@end date,
	@days int,
	@length int

IF @budget_name IS NULL
	SET @budget_name = (SELECT TOP(1) [id] FROM [finances].[budget_index] ORDER BY [start_date] DESC)
SET @budget_id = [finances].[ufn_budget_id](@budget_name)
IF @budget_id IS NULL
	RETURN
ELSE
	SET @budget_name = [finances].[ufn_budget_name](@budget_id)

SET @start = (SELECT [start_date] FROM [finances].[budget_index] WHERE [id] = @budget_id)
SET @end = ISNULL((SELECT [end_date] FROM [finances].[budget_index] WHERE [id] = @budget_id), GETDATE())
SET @days = DATEDIFF(day, @start, IIF(@end < GETDATE(), @end, GETDATE()))
SET @length = DATEDIFF(day, @start, @end)

;WITH Actual AS (
SELECT [Category ID] AS [Category],
	SUM([Change])/@Days AS [Amount]
FROM [finances].[v_master_statement]
WHERE [Account Type ID] IN (1, 18)
	AND [Date] BETWEEN @start AND @end
GROUP BY [Category ID]
), Budget AS (
SELECT [bd].[category] AS [Category],
	[bd].[amount]/365.25 AS [Amount]
FROM [finances].[budget_index] AS [bi]
	INNER JOIN [finances].[budgets] AS [bd] ON [bd].[index] = [bi].[id]
WHERE [bi].[id] = @budget_id
), Calcs AS (
SELECT [ct].[id] AS [ID],
	[ct].[name] AS [Category],
	@Days*ISNULL([A].[Amount], 0) AS [Current Actual],
	@Length*ISNULL([A].[Amount], 0) AS [Projected Actual],
	365.25*ISNULL([A].[Amount], 0) AS [Annual Actual],
	30.4375*ISNULL([A].[Amount], 0) AS [Monthly Actual],
	7*ISNULL([A].[Amount], 0) AS [Weekly Actual],
	ISNULL([A].[Amount], 0) AS [Daily Actual],
	@Days*ISNULL([B].[Amount], 0) AS [Current Budget],
	@Length*ISNULL([B].[Amount], 0) AS [Projected Budget],
	365.25*ISNULL([B].[Amount], 0) AS [Annual Budget],
	30.4375*ISNULL([B].[Amount], 0) AS [Monthly Budget],
	7*ISNULL([B].[Amount], 0) AS [Weekly Budget],
	ISNULL([B].[Amount], 0) AS [Daily Budget]
FROM [finances].[categories] AS [ct]
	LEFT JOIN Actual AS [A]
		ON [A].[Category] = [ct].[id]
	LEFT JOIN Budget AS [B]
		ON [B].[Category] = [ct].[id]
WHERE COALESCE([A].[Category], [B].[Category]) IS NOT NULL
)
INSERT INTO @output
SELECT @budget_id AS [Budget ID],
	@budget_name AS [Budget Name],
	[C].[ID] AS [Category ID],
	[C].[Category],
	CAST(ROUND([C].[Current Actual] - [C].[Projected Budget], 2) AS money) AS [Remaining],
	CAST(ROUND([C].[Current Actual], 2) AS money) AS [Current Actual],
	CAST(ROUND([C].[Current Budget], 2) AS money) AS [Current Budget],
	CAST(ROUND([C].[Current Actual] - [C].[Current Budget], 2) AS money) AS [Current Difference],
	CAST(ROUND([C].[Projected Actual], 2) AS money) AS [Projected Actual],
	CAST(ROUND([C].[Projected Budget], 2) AS money) AS [Projected Budget],
	CAST(ROUND([C].[Projected Actual] - [C].[Projected Budget], 2) AS money) AS [Projected Difference],
	CAST(ROUND([C].[Annual Actual], 2) AS money) AS [Annual Actual],
	CAST(ROUND([C].[Annual Budget], 2) AS money) AS [Annual Budget],
	CAST(ROUND([C].[Annual Actual] - [C].[Annual Budget], 2) AS money) AS [Annual Difference],
	CAST(ROUND([C].[Monthly Actual], 2) AS money) AS [Monthly Actual],
	CAST(ROUND([C].[Monthly Budget], 2) AS money) AS [Monthly Budget],
	CAST(ROUND([C].[Monthly Actual] - [C].[Monthly Budget], 2) AS money) AS [Monthly Difference],
	CAST(ROUND([C].[Weekly Actual], 2) AS money) AS [Weekly Actual],
	CAST(ROUND([C].[Weekly Budget], 2) AS money) AS [Weekly Budget],
	CAST(ROUND([C].[Weekly Actual] - [C].[Weekly Budget], 2) AS money) AS [Weekly Difference],
	CAST(ROUND([C].[Daily Actual], 2) AS money) AS [Daily Actual],
	CAST(ROUND([C].[Daily Budget], 2) AS money) AS [Daily Budget],
	CAST(ROUND([C].[Daily Actual] - [C].[Daily Budget], 2) AS money) AS [Daily Difference]
FROM Calcs AS [C]
ORDER BY 4
RETURN
END
GO

CREATE OR ALTER FUNCTION [finances].[ufn_get_balances] (
	@date date = NULL,
	@location_type nvarchar(64) = NULL,
	@location nvarchar(64) = NULL
)
RETURNS table
RETURN
WITH [data] AS (
	SELECT ROW_NUMBER() OVER(PARTITION BY [Account ID] ORDER BY [Date] DESC) AS [R],
		[Account Type ID],
		[Account Type Name],
		[Account ID],
		[Account Name],
		[Balance],
		[Date]
	FROM [finances].[v_master_statement]
	WHERE [Date] <= ISNULL(@date, GETDATE())
		AND [Account ID] = ISNULL([finances].[ufn_location_id](@location), [Account ID])
		AND [Account Type ID] = ISNULL([finances].[ufn_location_type_id](@location_type), [Account Type ID])
)
SELECT * FROM [data] WHERE [R] = 1
GO

CREATE OR ALTER PROCEDURE [finances].[usp_get_budget](@budget_name nvarchar(64) = NULL)
AS
SELECT * FROM [finances].[ufn_get_budget](@budget_name) ORDER BY [Budget Name], [Category]
GO

CREATE OR ALTER PROCEDURE [finances].[usp_get_balances](
	@date date = NULL,
	@location_type nvarchar(64) = NULL,
	@location nvarchar(64) = NULL
)
AS
SELECT * FROM [finances].[ufn_get_balances](@date, @location_type, @location) ORDER BY [Account Type Name] ASC, [Account Name] ASC
GO

CREATE OR ALTER PROCEDURE [finances].[usp_get_transactions](
	@start_date date = '2017-07-01',
	@end_date date = NULL,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = NULL,
	@description nvarchar(max) = '%',
	@source nvarchar(64) = NULL,
	@created_start datetime = NULL,
	@created_end datetime = NULL,
	@modified_start datetime = NULL,
	@modified_end datetime = NULL
)
AS
SELECT * FROM [finances].[ufn_get_transactions](@start_date, @end_date, @from, @to, @category, @description, @source, @created_start, @created_end, @modified_start, @modified_end) ORDER BY [Date] DESC, [ID] DESC
GO