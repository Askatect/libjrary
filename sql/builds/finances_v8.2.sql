CREATE OR ALTER PROCEDURE [jra].[usp_drop_and_create_[personal]]_[finances]]_[jra]]] AS
/*
Author: [jra].[usp_build_db_creator]
Date: 2024-01-15 11:09:05

Description:
Drops and creates the objects from the schemata finances, from database personal.

Returns: 
- jra (schema)
- finances (schema)
- budgets (table)
- budget_index (table)
- categories (table)
- locations (table)
- load (table)
- location_types (table)
- sources (table)
- transactions (table)
- budgets_data (data)
- locations_data (data)
- transactions_data (data)
- budget_index_data (data)
- load_data (data)
- location_types_data (data)
- categories_data (data)
- sources_data (data)
- D_finances_budgets_amount (default constraint)
- D_finances_budgets_created_date (default constraint)
- D_finances_budgets_modified_date (default constraint)
- D_finances_budget_index_created_date (default constraint)
- D_finances_budget_index_modified_date (default constraint)
- D_finances_categories_created_date (default constraint)
- D_finances_categories_modified_date (default constraint)
- D_finances_locations_initial (default constraint)
- D_finances_locations_created_date (default constraint)
- D_finances_locations_modified_date (default constraint)
- D_finances_load_currency (default constraint)
- D_finances_load_created_date (default constraint)
- D_finances_load_modified_date (default constraint)
- D_finances_location_types_created_date (default constraint)
- D_finances_location_types_modified_date (default constraint)
- D_finances_sources_created_date (default constraint)
- D_finances_sources_modified_date (default constraint)
- D_finances_transactions_currency (default constraint)
- D_finances_transactions_amount (default constraint)
- D_finances_transactions_category (default constraint)
- D_finances_transactions_description (default constraint)
- D_finances_transactions_source (default constraint)
- D_finances_transactions_created_date (default constraint)
- D_finances_transactions_modified_date (default constraint)
- C_finances_transactions_amount (check constraint)
- C_finances_transactions_date (check constraint)
- PK_finances_budgets_id (primary key)
- PK_finances_budget_index_id (primary key)
- PK_finances_categories_id (primary key)
- PK_finances_locations_id (primary key)
- PK_finances_load_id (primary key)
- PK_finances_location_types_id (primary key)
- PK_finances_sources_id (primary key)
- PK_finances_transactions_id (primary key)
- UQ_finances_budget_index_name (unique constraint)
- UQ_finances_categories_name (unique constraint)
- UQ_finances_locations_name (unique constraint)
- UQ_finances_location_types_name (unique constraint)
- F_finances_budgets_index (foreign key)
- F_finances_budgets_category (foreign key)
- F_finances_transactions_category (foreign key)
- F_finances_transactions_from (foreign key)
- F_finances_transactions_to (foreign key)
- F_finances_locations_type (foreign key)
- F_finances_transactions_source (foreign key)
- TR_finances_budget_index_modified (trigger)
- TR_finances_budgets_modified (trigger)
- TR_finances_categories_modified (trigger)
- TR_finances_load_modified (trigger)
- TR_finances_location_types_modified (trigger)
- TR_finances_locations_modified (trigger)
- TR_finances_sources_modified (trigger)
- TR_finances_transactions_modified (trigger)
- usp_select_to_html (stored procedure)
- usp_timer (stored procedure)
- usp_transpose_table (stored procedure)
- usp_load_transactions (stored procedure)
- usp_new_transaction (stored procedure)
- usp_get_transactions (stored procedure)
- usp_get_budget (stored procedure)
- usp_get_balances (stored procedure)
- usp_string_table_split (stored procedure)
- usp_print (stored procedure)
- ufn_budget_name (scalar function)
- ufn_budget_id (scalar function)
- ufn_category_name (scalar function)
- ufn_category_id (scalar function)
- ufn_location_name (scalar function)
- ufn_location_id (scalar function)
- ufn_location_type_name (scalar function)
- ufn_location_type_id (scalar function)
- ufn_source_name (scalar function)
- ufn_source_id (scalar function)
- ufn_age (scalar function)
- ufn_extract_pattern (scalar function)
- ufn_gradient_hex (scalar function)
- ufn_json_formatter (scalar function)
- ufn_get_transactions (inline table function)
- ufn_location_statement (inline table function)
- ufn_get_balances (inline table function)
- ufn_duplicate_transactions (inline table function)
- ufn_get_budget (table-valued function)
- ufn_string_split (table-valued function)
- ufn_hexcode_to_rgb (table-valued function)
- v_master_statement (view)

Usage:
EXECUTE [jra].[usp_drop_and_create_[personal]]_[finances]]_[jra]]]
*/
BEGIN
--============================================================--
/* Foreign Key Dropping */

/* [finances].[budgets] */
-- F_finances_budgets_category
IF (OBJECT_ID('F_finances_budgets_category', 'F') IS NOT NULL)
	ALTER TABLE [finances].[budgets] DROP CONSTRAINT F_finances_budgets_category;

-- F_finances_budgets_index
IF (OBJECT_ID('F_finances_budgets_index', 'F') IS NOT NULL)
	ALTER TABLE [finances].[budgets] DROP CONSTRAINT F_finances_budgets_index;

/* [finances].[locations] */
-- F_finances_locations_type
IF (OBJECT_ID('F_finances_locations_type', 'F') IS NOT NULL)
	ALTER TABLE [finances].[locations] DROP CONSTRAINT F_finances_locations_type;

/* [finances].[transactions] */
-- F_finances_transactions_category
IF (OBJECT_ID('F_finances_transactions_category', 'F') IS NOT NULL)
	ALTER TABLE [finances].[transactions] DROP CONSTRAINT F_finances_transactions_category;

-- F_finances_transactions_from
IF (OBJECT_ID('F_finances_transactions_from', 'F') IS NOT NULL)
	ALTER TABLE [finances].[transactions] DROP CONSTRAINT F_finances_transactions_from;

-- F_finances_transactions_source
IF (OBJECT_ID('F_finances_transactions_source', 'F') IS NOT NULL)
	ALTER TABLE [finances].[transactions] DROP CONSTRAINT F_finances_transactions_source;

-- F_finances_transactions_to
IF (OBJECT_ID('F_finances_transactions_to', 'F') IS NOT NULL)
	ALTER TABLE [finances].[transactions] DROP CONSTRAINT F_finances_transactions_to;

--============================================================--
/* Trigger Dropping */

/* [finances].[budget_index] */
-- TR_finances_budget_index_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_budget_index_modified];

/* [finances].[budgets] */
-- TR_finances_budgets_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_budgets_modified];

/* [finances].[categories] */
-- TR_finances_categories_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_categories_modified];

/* [finances].[load] */
-- TR_finances_load_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_load_modified];

/* [finances].[location_types] */
-- TR_finances_location_types_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_location_types_modified];

/* [finances].[locations] */
-- TR_finances_locations_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_locations_modified];

/* [finances].[sources] */
-- TR_finances_sources_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_sources_modified];

/* [finances].[transactions] */
-- TR_finances_transactions_modified
DROP TRIGGER IF EXISTS [finances].[TR_finances_transactions_modified];

--============================================================--
/* Scalar Function Dropping */

-- ufn_budget_id
DROP FUNCTION IF EXISTS [finances].[ufn_budget_id];

-- ufn_budget_name
DROP FUNCTION IF EXISTS [finances].[ufn_budget_name];

-- ufn_category_id
DROP FUNCTION IF EXISTS [finances].[ufn_category_id];

-- ufn_category_name
DROP FUNCTION IF EXISTS [finances].[ufn_category_name];

-- ufn_location_id
DROP FUNCTION IF EXISTS [finances].[ufn_location_id];

-- ufn_location_name
DROP FUNCTION IF EXISTS [finances].[ufn_location_name];

-- ufn_location_type_id
DROP FUNCTION IF EXISTS [finances].[ufn_location_type_id];

-- ufn_location_type_name
DROP FUNCTION IF EXISTS [finances].[ufn_location_type_name];

-- ufn_source_id
DROP FUNCTION IF EXISTS [finances].[ufn_source_id];

-- ufn_source_name
DROP FUNCTION IF EXISTS [finances].[ufn_source_name];

-- ufn_age
DROP FUNCTION IF EXISTS [jra].[ufn_age];

-- ufn_extract_pattern
DROP FUNCTION IF EXISTS [jra].[ufn_extract_pattern];

-- ufn_gradient_hex
DROP FUNCTION IF EXISTS [jra].[ufn_gradient_hex];

-- ufn_json_formatter
DROP FUNCTION IF EXISTS [jra].[ufn_json_formatter];

--============================================================--
/* Table-Valued Function Dropping */

-- ufn_get_budget
DROP FUNCTION IF EXISTS [finances].[ufn_get_budget];

-- ufn_hexcode_to_rgb
DROP FUNCTION IF EXISTS [jra].[ufn_hexcode_to_rgb];

-- ufn_string_split
DROP FUNCTION IF EXISTS [jra].[ufn_string_split];

--============================================================--
/* View Dropping */

-- v_master_statement
DROP VIEW IF EXISTS [finances].[v_master_statement];

--============================================================--
/* Table Dropping */
DROP TABLE IF EXISTS [finances].[budget_index];
DROP TABLE IF EXISTS [finances].[budgets];
DROP TABLE IF EXISTS [finances].[categories];
DROP TABLE IF EXISTS [finances].[load];
DROP TABLE IF EXISTS [finances].[location_types];
DROP TABLE IF EXISTS [finances].[locations];
DROP TABLE IF EXISTS [finances].[sources];
DROP TABLE IF EXISTS [finances].[transactions];

--============================================================--
/* Schema Creating */

-- finances
IF SCHEMA_ID('finances') IS NULL
	EXEC('CREATE SCHEMA [finances]');

-- jra
IF SCHEMA_ID('jra') IS NULL
	EXEC('CREATE SCHEMA [jra]');

--============================================================--
/* Table Creating */

-- budget_index
IF (OBJECT_ID('[finances].[budget_index]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[budget_index] (
	[id] int NOT NULL IDENTITY(8, 1),
	[name] nvarchar(64) NOT NULL,
	[start_date] date NOT NULL,
	[end_date] date NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- budgets
IF (OBJECT_ID('[finances].[budgets]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[budgets] (
	[id] int NOT NULL IDENTITY(193, 1),
	[index] int NOT NULL,
	[category] int NOT NULL,
	[amount] money NOT NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- categories
IF (OBJECT_ID('[finances].[categories]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[categories] (
	[id] int NOT NULL IDENTITY(44, 1),
	[name] nvarchar(64) NOT NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- load
IF (OBJECT_ID('[finances].[load]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[load] (
	[id] int NOT NULL IDENTITY(17, 1),
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
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- location_types
IF (OBJECT_ID('[finances].[location_types]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[location_types] (
	[id] int NOT NULL IDENTITY(19, 1),
	[name] nvarchar(64) NOT NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- locations
IF (OBJECT_ID('[finances].[locations]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[locations] (
	[id] int NOT NULL IDENTITY(1007, 1),
	[type] int NULL,
	[name] nvarchar(64) NOT NULL,
	[initial] money NOT NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- sources
IF (OBJECT_ID('[finances].[sources]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[sources] (
	[id] int NOT NULL IDENTITY(11, 1),
	[name] nvarchar(128) NOT NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

-- transactions
IF (OBJECT_ID('[finances].[transactions]', 'U') IS NULL)
BEGIN
CREATE TABLE [finances].[transactions] (
	[id] int NOT NULL IDENTITY(2706, 1),
	[currency] nchar(1) NOT NULL,
	[date] date NOT NULL,
	[amount] money NOT NULL,
	[from] int NULL,
	[to] int NULL,
	[category] int NOT NULL,
	[description] nvarchar(256) NOT NULL,
	[source] int NULL,
	[created] datetime NOT NULL,
	[modified] datetime NOT NULL
)
END;

--============================================================--
/* Data Creating */

/* [finances].[budget_index] */
-- budget_index_data
SET IDENTITY_INSERT [finances].[budget_index] ON
INSERT INTO [finances].[budget_index]([id], [name], [start_date], [end_date], [created], [modified])
VALUES ('1', 'Overall', '2017-07-01 00:00:00.000', NULL, '2023-02-26 21:17:28.443', '2023-12-31 16:40:46.363'),
	('2', 'First Year', '2018-07-01 00:00:00.000', '2019-06-30 00:00:00.000', '2023-02-26 21:17:28.443', '2023-12-31 16:40:46.363'),
	('3', 'Second Year', '2019-07-01 00:00:00.000', '2020-06-30 00:00:00.000', '2023-02-26 21:17:28.443', '2023-12-31 16:40:46.363'),
	('4', 'Third Year', '2020-07-01 00:00:00.000', '2021-06-30 00:00:00.000', '2023-02-26 21:17:28.443', '2023-12-31 16:40:46.363'),
	('5', 'Fourth Year', '2021-07-01 00:00:00.000', '2022-06-30 00:00:00.000', '2023-02-26 21:17:28.443', '2023-12-31 16:40:46.363'),
	('6', 'SJD MIS Assistant', '2022-07-01 00:00:00.000', '2023-02-28 00:00:00.000', '2023-04-09 21:14:05.667', '2023-12-31 16:40:46.363'),
	('7', 'House Hunting', '2023-03-01 00:00:00.000', NULL, '2023-04-09 21:14:05.667', '2023-12-31 16:40:46.363')
SET IDENTITY_INSERT [finances].[budget_index] OFF;

/* [finances].[budgets] */
-- budgets_data
SET IDENTITY_INSERT [finances].[budgets] ON
INSERT INTO [finances].[budgets]([id], [index], [category], [amount], [created], [modified])
VALUES ('1', '2', '1', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('2', '2', '2', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('3', '2', '3', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('4', '2', '4', '-180.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('5', '2', '5', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('6', '2', '6', '-2000.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('7', '2', '7', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('8', '2', '9', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('9', '2', '10', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('10', '2', '11', '32.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('11', '2', '13', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('12', '2', '19', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('13', '2', '16', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('14', '2', '17', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('15', '2', '20', '-120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('16', '2', '21', '120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('17', '2', '22', '-200.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('18', '2', '24', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('19', '2', '25', '4000.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('20', '2', '26', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('21', '2', '32', '450.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('22', '2', '29', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('23', '2', '30', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('24', '2', '31', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('25', '2', '14', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('26', '3', '1', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('27', '3', '2', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('28', '3', '3', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('29', '3', '4', '-150.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('30', '3', '5', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('31', '3', '6', '-450.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('32', '3', '7', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('33', '3', '9', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('34', '3', '10', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('35', '3', '11', '60.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('36', '3', '13', '-30.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('37', '3', '19', '500.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('38', '3', '16', '300.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('39', '3', '17', '250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('40', '3', '20', '80.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('41', '3', '21', '120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('42', '3', '22', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('43', '3', '24', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('44', '3', '25', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('45', '3', '26', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('46', '3', '32', '450.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('47', '3', '29', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('48', '3', '30', '-150.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('49', '3', '31', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('50', '3', '14', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('51', '4', '1', '-25.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('52', '4', '2', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('53', '4', '3', '-60.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('54', '4', '4', '-200.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('55', '4', '5', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('56', '4', '6', '-450.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('57', '4', '7', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('58', '4', '9', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('59', '4', '10', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('60', '4', '11', '25.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('61', '4', '13', '-25.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('62', '4', '19', '750.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('63', '4', '16', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('64', '4', '17', '250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('65', '4', '20', '75.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('66', '4', '21', '120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('67', '4', '22', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('68', '4', '24', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('69', '4', '23', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('70', '4', '25', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('71', '4', '26', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('72', '4', '32', '405.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('73', '4', '29', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('74', '4', '30', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('75', '4', '31', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('76', '4', '14', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('77', '5', '1', '-20.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('78', '5', '2', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('79', '5', '3', '-80.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('80', '5', '4', '-300.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('81', '5', '5', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('82', '5', '6', '-650.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('83', '5', '7', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('84', '5', '9', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('85', '5', '10', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('86', '5', '11', '20.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('87', '5', '13', '-20.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('88', '5', '19', '1500.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('89', '5', '16', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('90', '5', '17', '1500.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('91', '5', '20', '-200.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('92', '5', '21', '120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('93', '5', '22', '-350.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('94', '5', '24', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('95', '5', '23', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('96', '5', '25', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('97', '5', '26', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('98', '5', '32', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('99', '5', '29', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('100', '5', '30', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('101', '5', '31', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('102', '5', '14', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('103', '5', '18', '2000.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('104', '1', '1', '-20.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('105', '1', '2', '-50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('106', '1', '3', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('107', '1', '4', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('108', '1', '5', '-100.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('109', '1', '6', '-400.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('110', '1', '7', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('111', '1', '9', '-120.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('112', '1', '10', '-40.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('113', '1', '11', '50.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('114', '1', '13', '-20.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('115', '1', '16', '170.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('116', '1', '17', '70.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('117', '1', '18', '390.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('118', '1', '19', '1380.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('119', '1', '20', '-40.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('120', '1', '21', '110.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('121', '1', '22', '-400.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('122', '1', '23', '0.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('123', '1', '24', '-10.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('124', '1', '25', '170.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('125', '1', '26', '-80.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('126', '1', '29', '1730.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('127', '1', '30', '-250.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('128', '1', '31', '-110.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('129', '1', '32', '330.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('130', '1', '14', '10.00', '2023-02-26 21:53:56.943', '2023-12-31 16:40:46.370'),
	('131', '6', '2', '-30.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('132', '6', '3', '-100.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('133', '6', '4', '250.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('134', '6', '5', '-150.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('135', '6', '6', '-500.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('136', '6', '7', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('137', '6', '8', '-150.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('138', '6', '10', '-40.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('139', '6', '11', '100.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('140', '6', '14', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('141', '6', '16', '500.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('142', '6', '17', '60.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('143', '6', '19', '1000.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('144', '6', '20', '-100.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('145', '6', '21', '50.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('146', '6', '22', '-400.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('147', '6', '23', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('148', '6', '24', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('149', '6', '26', '-50.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('150', '6', '27', '-170.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('151', '6', '28', '-30.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('152', '6', '29', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('153', '6', '30', '-500.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('154', '6', '31', '0.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('155', '6', '36', '9000.00', '2023-04-11 19:26:25.350', '2023-12-31 16:40:46.370'),
	('156', '7', '1', '-20.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('157', '7', '2', '-150.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('158', '7', '3', '-200.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('159', '7', '4', '-500.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('160', '7', '5', '-500.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('161', '7', '6', '-1000.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('162', '7', '7', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('163', '7', '8', '-100.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('164', '7', '10', '-700.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('165', '7', '11', '600.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('166', '7', '12', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('167', '7', '13', '-50.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('168', '7', '14', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('169', '7', '15', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('170', '7', '35', '20000.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('171', '7', '17', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('172', '7', '36', '2000.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('173', '7', '19', '3000.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('174', '7', '20', '-100.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('175', '7', '22', '-750.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('176', '7', '23', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('177', '7', '24', '-1200.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('178', '7', '26', '-50.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('179', '7', '27', '-200.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('180', '7', '28', '-50.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('181', '7', '29', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('182', '7', '30', '-600.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('183', '7', '31', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('184', '7', '0', '0.00', '2023-04-11 21:50:39.323', '2023-12-31 16:40:46.370'),
	('185', '1', '8', '-100.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('186', '1', '12', '0.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('187', '1', '15', '-730.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('188', '1', '35', '290.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('189', '1', '36', '2150.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('190', '1', '27', '-30.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('191', '1', '28', '-30.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370'),
	('192', '1', '0', '0.00', '2023-04-11 22:13:18.820', '2023-12-31 16:40:46.370')
SET IDENTITY_INSERT [finances].[budgets] OFF;

/* [finances].[categories] */
-- categories_data
SET IDENTITY_INSERT [finances].[categories] ON
INSERT INTO [finances].[categories]([id], [name], [created], [modified])
VALUES ('0', 'Unknown', '2023-02-26 20:42:09.713', '2023-12-31 16:40:46.377'),
	('1', 'Cleaning', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('2', 'Clothes', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('3', 'Devices', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('4', 'Entertainment', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('5', 'Experiences', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('6', 'Food', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('7', 'Gifts', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('8', 'Health', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('9', 'Health and Toiletries', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('10', 'Insurance', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('11', 'Interest', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('12', 'Investment', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('13', 'Kitchen', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('14', 'Miscellaneous', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('15', 'Music', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('16', 'Payment (Music)', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('17', 'Payment (Other)', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('18', 'Payment (Supervising)', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('19', 'Payment (Tutoring)', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('20', 'Phone Contract', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('21', 'Pocket Money', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('22', 'Presents', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('23', 'Prizes', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('24', 'Rent', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('25', 'Student Loan', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('26', 'Study', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('27', 'Tax', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('28', 'Toiletries', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('29', 'Transfer', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('30', 'Transport', '2023-02-26 20:51:05.453', '2024-01-10 21:58:44.890'),
	('31', 'Tuition', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('32', 'Uni Support', '2023-02-26 20:51:05.453', '2023-12-31 16:40:46.377'),
	('35', 'Payment (Euler)', '2023-04-05 21:41:28.973', '2023-12-31 16:40:46.377'),
	('36', 'Payment (SJD)', '2023-04-05 21:41:28.973', '2023-12-31 16:40:46.377'),
	('39', 'Legal', '2023-10-19 22:23:08.240', '2023-12-31 16:40:46.377'),
	('40', 'Pool', '2023-10-29 17:09:55.850', '2023-12-31 16:40:46.377')
SET IDENTITY_INSERT [finances].[categories] OFF;

/* [finances].[load] */
-- load_data
SET IDENTITY_INSERT [finances].[load] ON
INSERT INTO [finances].[load]([id], [transaction], [currency], [date], [amount], [from_id], [from_alpha], [to_id], [to_alpha], [category_id], [category_alpha], [description], [source_id], [source_alpha], [load_date], [created], [modified])
VALUES ('15', '2704', '£', '2023-12-01 00:00:00.000', '100.00', '10', 'G&G', '2', 'Wallet', '17', 'Payment (Other)', 'Payment for dogsitting Eric.', '0', 'Unknown', '2024-01-05 17:11:37.820', '2024-01-05 17:11:24.400', '2024-01-05 17:11:37.827'),
	('16', '2705', '£', '2023-12-01 00:00:00.000', '100.00', '10', 'G&G', '2', 'Wallet', '17', 'Payment (Other)', 'Payment for dogsitting Eric.', '0', 'Unknown', '2024-01-05 17:11:37.820', '2024-01-05 17:11:37.817', '2024-01-05 17:11:37.827')
SET IDENTITY_INSERT [finances].[load] OFF;

/* [finances].[location_types] */
-- location_types_data
SET IDENTITY_INSERT [finances].[location_types] ON
INSERT INTO [finances].[location_types]([id], [name], [created], [modified])
VALUES ('1', 'Personal', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('2', 'Family', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('3', 'Friends', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('4', 'Bookstore', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('5', 'Clothier', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('6', 'Cosmetics', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('7', 'Education', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('8', 'Gift Shop', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('9', 'Insurance', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('10', 'Medicine', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('11', 'Restaurant', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('12', 'Service', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('13', 'Supermarket', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('14', 'Tech', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('15', 'Transport', '2023-02-26 20:59:28.923', '2023-12-31 16:40:46.390'),
	('18', 'Joint', '2023-10-19 22:23:08.237', '2023-12-31 16:40:46.390')
SET IDENTITY_INSERT [finances].[location_types] OFF;

/* [finances].[locations] */
-- locations_data
SET IDENTITY_INSERT [finances].[locations] ON
INSERT INTO [finances].[locations]([id], [type], [name], [initial], [created], [modified])
VALUES ('0', NULL, 'Unknown', '0.00', '2023-04-07 13:52:00.057', '2023-12-31 16:40:46.380'),
	('1', '1', 'Barclays Current', '1513.71', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('2', '1', 'Wallet', '-68.30', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('3', '1', 'Wallet (Euros)', '52.16', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('4', '1', 'Santander Student', '143.25', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('5', '1', 'Barclays H2B', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('6', '1', 'NS&I', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('7', '1', 'YBS Savings', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('8', '1', 'Santander eSaver', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('9', '1', 'Santander Monthly Saver', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('10', '2', 'G&G', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('11', '2', 'Parents', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('12', '2', 'Dad', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('13', '2', 'Mum', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('14', '2', 'Paula', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('15', '3', 'Katherine', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('16', '4', 'Waterstone''s', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('17', '4', 'The Works', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('18', '4', 'WHSmith', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('19', '4', 'Bargain Book Department', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('20', '4', 'Audible', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('21', '4', 'Amazon Kindle', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('22', '4', 'WeBuyBooks', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('23', '4', 'Bookingham Palace', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('24', '5', 'Peacocks', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('25', '5', 'Rosebank Sports', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('26', '5', 'ProDirectRunning', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('27', '5', 'GAP Outlet', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('28', '5', 'Skechers', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('29', '5', 'Ede & Ravenscroft', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('30', '5', 'Asda George', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('31', '6', 'Harry''s', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('32', '6', 'Francesco Group', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('33', '6', 'Lloyd''s Barbers', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('34', '6', 'Peaky Barbers', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('35', '6', 'H. Samuel', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('36', '6', 'The Body Shop', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('37', '7', 'University of Warwick', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('38', '7', 'AA Driving School', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('39', '8', 'Family Day Market', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('40', '8', 'OnCampusArt', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('41', '8', 'Flying Tiger', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('42', '8', 'Roman Baths', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('43', '8', 'CardFactory', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('44', '8', 'ParkRun', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('45', '8', 'Warwick University Bookshop', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('46', '8', 'Thorntons', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('47', '8', 'Silverstone Merchandise', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('48', '8', 'MoonPig', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('49', '8', 'The Gift Company', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('50', '8', 'Blakemere Antiques Emporium', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('51', '8', 'Haribo', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('52', '8', 'MenKind', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('53', '9', 'Endsleigh', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('54', '9', 'Allianz', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('55', '9', 'Admiral', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('56', '10', 'Simpkins Dental Practice', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('57', '10', 'Boots', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('58', '10', 'Boots Opticians', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('59', '10', 'Lloyd''s Pharmacy', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('60', '10', 'Beach Road Dental Practice', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('61', '10', 'Specsavers', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('62', '11', 'Sayers', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('63', '11', 'McDonald''s', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('64', '11', 'Bakery in Skipton', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('65', '11', 'Greggs', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('66', '11', 'Tealith', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('67', '11', 'Thirsty Meeples', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('68', '11', 'PoundBakery', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('69', '11', 'Dinky Donuts', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('70', '11', 'Williamsons of Castle', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('71', '11', 'JustEat', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('72', '11', 'NAIC Café', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('73', '11', 'Storyhouse', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('74', '11', 'Woodstone Pizza & Grill', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('75', '12', 'MyTutor', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('76', '12', 'Totum', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('77', '12', 'Circuit Laundry', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('78', '12', 'Post Office', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('79', '12', 'iD Mobile', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('80', '12', 'Avonview Services', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('81', '12', 'Skydome Carpark', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('82', '12', 'GOV.UK', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('83', '12', 'HMRC', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('84', '12', 'DVLA', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('85', '12', 'Premier Inn', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('86', '13', 'Tesco', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('87', '13', 'Marks & Spencer', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('88', '13', 'Asda', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('89', '13', 'Aldi', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('90', '13', 'Wilko', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('91', '13', 'Bargain Buys', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('92', '13', 'Sainsbury''s', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('93', '13', 'Poundland', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('94', '13', 'Quality Save', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('95', '13', 'Morrisons', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('96', '13', 'Rootes Grocery Store', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('97', '13', 'Iceland', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('98', '13', 'Poundstretcher', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('99', '13', 'Waitrose', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('100', '13', 'B&M', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('101', '13', 'Lakes & Dales Coop', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('102', '14', 'CeX', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('103', '14', 'Carphone Warehouse', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('104', '14', 'Currys', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('105', '14', 'MusicMagpie', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('106', '15', 'Johnson''s Excelbus', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('107', '15', 'Virgin Trains', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('108', '15', 'Great Western Railway', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('109', '15', 'Stagecoach', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('110', '15', 'National Express', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('111', '15', 'First Buses', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('112', '15', 'CrossCountry', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('113', '15', 'Northwich Taxis', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('114', '15', 'West Midland Rail and CrossCountry', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('115', '15', 'Arriva', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('116', '15', 'London Northwestern Railway', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('117', '15', 'TfL', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('118', '15', 'Chiltern Railways', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('119', '15', 'Northwestern Railway', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('120', '15', 'Avanti West Coast', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('121', '15', 'The Trainline', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('122', '15', 'Warrington''s Own Buses', '0.00', '2023-02-26 21:10:15.577', '2023-12-31 16:40:46.380'),
	('124', '13', 'Co-op', '0.00', '2023-03-12 22:23:02.543', '2023-12-31 16:40:46.380'),
	('164', NULL, 'Santander', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('165', NULL, 'Warwick Arts Centre', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('166', NULL, 'Amazon', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('167', NULL, 'Friends of Cheshire Youth Orchestra', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('168', NULL, 'Barclays', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('169', NULL, 'Warwick SU', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('170', NULL, 'Myton Hospice', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('171', NULL, 'IKEA', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('172', NULL, 'Google Play', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('173', NULL, 'NCBF', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('174', NULL, 'Student Tenant', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('175', NULL, 'Showcase Cinema', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('176', NULL, 'Warwick Student Cinema', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('177', NULL, 'MacMillan', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('178', NULL, 'Odeon', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('179', NULL, 'JustGiving', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('180', NULL, 'Sweet Emporium', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('181', NULL, 'Nintendo eShop', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('182', NULL, 'Sweet shop', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('183', NULL, 'Steam', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('184', NULL, 'eBay', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('185', NULL, 'Weaverham Newsagents', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('186', NULL, 'Disney Plus', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('187', NULL, 'St Mary''s Lighthouse', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('188', NULL, 'Kickstarter', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('189', NULL, 'Sir John Brunner Foundation', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('190', NULL, 'Leisure Events Limited', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('191', NULL, 'NETGO!', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('192', NULL, 'Nintendo', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('193', NULL, 'UWWO', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('194', NULL, 'Itch.io', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('195', NULL, 'ShopTo', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('196', NULL, 'Rosehill Instruments', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('197', NULL, 'National Trust', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('198', NULL, 'NS&I Prizes', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('199', NULL, 'Argos', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('200', NULL, 'NowTV', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('201', NULL, 'Rotary Club', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('202', NULL, 'Sir John Deane''s College', '0.00', '2023-03-12 23:15:22.337', '2023-12-31 16:40:46.380'),
	('203', '15', 'PayByPhone', '0.00', '2023-03-12 23:44:04.337', '2023-12-31 16:40:46.380'),
	('204', '13', 'Halfords', '0.00', '2023-04-05 21:12:20.983', '2023-12-31 16:40:46.380'),
	('205', NULL, 'Euler', '0.00', '2023-04-05 21:21:52.977', '2023-12-31 16:40:46.380'),
	('206', NULL, 'North Cheshire Wind Orchestra', '0.00', '2023-04-05 21:28:44.717', '2023-12-31 16:40:46.380'),
	('976', '13', 'Lidl', '0.00', '2023-04-16 20:39:19.790', '2023-12-31 16:40:46.380'),
	('977', NULL, 'Yorkshire Building Society', '0.00', '2023-04-16 20:52:17.977', '2023-12-31 16:40:46.380'),
	('980', NULL, 'Homestead Garage', '0.00', '2023-05-03 19:52:17.220', '2023-12-31 16:40:46.380'),
	('981', '3', 'Ali Westmacott', '0.00', '2023-07-24 20:40:23.803', '2023-12-31 16:40:46.380'),
	('982', '2', 'Dan', '0.00', '2023-07-24 20:40:23.803', '2023-12-31 16:40:46.380'),
	('983', '3', 'Schofields', '0.00', '2023-07-24 20:40:23.803', '2023-12-31 16:40:46.380'),
	('984', '3', 'Georgina Hill', '0.00', '2023-07-24 20:40:23.803', '2023-12-31 16:40:46.380'),
	('985', '3', 'Ray Treacher', '0.00', '2023-07-24 20:40:23.803', '2023-12-31 16:40:46.380'),
	('986', '15', 'TwinPay', '0.00', '2023-08-23 09:06:38.603', '2023-12-31 16:40:46.380'),
	('995', '8', 'Dunoon Mugs', '0.00', '2023-10-19 22:23:08.240', '2023-12-31 16:40:46.380'),
	('996', NULL, 'Poole Alcock', '0.00', '2023-10-19 22:23:08.240', '2023-12-31 16:40:46.380'),
	('997', '9', 'Coverwise', '0.00', '2023-10-19 22:23:08.240', '2023-12-31 16:40:46.380'),
	('998', '18', 'Santander Joint', '0.00', '2023-10-19 22:23:08.240', '2023-12-31 16:40:46.380'),
	('1000', '1', 'YBS Regular Saver', '0.00', '2023-10-22 17:08:57.530', '2023-12-31 16:40:46.380'),
	('1001', NULL, 'Delamere Forest', '0.00', '2023-10-22 17:11:59.323', '2023-12-31 16:40:46.380'),
	('1002', '1', 'YBS eSaver', '0.00', '2023-10-29 16:45:06.070', '2023-12-31 16:40:46.380'),
	('1003', '8', 'Maths Gear', '0.00', '2023-11-13 20:57:35.850', '2023-12-31 16:40:46.380'),
	('1004', '11', 'KFC', '0.00', '2023-11-13 21:05:35.223', '2023-12-31 16:40:46.380'),
	('1005', '13', 'OneBeyond', '0.00', '2023-11-21 21:36:17.347', '2023-12-31 16:40:46.380'),
	('1006', '8', 'Onsi', '0.00', '2023-12-28 19:47:03.877', '2023-12-31 16:40:46.380')
SET IDENTITY_INSERT [finances].[locations] OFF;

/* [finances].[sources] */
-- sources_data
SET IDENTITY_INSERT [finances].[sources] ON
INSERT INTO [finances].[sources]([id], [name], [created], [modified])
VALUES ('0', 'Unknown', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('1', 'Receipt', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('2', 'Barclays Mobile Banking App', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('3', 'Barclays Online Banking', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('4', 'Santander Mobile Banking App', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('5', 'Santander Online Banking', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('6', 'YBS Mobile Banking App', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('7', 'YBS Online Banking', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('8', 'NS&I Prize Checker', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('9', 'Email', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397'),
	('10', 'Amazon Order History', '2023-12-31 16:40:46.397', '2023-12-31 16:40:46.397')
SET IDENTITY_INSERT [finances].[sources] OFF;

/* [finances].[transactions] */
-- transactions_data
/* Source table has more than 1000 rows. */;

--============================================================--
/* Default Constraint Creating */

/* [finances].[budget_index] */
-- D_finances_budget_index_created_date
IF (OBJECT_ID('[finances].[D_finances_budget_index_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT D_finances_budget_index_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_budget_index_modified_date
IF (OBJECT_ID('[finances].[D_finances_budget_index_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT D_finances_budget_index_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[budgets] */
-- D_finances_budgets_amount
IF (OBJECT_ID('[finances].[D_finances_budgets_amount]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_amount
	DEFAULT ((0)) FOR [amount]
END;

-- D_finances_budgets_created_date
IF (OBJECT_ID('[finances].[D_finances_budgets_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_budgets_modified_date
IF (OBJECT_ID('[finances].[D_finances_budgets_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT D_finances_budgets_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[categories] */
-- D_finances_categories_created_date
IF (OBJECT_ID('[finances].[D_finances_categories_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[categories]
	ADD CONSTRAINT D_finances_categories_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_categories_modified_date
IF (OBJECT_ID('[finances].[D_finances_categories_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[categories]
	ADD CONSTRAINT D_finances_categories_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[load] */
-- D_finances_load_created_date
IF (OBJECT_ID('[finances].[D_finances_load_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_load_currency
IF (OBJECT_ID('[finances].[D_finances_load_currency]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_currency
	DEFAULT ('£') FOR [currency]
END;

-- D_finances_load_modified_date
IF (OBJECT_ID('[finances].[D_finances_load_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[load]
	ADD CONSTRAINT D_finances_load_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[location_types] */
-- D_finances_location_types_created_date
IF (OBJECT_ID('[finances].[D_finances_location_types_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT D_finances_location_types_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_location_types_modified_date
IF (OBJECT_ID('[finances].[D_finances_location_types_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT D_finances_location_types_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[locations] */
-- D_finances_locations_created_date
IF (OBJECT_ID('[finances].[D_finances_locations_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_locations_initial
IF (OBJECT_ID('[finances].[D_finances_locations_initial]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_initial
	DEFAULT ((0)) FOR [initial]
END;

-- D_finances_locations_modified_date
IF (OBJECT_ID('[finances].[D_finances_locations_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT D_finances_locations_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[sources] */
-- D_finances_sources_created_date
IF (OBJECT_ID('[finances].[D_finances_sources_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[sources]
	ADD CONSTRAINT D_finances_sources_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_sources_modified_date
IF (OBJECT_ID('[finances].[D_finances_sources_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[sources]
	ADD CONSTRAINT D_finances_sources_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

/* [finances].[transactions] */
-- D_finances_transactions_amount
IF (OBJECT_ID('[finances].[D_finances_transactions_amount]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_amount
	DEFAULT ((0)) FOR [amount]
END;

-- D_finances_transactions_category
IF (OBJECT_ID('[finances].[D_finances_transactions_category]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_category
	DEFAULT ((0)) FOR [category]
END;

-- D_finances_transactions_created_date
IF (OBJECT_ID('[finances].[D_finances_transactions_created_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_created_date
	DEFAULT (getutcdate()) FOR [created]
END;

-- D_finances_transactions_currency
IF (OBJECT_ID('[finances].[D_finances_transactions_currency]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_currency
	DEFAULT ('£') FOR [currency]
END;

-- D_finances_transactions_description
IF (OBJECT_ID('[finances].[D_finances_transactions_description]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_description
	DEFAULT ('') FOR [description]
END;

-- D_finances_transactions_modified_date
IF (OBJECT_ID('[finances].[D_finances_transactions_modified_date]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_modified_date
	DEFAULT (getutcdate()) FOR [modified]
END;

-- D_finances_transactions_source
IF (OBJECT_ID('[finances].[D_finances_transactions_source]', 'D') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT D_finances_transactions_source
	DEFAULT ((0)) FOR [source]
END;

--============================================================--
/* Check Constraint Creating */

/* [finances].[transactions] */
-- C_finances_transactions_amount
IF (OBJECT_ID('[finances].[C_finances_transactions_amount]', 'C') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT C_finances_transactions_amount
	CHECK ([amount]>=(0))
END;

-- C_finances_transactions_date
IF (OBJECT_ID('[finances].[C_finances_transactions_date]', 'C') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT C_finances_transactions_date
	CHECK ([date]>='20170701')
END;

--============================================================--
/* Primary Key Creating */

/* [finances].[budget_index] */
-- PK_finances_budget_index_id
IF (OBJECT_ID('[finances].[PK_finances_budget_index_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT PK_finances_budget_index_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[budgets] */
-- PK_finances_budgets_id
IF (OBJECT_ID('[finances].[PK_finances_budgets_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT PK_finances_budgets_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[categories] */
-- PK_finances_categories_id
IF (OBJECT_ID('[finances].[PK_finances_categories_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[categories]
	ADD CONSTRAINT PK_finances_categories_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[load] */
-- PK_finances_load_id
IF (OBJECT_ID('[finances].[PK_finances_load_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[load]
	ADD CONSTRAINT PK_finances_load_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[location_types] */
-- PK_finances_location_types_id
IF (OBJECT_ID('[finances].[PK_finances_location_types_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT PK_finances_location_types_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[locations] */
-- PK_finances_locations_id
IF (OBJECT_ID('[finances].[PK_finances_locations_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT PK_finances_locations_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[sources] */
-- PK_finances_sources_id
IF (OBJECT_ID('[finances].[PK_finances_sources_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[sources]
	ADD CONSTRAINT PK_finances_sources_id
	PRIMARY KEY CLUSTERED([id])
END;

/* [finances].[transactions] */
-- PK_finances_transactions_id
IF (OBJECT_ID('[finances].[PK_finances_transactions_id]', 'PK') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT PK_finances_transactions_id
	PRIMARY KEY CLUSTERED([id])
END;

--============================================================--
/* Unique Constraint Creating */

/* [finances].[budget_index] */
-- UQ_finances_budget_index_name
IF (OBJECT_ID('[finances].[UQ_finances_budget_index_name]', 'UQ') IS NULL)
BEGIN
ALTER TABLE [finances].[budget_index]
	ADD CONSTRAINT UQ_finances_budget_index_name
	UNIQUE NONCLUSTERED([name])
END;

/* [finances].[categories] */
-- UQ_finances_categories_name
IF (OBJECT_ID('[finances].[UQ_finances_categories_name]', 'UQ') IS NULL)
BEGIN
ALTER TABLE [finances].[categories]
	ADD CONSTRAINT UQ_finances_categories_name
	UNIQUE NONCLUSTERED([name])
END;

/* [finances].[location_types] */
-- UQ_finances_location_types_name
IF (OBJECT_ID('[finances].[UQ_finances_location_types_name]', 'UQ') IS NULL)
BEGIN
ALTER TABLE [finances].[location_types]
	ADD CONSTRAINT UQ_finances_location_types_name
	UNIQUE NONCLUSTERED([name])
END;

/* [finances].[locations] */
-- UQ_finances_locations_name
IF (OBJECT_ID('[finances].[UQ_finances_locations_name]', 'UQ') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT UQ_finances_locations_name
	UNIQUE NONCLUSTERED([name])
END;

--============================================================--
/* Foreign Key Creating */

/* [finances].[budgets] */
-- F_finances_budgets_category
IF (OBJECT_ID('[finances].[F_finances_budgets_category]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT F_finances_budgets_category FOREIGN KEY ([category])
	REFERENCES [finances].[categories] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

-- F_finances_budgets_index
IF (OBJECT_ID('[finances].[F_finances_budgets_index]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[budgets]
	ADD CONSTRAINT F_finances_budgets_index FOREIGN KEY ([index])
	REFERENCES [finances].[budget_index] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

/* [finances].[locations] */
-- F_finances_locations_type
IF (OBJECT_ID('[finances].[F_finances_locations_type]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[locations]
	ADD CONSTRAINT F_finances_locations_type FOREIGN KEY ([type])
	REFERENCES [finances].[location_types] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

/* [finances].[transactions] */
-- F_finances_transactions_category
IF (OBJECT_ID('[finances].[F_finances_transactions_category]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_category FOREIGN KEY ([category])
	REFERENCES [finances].[categories] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

-- F_finances_transactions_from
IF (OBJECT_ID('[finances].[F_finances_transactions_from]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_from FOREIGN KEY ([from])
	REFERENCES [finances].[locations] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

-- F_finances_transactions_source
IF (OBJECT_ID('[finances].[F_finances_transactions_source]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_source FOREIGN KEY ([source])
	REFERENCES [finances].[sources] ([id])
	ON DELETE NO ACTION
	ON UPDATE CASCADE
END;

-- F_finances_transactions_to
IF (OBJECT_ID('[finances].[F_finances_transactions_to]', 'F') IS NULL)
BEGIN
ALTER TABLE [finances].[transactions]
	ADD CONSTRAINT F_finances_transactions_to FOREIGN KEY ([to])
	REFERENCES [finances].[locations] ([id])
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
END;

--============================================================--
/* Trigger Creating */

/* [finances].[budget_index] */
-- TR_finances_budget_index_modified
IF (OBJECT_ID('[finances].[TR_finances_budget_index_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_budget_index_modified
ON [finances].[budget_index]
AFTER UPDATE
AS
UPDATE [finances].[budget_index]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[budgets] */
-- TR_finances_budgets_modified
IF (OBJECT_ID('[finances].[TR_finances_budgets_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_budgets_modified
ON [finances].[budgets]
AFTER UPDATE
AS
UPDATE [finances].[budgets]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[categories] */
-- TR_finances_categories_modified
IF (OBJECT_ID('[finances].[TR_finances_categories_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER [finances].[TR_finances_categories_modified]
ON [finances].[categories]
AFTER UPDATE
AS
UPDATE [finances].[categories]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[load] */
-- TR_finances_load_modified
IF (OBJECT_ID('[finances].[TR_finances_load_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_load_modified
ON [finances].[load]
AFTER UPDATE
AS
UPDATE [finances].[load]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[location_types] */
-- TR_finances_location_types_modified
IF (OBJECT_ID('[finances].[TR_finances_location_types_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_location_types_modified
ON [finances].[location_types]
AFTER UPDATE
AS
UPDATE [finances].[location_types]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[locations] */
-- TR_finances_locations_modified
IF (OBJECT_ID('[finances].[TR_finances_locations_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_locations_modified
ON [finances].[locations]
AFTER UPDATE
AS
UPDATE [finances].[locations]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[sources] */
-- TR_finances_sources_modified
IF (OBJECT_ID('[finances].[TR_finances_sources_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_sources_modified
ON [finances].[sources]
AFTER UPDATE
AS
UPDATE [finances].[sources]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

/* [finances].[transactions] */
-- TR_finances_transactions_modified
IF (OBJECT_ID('[finances].[TR_finances_transactions_modified]', 'TR') IS NULL)
BEGIN
EXEC('CREATE TRIGGER TR_finances_transactions_modified
ON [finances].[transactions]
AFTER UPDATE
AS
UPDATE [finances].[transactions]
SET [modified] = GETUTCDATE()
WHERE [id] IN (SELECT [id] FROM inserted)')
END;

--============================================================--
/* Stored Procedure Creating */

-- usp_get_balances
IF (OBJECT_ID('[finances].[usp_get_balances]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [finances].[usp_get_balances](
	@date date = NULL,
	@location_type nvarchar(64) = NULL,
	@location nvarchar(64) = NULL
)
/* 
Version: 1.0
Author: JRA
Date: 2024-07-01

Description: 
Retrieves location balances at given date.

Parameters:
- @date (date): The date to check balances at. Defaults to current date.
- @location_type (nvarchar(64)): The location type of locations to get balances for. Defaults to all.
- @location (nvarchar(64)): The locations to get balances for. Defaults to all.

Prerequisites:
- [finances].[ufn_get_balances]: This function actually gets the data.

Returns:
Balances of locations at the given date.

Usage:
EXECUTE [finances].[ufn_get_balances]
*/
AS
SELECT * FROM [finances].[ufn_get_balances](@date, @location_type, @location) ORDER BY [Account Type Name] ASC, [Account Name] ASC
')
END;

-- usp_get_budget
IF (OBJECT_ID('[finances].[usp_get_budget]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [finances].[usp_get_budget](@budget_name nvarchar(64) = NULL)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description: 
Displays budget data for the requested budget.

Parameters:
- @budget_name (nvarchar(64)): The name of the budget to get data for. Defaults to most recently started budget.

Prerequisites:
- [finances].[ufn_get_budget]: This function actually gets the data.

Returns:
Budget data for the requested budget.

Usage:
EXECUTE [finances].[ufn_get_budget]
*/
AS
SELECT * FROM [finances].[ufn_get_budget](@budget_name) ORDER BY [Budget Name], [Category]
')
END;

-- usp_get_transactions
IF (OBJECT_ID('[finances].[usp_get_transactions]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [finances].[usp_get_transactions](
	@start_date date = ''2017-07-01'',
	@end_date date = NULL,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = NULL,
	@description nvarchar(max) = ''%'',
	@source nvarchar(64) = NULL,
	@created_start datetime = NULL,
	@created_end datetime = NULL,
	@modified_start datetime = NULL,
	@modified_end datetime = NULL
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Gets transactions data with lookups.

Parameters:
- @start_date (date): Earliest transaction date. Defaults to earliest possible.
- @end_date (date): Latest transaction date. Defaults to current date.
- @from (nvarchar(64)): Gets transactions with this sender. Defaults to all.
- @to (nvarchar(64)): Gets transactions with this recipient. Defaults to all.
- @category (nvarchar(64)): Gets transactions under this category. Defaults to all.
- @description (nvarchar(max)): Gets transactions with descriptions like this. Defaults to all.
- @source (nvarchar(64)): Gets transactions with this source. Defaults to all.
- @created_start (datetime): Gets records created after this date. Defaults to all.
- @created_end (datetime): Gets records before after this date. Defaults to all.
- @modified_start (datetime): Gets records modified after this date. Defaults to all.
- @modified_end (datetime): Gets records modified after this date. Defaults to all.

Prerequisites:
- [finances].[ufn_get_transactions]: This function actually retrieves the data.

Returns:
Transactions data with lookups.

Usage:
EXECUTE [finances].[ufn_get_transactions]
*/
AS
SELECT * FROM [finances].[ufn_get_transactions](@start_date, @end_date, @from, @to, @category, @description, @source, @created_start, @created_end, @modified_start, @modified_end) ORDER BY [Date] DESC, [ID] DESC

')
END;

-- usp_load_transactions
IF (OBJECT_ID('[finances].[usp_load_transactions]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [finances].[usp_load_transactions] (
	@commit bit = 0,
	@print bit = 1,
	@delete bit = 0,
	@id int = NULL
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Ingests transactions data from the [finances].[load] table into [finances].[transactions].

Parameters:
- @commit (bit): If true, ingestion is committed.
- @print (bit): If true, debug statements are printed, including a table of possible duplicate transactions.
- @delete (bit): If true, ingested rows are deleted from the load table before ingestion.
- @id (int): This particular load id record is ingested. Defaults to all.

Prerequisites:
- [finances].[ufn_location_id]: Maps location names/ids to ids.
- [finances].[ufn_category_id]: Maps category names/ids to ids.
- [finances].[ufn_source_id]: Maps source names/ids to ids.
- [finances].[ufn_location_name]: Maps location ids to names.
- [finances].[ufn_category_name]: Maps category ids to names.
- [finances].[ufn_source_name]: Maps source ids to names.
- [finances].[ufn_get_transactions]: Reads transactions table with lookups.
- [finances].[ufn_duplicate_transactions]: Checks for possible duplicate transactions.

Returns: 
Load table rows are ingested to transactions if not already done so.

Usage:
EXECUTE [finances].[load_transactions] 0, 1, 0
*/
AS
IF @print = 1 AND @commit = 1
	PRINT(''Committing transactions.'')
ELSE IF @print = 1 AND @commit = 0
	PRINT(''Not committing transactions.'')
ELSE
	SET NOCOUNT ON

BEGIN TRANSACTION

DECLARE @now datetime
SET @now = GETUTCDATE()

IF @delete = 1
	IF @print = 1
		PRINT(''Deleting loaded data from load table.'')
	DELETE FROM [finances].[load] WHERE [load_date] IS NOT NULL

IF @print = 1
	PRINT(''Updating load table item identities from alpha columns.'')
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
	PRINT(''Inserting into transactions table.'')
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
	PRINT(''Updating load table with load date and transaction identity.'')
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
	FROM [finances].[ufn_get_transactions](''2017-07-01'', NULL, NULL, NULL, NULL, ''%'', NULL, @now, @now, @now, @now)
	WHERE [ID] IN (SELECT [transaction] FROM [finances].[load] WHERE [load_date] = @now)
	ORDER BY [Date] DESC, [ID] DESC
END

IF @print = 1
	SELECT * FROM [finances].[ufn_duplicate_transactions](8)

IF @commit = 1
	COMMIT TRANSACTION
ELSE
	ROLLBACK TRANSACTION

')
END;

-- usp_new_transaction
IF (OBJECT_ID('[finances].[usp_new_transaction]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [finances].[usp_new_transaction] (
	@commit bit = 0,
	@print bit = 1,
	@currency nchar(1) = ''£'',
	@amount money,
	@date date,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = 0,
	@description nvarchar(256),
	@source nvarchar(64) = 0
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Loads a record with given data into transactions table.

Parameters:
- @commit (bit): If true, ingestion is committed.
- @print (bit): If true, debug statements are printed.
- @currency (nchar(1)): Currency of transaction. Defaults to ''£''.
- @amount (money): Amount of transaction.
- @date (date): Date of transaction.
- @from (nvarchar(64)): Sender of transaction.
- @to (nvarchar(64)): Recipient of transaction.
- @category (nvarchar(64)): Category of transaction. Defaults to miscellaneous.
- @description (nvarchar(256)): Description of transaction.
- @source (nvarchar(64)): Source of transaction data.

Prerequisites:
- [finances].[usp_load_transactions]: Loads the record.

Returns:
The given transaction data is ingested.

Usage:
EXECUTE [finances].[usp_new_transaction] 0, 1, ''£'', 279.99, ''2017-03-03'', ''Bank Account'', ''Nintendo'', ''Entertainment'', ''Nintendo Switch console.'', ''Mobile Banking App''
*/
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

')
END;

-- usp_print
IF (OBJECT_ID('[jra].[usp_print]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [jra].[usp_print] (@string nvarchar(max))
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Prints large strings to console.

Parameters:
- @string (nvarchar(max)): String to print.

Returns:
Prints the given string.

Usage:
EXECUTE [jra].[usp_print](<long string>)
>>> <long string>
*/
AS

SET @string = REPLACE(@string, CHAR(13), CHAR(10))

DECLARE @working_string nvarchar(max),
	@position int, 
	@newline tinyint

WHILE LEN(@string) > 0
BEGIN
	SET @working_string = SUBSTRING(@string, 1, 4000)
	SET @position = CHARINDEX(CHAR(10), REVERSE(@working_string))
	IF @position = 0
	BEGIN
		SET @position = LEN(@working_string)
		SET @newline = 1
	END
	ELSE
	BEGIN
		SET @position = LEN(@working_string) - @position
		SET @newline = 2
	END
	PRINT(SUBSTRING(@string, 1, @position))
	SET @string = SUBSTRING(@string, @newline + @position, LEN(@string))
END
')
END;

-- usp_select_to_html
IF (OBJECT_ID('[jra].[usp_select_to_html]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [jra].[select_to_html] (
    @query varchar(max),
    @order_by varchar(max) = NULL,
	@out varchar(max) = '''' OUTPUT,
	@sum bit = 0,
    @print bit = 0,
    @display bit = 0
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Takes the output table from a given DQL script and writes it to a HTML table.

Parameters:
- @query (nvarchar(max)): DQL script to get data.
- @order_by (varchar(max)): ORDER BY clause. Defaults to no ordering.
- @out (varchar(max) OUTPUT): Output HTML.
- @sum (bit): If true, aggregates numerical data by summation. Defaults to false.
- @print (bit): If true, prints debug statements. Defaults to false.
- @display (bit): If true, displays outputs. Defaults to false.

Prerequisites:
- [jra].[ufn_gradient_hex]: Calculates gradients for numerical columns.

Returns:
- @out (nvarchar(max) OUTPUT): Output HTML.

Usage:
DECLARE @output nvarchar(max) = ''''
EXECUTE [jra].[usp_select_to_html] @query = ''SELECT * FROM [table]'', @order_by = ''[column1] ASC, [column2] DESC'', @out = @output OUTPUT
*/
AS
IF @print = 0
    SET NOCOUNT ON

BEGIN
    DECLARE @main char(7) = ''#181848'',
        @black char(7) = ''#000000'',
        @white char(7) = ''#ffffff'',
        @dark_accent char(7) = ''#541b8c'',
        @light_accent char(7) = ''#72abe3'',
        @grey char(7) = ''#cfcfcf'',
        @negative char(7) = ''#8c1b1b'',
        @null char(7) = ''#8c8c1b'',
        @positive char(7) = ''#1b8c1b''

    DROP TABLE IF EXISTS [jra].[temp];
    CREATE TABLE [jra].[temp]([R] int) -- Beat the intellisense.

    DECLARE @cmd nvarchar(max) = CONCAT(''
        DROP TABLE IF EXISTS [jra].[temp];
        
        SELECT * 
        INTO [jra].[temp] 
        FROM ('', @query, '') AS [T]
    '')
    IF @print = 1
	    PRINT(@cmd)
    EXEC(@cmd)

    DROP TABLE IF EXISTS #columns;

    SELECT [c].[column_id] AS [c],
        [c].[name] AS [col],
        CASE WHEN [c].[system_type_id] IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127/*, 40, 41, 42, 43, 61, 189*/) THEN [c].[system_type_id]
            ELSE 0
            END AS [numeric],
        CONVERT(float, 0.0) AS [min],
        CONVERT(float, 0.0) AS [max],
		CONVERT(float, 0.0) AS [sum]
    INTO #columns
    FROM [sys].[columns] AS [c]
    WHERE [c].[object_id] = OBJECT_ID(''[jra].[temp]'')
    ORDER BY [c].[column_id]

    ALTER TABLE [jra].[temp]
    ADD [ID] int IDENTITY(0, 1) NOT NULL, [R] int;

    SET @cmd = CONCAT(''
        UPDATE [jra].[temp]
        SET [R] = [T].[R]
        FROM (SELECT (ROW_NUMBER() OVER(ORDER BY '', ISNULL(@order_by, (SELECT TOP(1) ''['' + [col] + '']'' FROM #columns)), '') - 1) AS [R], [ID] FROM [jra].[temp]) AS [T]
        WHERE [jra].[temp].[ID] = [T].[ID]
    '')
    IF @print = 1
	    PRINT(@cmd)
    EXEC(@cmd)

    ALTER TABLE [jra].[temp]
    DROP COLUMN [ID];

    DECLARE @c int,
        @col nvarchar(max),
        @numeric tinyint,
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
        IF @numeric <> 0
        BEGIN
            SET @cmd = CONCAT(''
                UPDATE #columns
                SET [min] = CONVERT(float, (SELECT MIN(['', @col, '']) FROM [jra].[temp])),
                    [max] = CONVERT(float, (SELECT MAX(['', @col, '']) FROM [jra].[temp]))
                WHERE [col] = '''''', @col, ''''''
            '')
            IF @print = 1
			    PRINT(@cmd)
            EXEC(@cmd)
			IF @sum = 1
			BEGIN
				SET @cmd = CONCAT(''
					UPDATE #columns
					SET [sum] = CONVERT(float, (SELECT SUM(['', @col, '']) FROM [jra].[temp]))
					WHERE [col] = '''''', @col, ''''''
				'')
                IF @print = 1
				    PRINT(@cmd)
				EXEC(@cmd)
			END
        END

        FETCH NEXT FROM row_cursor
        INTO @c, @col, @numeric, @min, @max
    END

    DECLARE @separator varchar(100) = CONCAT(''</th>'', CHAR(10), CHAR(9), CHAR(9), ''<th style="background-color:'', @main, '';border:2px solid '', @black, '';text-align:center">'')
	DECLARE @html varchar(max) = ''''
    SET @html += ''<table style="font-size:.9em;font-family:Verdana,Sans-Serif;border:3px solid'' + @black + '';border-collapse:collapse">'' + CHAR(10)
    SET @html += (SELECT 
        CONCAT(
            CHAR(9), ''<tr style="color:'' + @white + ''">'', CHAR(10), 
            CHAR(9), CHAR(9), ''<th style="background-color:'' + @main + '';border:2px solid '' + @black + '';text-align:center">'', STRING_AGG([col], @separator), ''</th>'', CHAR(10), 
            CHAR(9), ''</tr>'', CHAR(10)
        ) 
        FROM #columns
    )

	WHILE @R <= (SELECT MAX([R]) FROM [jra].[temp])
    BEGIN
	    FETCH FIRST FROM row_cursor
        INTO @c, @col, @numeric, @min, @max

        SET @html += CONCAT(CHAR(9), ''<tr>'', CHAR(10))

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @cmd = CONCAT(''
                SELECT @value_char = REPLACE(CONVERT(varchar(max), ['', @col, ''], 21), CHAR(10), ''''''''),
                    @value_float = '', IIF(@numeric <> 0, CONCAT(''CONVERT(float, ['', @col, ''])''), ''NULL''), ''
                FROM [jra].[temp]
                WHERE [R] = '', @R, ''
            '')
            IF @print = 1
			    PRINT(@cmd)
            EXEC sp_executesql @cmd, N''@value_char varchar(max) OUTPUT, @value_float float OUTPUT'', @value_char OUTPUT, @value_float OUTPUT

            IF @c = 1
                SET @html += CONCAT(CHAR(9), CHAR(9), ''<td style="border:2px solid '', @black,'';background-color:'', @dark_accent, '';color:'', @white, '';text-align:center">'', @value_char, ''</td>'', CHAR(10))
            ELSE
            BEGIN
				IF @numeric <> 0
					SET @text_align = ''right''
				ELSE
					SET @text_align = ''center''
				
                IF @numeric <> 0 AND @min < 0
                BEGIN
                    IF @value_float < 0
                        SET @background = [jra].[ufn_gradient_hex](@value_float, @min, 0, @negative, @null)
                    ELSE
                        SET @background = [jra].[ufn_gradient_hex](@value_float, @max, 0, @positive, @null)
                    SET @fontcolour = @white
                END
                ELSE
                BEGIN
                    IF @numeric <> 0
                        SET @background = [jra].[ufn_gradient_hex](@value_float, @min, @max, @white, @light_accent)
                    ELSE IF @R % 2 = 0
                        SET @background = @white
                    ELSE
                        SET @background = @grey
                    SET @fontcolour = @black
                END
                SET @html += CONCAT(CHAR(9), CHAR(9), ''<td style="border:2px solid '', @black,'';background-color:'', @background, '';color:'', @fontcolour, '';text-align:'', @text_align, ''">'', @value_char, ''</td>'', CHAR(10))
            END

            FETCH NEXT FROM row_cursor
            INTO @c, @col, @numeric, @min, @max
        END

        SET @html += CONCAT(CHAR(9), ''</tr>'', CHAR(10))
        SET @R += 1
    END

	IF @sum = 1
	BEGIN
		FETCH FIRST FROM row_cursor
		INTO @c, @col, @numeric, @min, @max

		SET @html += CONCAT(CHAR(9), ''<tr>'', CHAR(10))

		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF @c = 1
				SET @html += CONCAT(CHAR(9), CHAR(9), ''<td style="border:2px solid '', @black,'';background-color:'', @dark_accent, '';color:'', @white, '';text-align:center;font-style:italic">Total</td>'', CHAR(10))
			ELSE IF @numeric <> 0
			BEGIN
				SET @cmd = CONCAT(''
					SELECT @value_char = CONVERT(varchar(max), CONVERT('', (SELECT [name] FROM [sys].[types] WHERE [system_type_id] = @numeric), '', [sum]), 21),
						@value_float = [sum]
					FROM #columns
					WHERE [col] = '''''', @col, ''''''
				'')
                IF @print = 1
				    PRINT(@cmd)
				EXEC sp_executesql @cmd, N''@value_char varchar(max) OUTPUT, @value_float float OUTPUT'', @value_char OUTPUT, @value_float OUTPUT

				IF @min < 0
                BEGIN
                    IF @value_float < 0
                        SET @background = @negative
                    ELSE
                        SET @background = @positive
                    SET @fontcolour = @white
                END
                ELSE
                BEGIN
                    SET @background = @light_accent
                    SET @fontcolour = @black
                END
				SET @html += CONCAT(CHAR(9), CHAR(9), ''<td style="border:2px solid '', @black,'';background-color:'', @background, '';color:'', @fontcolour, '';text-align:right;font-style:italic">'', @value_char, ''</td>'', CHAR(10))
			END
			ELSE
				SET @html += CONCAT(CHAR(9), CHAR(9), ''<td style="border:2px solid '', @black,'';background-color:'', @black, '';color:'', @black, '';text-align:center"></td>'', CHAR(10))

			FETCH NEXT FROM row_cursor
			INTO @c, @col, @numeric, @min, @max
		END
		SET @html += CONCAT(CHAR(9), ''</tr>'', CHAR(10))
	END

    SET @html += ''</table>''

    CLOSE row_cursor
    DEALLOCATE row_cursor

    DROP TABLE IF EXISTS [jra].[temp]
    DROP TABLE IF EXISTS #columns

	SET @out = @html

    IF @print = 1
        PRINT(@out)
    IF @display = 1
        SELECT @out AS [html]

	RETURN;
END
')
END;

-- usp_string_table_split
IF (OBJECT_ID('[jra].[usp_string_table_split]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [jra].[usp_string_table_split] (
    @string nvarchar(max),
    @separator nvarchar(max) = '','',
    @row_separator nvarchar(max) = '';'',
    @header bit = 1,
    @print bit = 1,
    @display bit = 1
)
/* 
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Given a string of delimited data and the delimiters, a tabulated form of the data is created.

Parameters:
- @string (nvarchar(max)): The string of delimited data.
- @separator (nvarchar(max)): The column separator.
- @row_separator (nvarchar(max)): The row separator.
- @header (bit): If true, the first row of the data is used as a header.
- @print (bit): If true, debug prints will be displayed.
- @display (bit): If true, tabular output will be displayed.

Returns:
- ##output (table): Tabulated data from given string.

Usage:
EXECUTE [jra].[usp_string_table_split](''Boss,Location;Massive Moss Charger,Greenpath'', DEFAULT, DEFAULT, DEFAULT, 0, 1)
>>> #======================#===========#
    | Boss                 | Location  |
    #======================#===========#
    | Massive Moss Charger | Greenpath |
    +----------------------+-----------+
*/
AS
IF @print = 0
    SET NOCOUNT ON

DECLARE @cmd nvarchar(max),
    @row nvarchar(max),
    @col_count int,
    @row_count int,
    @c int = 1

SET @row_count = LEN(@string) - LEN(REPLACE(@string, @row_separator, ''''))

IF @row_count = 0
    SET @row = @string
ELSE
    SET @row = SUBSTRING(@string, 1, CHARINDEX(@row_separator, @string, 1) - 1)

SET @col_count = 1 + LEN(@row) - LEN(REPLACE(@row, @separator, ''''))

IF @header = 0
BEGIN
    SET @row = ''''
    WHILE @c <= @col_count
    BEGIN
        SET @row += CONCAT(''Column'', @c, @separator)
        SET @c += 1
    END
END
ELSE
    SET @row += @separator

SET @row = REPLACE(@row, @separator, '' nvarchar(max), '')
SET @row = LEFT(@row, LEN(@row) - 1)

IF 1 = 0
    DROP TABLE IF EXISTS ##output; SELECT '''' AS [ ] INTO ##output -- Beat the intellisense.

SET @cmd = CONCAT(''DROP TABLE IF EXISTS ##output; 
CREATE TABLE ##output ('', @row, '')'')
IF @print = 1
    PRINT(@cmd)
EXEC(@cmd)

SET @cmd = CONCAT(''INSERT INTO ##output
VALUES '', IIF(@header = 1, ''--'', ''''), ''('''''', REPLACE(REPLACE(@string, @separator, '''''', ''''''), @row_separator, CONCAT(''''''), '', CHAR(10), CHAR(9), ''('''''')), '''''')''
)
IF @print = 1
    PRINT(@cmd)
EXEC(@cmd)

IF @display = 1
    SELECT * FROM ##output')
END;

-- usp_timer
IF (OBJECT_ID('[jra].[usp_timer]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [jra].[usp_timer] (
	@Start datetime,
	@Process varchar(128) = ''Process'',
	@End datetime = NULL
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Prints time elapsed during processes.

Parameters:
- @Start (datetime): Input process start time.
- @Process varchar(128): Name of process for display. Defaults to ''Process''.
- @End (datetime): End time of process.

Returns:
Prints time elapsed during processes.

Usage:
DECLARE @start datetime
SET @start = GETDATE()
<script>
EXECUTE [jra].[usp_timer] @start, ''Script''
*/
AS
BEGIN
SET @End = ISNULL(@End, GETDATE())
DECLARE @Length int = DATEDIFF(second, @Start, @End)
DECLARE @Hours int = @Length/3600,
	@Minutes int = (@Length % 3600)/60,
	@Seconds int = @Length % 60
PRINT(CONCAT(@Process, '' started at '', FORMAT(@Start, ''HH:mm:ss''), '' and finished at '', FORMAT(@End, ''HH:mm:ss''), ''.''))
PRINT(CONCAT(''This is a duration of '', 
		IIF(@Hours = 0, '''', CONCAT(@Hours, '' hour'', IIF(@Hours = 1, '', '', ''s, ''))), 
		IIF(@Minutes = 0, '''', CONCAT(@Minutes, '' minute'', IIF(@Minutes = 1, '' and '', ''s and ''))),
			@Seconds, '' second'', IIF(@Seconds = 1, ''.'', ''s.''), CHAR(10)))
END
')
END;

-- usp_transpose_table
IF (OBJECT_ID('[jra].[usp_transpose_table]', 'P') IS NULL)
BEGIN
EXEC('CREATE   PROCEDURE [jra].[usp_transpose_table] (@query varchar(max),
	@output varchar(max) = ''[jra].[output]''
)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Transposes a table defined by a DQR script.

Parameters:
- @query (varchar(max)): DQR script with table to transpose.
- @output (varchar(max)): Name of output table.

Usage:
EXECUTE [jra].[usp_transpose_table] @query = ''SELECT * FROM (VALUES (''''Sapphira'''', ''''Blue''''), (''''Thorn'''', ''''Red''''), (''''Glaedr'''', ''''Gold''''), (''''Firnen'''', ''''Green''''), (''''Shruikan'''', ''''Black'''')) AS [T]([Dragon] nvarchar(max), [Colour] nvarchar(max))'', @output = ''##Transposed''
SELECT * FROM ##Transposed
>>> #========#==========#=======#========#========#==========#
	| Dragon | Sapphira | Thorn | Glaedr | Firnen | Shruikan |
	#========#==========#=======#========#========#==========#
	| Colour | Blue     | Red   | Gold   | Green  | Black    |
	+--------+----------+-------+--------+--------+----------+
*/
AS
BEGIN
	SET NOCOUNT ON

	DROP TABLE IF EXISTS [jra].[temp]
	DROP TABLE IF EXISTS #columns
	DROP TABLE IF EXISTS ##insert

	DECLARE @cmd varchar(max)
	SET @cmd = CONCAT(''
		SELECT *
		INTO [jra].[temp]
		FROM ('', @query, '') AS [T]
	'')
	--PRINT(@cmd)
	EXEC(@cmd)

	DECLARE @c int, @col varchar(max)
	DECLARE cols_cursor SCROLL cursor FOR
	SELECT [column_id] AS [c],
		[name] AS [col]
	FROM sys.columns
	WHERE [object_id] = OBJECT_ID(''[jra].[temp]'')
	ORDER BY [column_id]

	OPEN cols_cursor

	FETCH FIRST FROM cols_cursor
	INTO @c, @col

	SET @cmd = CONCAT(''
		SELECT '', @c, '' AS [c], CONCAT(''''(['''', STRING_AGG(ISNULL(CONVERT(varchar, ['', @col, ''], 21), ''''NULL''''), ''''] varchar(max), [''''), ''''] varchar(max))'''') AS [values]
		INTO ##insert
		FROM [jra].[temp]
	'')

	WHILE @@FETCH_STATUS = 0
	BEGIN
		FETCH NEXT FROM cols_cursor
		INTO @c, @col

		SET @cmd += CONCAT(''
			UNION
			SELECT '', @c, '', CONCAT(''''('''''''''''', STRING_AGG(ISNULL(CONVERT(varchar, ['', @col, ''], 21), ''''NULL''''), '''''''''''', ''''''''''''), '''''''''''')'''')
			FROM [jra].[temp]
		'')
	END
	--PRINT(@cmd)
	EXEC(@cmd)

	SET @cmd = CONCAT(''
		CREATE TABLE '', @output, ''
		'', (SELECT [values] FROM ##insert WHERE [c] = 1)
	)
	--PRINT(@cmd)
	EXEC(@cmd)

	SET @cmd = CONCAT(''
		INSERT INTO '', @output, ''
		--OUTPUT inserted.*
		VALUES '', (SELECT STRING_AGG([values], '', '' + CHAR(10)) FROM ##insert WHERE [c] > 1)
	)
	--PRINT(@cmd)
	EXEC(@cmd)

	CLOSE cols_cursor
	DEALLOCATE cols_cursor

	DROP TABLE IF EXISTS [jra].[temp]
	DROP TABLE IF EXISTS #columns
	DROP TABLE IF EXISTS ##insert
END
')
END;

--============================================================--
/* Scalar Function Creating */

-- ufn_budget_id
IF (OBJECT_ID('[finances].[ufn_budget_id]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_budget_id](@name nvarchar(64))
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps budget names/ids to ids.

Parameters:
- @name (nvarchar(64)): Budget name or id to map.

Returns:
- (int): Identity value of budget.

Usage:
[finances].[ufn_budget_id](''General'')
>>> 1
[finances].[ufn_budget_id](''1'')
>>> 1
[finances].[ufn_budget_id](''Missing'')
>>> NULL
*/
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[budget_index] WHERE [name] = @name), (SELECT [id] FROM [finances].[budget_index] WHERE [id] = CONVERT(int, @name)))
END

')
END;

-- ufn_budget_name
IF (OBJECT_ID('[finances].[ufn_budget_name]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_budget_name](@id int)
RETURNS nvarchar(64)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps budget ids to names.

Parameters:
- @id (int): Budget name or id to map.

Returns:
- (nvarchar(64)): Name of budget.

Usage:
[finances].[ufn_budget_name](1)
>>> ''General''
[finances].[ufn_budget_name](-1)
>>> NULL
*/
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[budget_index] WHERE [id] = @id)
END

')
END;

-- ufn_category_id
IF (OBJECT_ID('[finances].[ufn_category_id]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_category_id](@name nvarchar(64))
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps category names/ids to ids.

Parameters:
- @name (nvarchar(64)): Category name or id to map.

Returns:
- (int): Identity value of category.

Usage:
[finances].[ufn_categories_id](''Entertainment'')
>>> 5
[finances].[ufn_categories_id](''1'')
>>> 1
[finances].[ufn_categories_id](''Missing'')
>>> NULL
*/
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[categories] WHERE [name] = @name), (SELECT [id] FROM [finances].[categories] WHERE [id] = CONVERT(int, @name)))
END

')
END;

-- ufn_category_name
IF (OBJECT_ID('[finances].[ufn_category_name]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_category_name](@id int)
RETURNS nvarchar(64)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps category ids to names.

Parameters:
- @id (int): Category name or id to map.

Returns:
- (nvarchar(64)): Name of category.

Usage:
[finances].[ufn_category_name](5)
>>> ''Entertainment''
[finances].[ufn_category_name](-1)
>>> NULL
*/
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[categories] WHERE [id] = @id)
END

')
END;

-- ufn_location_id
IF (OBJECT_ID('[finances].[ufn_location_id]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_location_id](@name nvarchar(64))
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps location names/ids to ids.

Parameters:
- @name (nvarchar(64)): Location name or id to map.

Returns:
- (int): Identity value of location.

Usage:
[finances].[ufn_location_id](''Entertainment'')
>>> 5
[finances].[ufn_location_id](''1'')
>>> 1
[finances].[ufn_location_id](''Missing'')
>>> NULL
*/
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[locations] WHERE [name] = @name), (SELECT [id] FROM [finances].[locations] WHERE [id] = CONVERT(int, @name)))
END

')
END;

-- ufn_location_name
IF (OBJECT_ID('[finances].[ufn_location_name]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_location_name](@id int)
RETURNS nvarchar(64)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps location ids to names.

Parameters:
- @id (int): Location name or id to map.

Returns:
- (nvarchar(64)): Name of location.

Usage:
[finances].[ufn_location_name](1)
>>> ''Bank Account''
[finances].[ufn_location_name](-1)
>>> NULL
*/
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[locations] WHERE [id] = @id)
END

')
END;

-- ufn_location_type_id
IF (OBJECT_ID('[finances].[ufn_location_type_id]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_location_type_id](@name nvarchar(64))
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps location type names/ids to ids.

Parameters:
- @name (nvarchar(64)): Location type name or id to map.

Returns:
- (int): Identity value of location type.

Usage:
[finances].[ufn_location_type_id](''Personal'')
>>> 1
[finances].[ufn_location_type_id](''1'')
>>> 1
[finances].[ufn_location_type_id](''Missing'')
>>> NULL
*/
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[location_types] WHERE [name] = @name), (SELECT [id] FROM [finances].[location_types] WHERE [id] = CONVERT(int, @name)))
END

')
END;

-- ufn_location_type_name
IF (OBJECT_ID('[finances].[ufn_location_type_name]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_location_type_name](@id int)
RETURNS nvarchar(64)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps location type ids to names.

Parameters:
- @id (int): Location type name or id to map.

Returns:
- (nvarchar(64)): Name of location type.

Usage:
[finances].[ufn_location_type_name](1)
>>> ''Personal''
[finances].[ufn_location_type_name](-1)
>>> NULL
*/
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[location_types] WHERE [id] = @id)
END

')
END;

-- ufn_source_id
IF (OBJECT_ID('[finances].[ufn_source_id]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_source_id](@name nvarchar(64))
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps source names/ids to ids.

Parameters:
- @name (nvarchar(64)): Source name or id to map.

Returns:
- (int): Identity value of source.

Usage:
[finances].[ufn_source_id](''Unknown'')
>>> 0
[finances].[ufn_source_id](''1'')
>>> 1
[finances].[ufn_source_id](''Missing'')
>>> NULL
*/
AS
BEGIN
RETURN COALESCE((SELECT [id] FROM [finances].[sources] WHERE [name] = @name), (SELECT [id] FROM [finances].[sources] WHERE [id] = CONVERT(int, @name)))
END

')
END;

-- ufn_source_name
IF (OBJECT_ID('[finances].[ufn_source_name]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_source_name](@id int)
RETURNS nvarchar(64)
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Maps source ids to names.

Parameters:
- @id (int): source name or id to map.

Returns:
- (nvarchar(64)): Name of source.

Usage:
[finances].[ufn_source_name](0)
>>> ''Unknown''
[finances].[ufn_source_name](-1)
>>> NULL
*/
AS
BEGIN
RETURN (SELECT [name] FROM [finances].[sources] WHERE [id] = @id)
END

')
END;

-- ufn_age
IF (OBJECT_ID('[jra].[ufn_age]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_age](
	@Start date, 
	@End date = NULL
)
RETURNS int
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Calculates the floor of the difference in years between two dates.

Parameters:
- @Start (date): Start date.
- @End (date): End date. Defaults to current date.

Returns:
- (int)

Usage:
[jra].[ufn_age](''1999-10-03 05:45'', ''2024-01-06'')
>>> 24
*/
BEGIN
	RETURN (CAST(FORMAT(ISNULL(@End, GETDATE()), ''yyyyMMdd'') AS int) - CAST(FORMAT(@Start, ''yyyyMMdd'') AS int))/10000
END
')
END;

-- ufn_extract_pattern
IF (OBJECT_ID('[jra].[ufn_extract_pattern]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_extract_pattern] (
    @string nvarchar(max),
    @pattern nvarchar(max) = ''[ABCDEFGHIJKLMNOPQRSTUVWXYZ,0-9]'',
    @pattern_length int = 1
)
RETURNS nvarchar(max)
/* 
Version: 1.1
Author: JRA
Date: 2024-01-06

Description:
Given a string, a pattern and its length, the function removes instances of the pattern from the string.

Parameters:
- @string (nvarchar(max)): The string to extract from.
- @pattern (nvarchar(max)): The pattern to extract. Defaults to uppercase alphanumeric characters.
- @pattern_length (int): The length of the pattern to extract, defaults to single characters.

Returns:
- (nvarchar(max)): The reduced string.

Usage:
[jra].[ufn_extract_pattern](''Execute Order 66'', DEFAULT, DEFAULT)
>>> ''EO66''
*/
AS
BEGIN
    DECLARE @c int = 1
    WHILE @c <= LEN(@string)
    BEGIN  
        IF PATINDEX(@pattern, SUBSTRING(@string, @c, @pattern_length) COLLATE Latin1_General_CS_AI) = 0
        BEGIN
            SET @string = STUFF(@string, @c, @pattern_length, '''')
            CONTINUE
        END
        SET @c += 1
    END
RETURN @string
END')
END;

-- ufn_gradient_hex
IF (OBJECT_ID('[jra].[ufn_gradient_hex]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_gradient_hex] (
	@target float, 
	@lower float, 
	@upper float,
	@hexmin char(7), 
	@hexmax char(7)
)
RETURNS char(7)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Uses linear interpolation to find the hexcode of a value on the gradient between two colours.

Parameters:
- @target (float): The target value.
- @lower (float): The lower bound of the target class.
- @upper (float): The upper bound of the target class.
- @hexmin (char(7)) The hexcode of the lowest colour on the gradient.
- @hexmax (char(7)) The hexcode of the highest colour on the gradient.

Prerequisites:
- [jra].[ufn_hexcode_to_rgb]: Converts input to RGB values for calculations.

Returns:
- @hexcode (char(7)): The target colour.

Usage:
[jra].[gradient_hex](1, 0, 2, ''#000000'', ''#FFFFFF'')
>>> ''#888888''
*/
AS
BEGIN
	IF @lower = @upper
		RETURN @hexmin

	DECLARE @hexcode char(7) = (
		SELECT ''#'' + STRING_AGG(CONVERT(char(2), CONVERT(binary(1), [result], 1), 2), '''')
		FROM (
			SELECT [rgb],
				[lower],
				[upper],
				CONVERT(int, [lower] + ([upper] - [lower])*(@target - @lower)/(@upper - @lower)) AS [result]
			FROM (
				SELECT [lower].[rgb],
					[lower].[value] AS [lower],
					[upper].[value] AS [upper]
				FROM [jra].[hexcode_to_rgb](@hexmin) AS [lower]
					INNER JOIN [jra].[hexcode_to_rgb](@hexmax) AS [upper]
						ON [lower].[rgb] = [upper].[rgb]
			) AS [T]
		) AS [T]
	)
	RETURN @hexcode
END
')
END;

-- ufn_json_formatter
IF (OBJECT_ID('[jra].[ufn_json_formatter]', 'FN') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_json_formatter](@json nvarchar(max))
RETURNS nvarchar(max)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Formats an input JSON string with line breaks and indents.

Parameters:
- @json (nvarchar(max)): Input JSON to format.

Returns:
 - @json (nvarchar(max)): Formatted JSON.

Usage:
[jra].[ufn_json_formatter](''{"game": ["Super", "Mario", "Odyssey"], "moons": 880, "bosses": {"Cascade": "Madame Broode", "Metro": "Mechawiggler", "Ruined": "Lord of Lightning"}}'')
>>> {
		"game": [
			"Super",
			"Mario",
			"Odyssey"
		],
		"moons": 880,
		"bosses": {
			"Cascade": "Madame Broode",
			"Metro": "Mechawiggler",
			"Ruined": "Lord of Lightning"
		}
	}
*/
AS
BEGIN
SET @json = REPLACE(REPLACE(@json, CHAR(10), ''''), CHAR(9), '''')

DECLARE @word nvarchar(256) = '''',
	@string bit = 0,
	@c int = 0,
	@char nvarchar(4) = ''''

DECLARE @jsonwords table (id int, word nvarchar(256))

WHILE @c <= len(@json)
BEGIN
	SET @char = SUBSTRING(@json, @c, 1)
	IF @char = ''"'' AND @string = 0
	BEGIN
		SET @string = 1
		SET @word += @char
	END
	ELSE IF @string = 1
	BEGIN
		SET @word += @char
		IF @char = ''"''
		BEGIN
			SET @string = 0
			INSERT INTO @jsonwords VALUES ((SELECT COUNT(*) FROM @jsonwords) + 1, @word)
			SET @word = ''''
		END
	END
	ELSE IF @char IN (''{'', ''}'', ''['', '']'', '':'', '','')
	BEGIN
		INSERT INTO @jsonwords VALUES ((SELECT COUNT(*) FROM @jsonwords) + 1, @word), ((SELECT COUNT(*) FROM @jsonwords) + 2, @char)
		SET @word = ''''
	END
	ELSE IF @char = '' ''
	BEGIN
		SET @c += 1
		CONTINUE
	END
	ELSE
		SET @word += @char

	SET @c += 1
END

DECLARE @value bit = 0
DECLARE @levels table (id int, level varchar(16))
SET @json = ''''
SET @c = 1

WHILE @c <= (SELECT COUNT(*) FROM @jsonwords)
BEGIN
	SET @word = (SELECT word FROM @jsonwords WHERE id = @c)

	IF LEN(@word) = 0
	BEGIN
		SET @c += 1
		CONTINUE
	END
	ELSE IF @word IN (''{'', ''['')
	BEGIN
		IF @value = 1
		BEGIN
			SET @value = 0
			SET @json += @word
		END
		ELSE
			SET @json += CHAR(10) + REPLICATE(CHAR(9), (SELECT COUNT(*) FROM @levels)) + @word
		IF @word = ''{''
			INSERT INTO @levels VALUES ((SELECT COUNT(*) FROM @levels) + 1, ''object'')
		ELSE
			INSERT INTO @levels VALUES ((SELECT COUNT(*) FROM @levels) + 1, ''array'')
	END
	ELSE IF @word IN (''}'', '']'')
	BEGIN
		SET @value = 0
		DELETE FROM @levels WHERE id = (SELECT COUNT(*) FROM @levels)
		SET @json += CHAR(10) + REPLICATE(CHAR(9), (SELECT COUNT(*) FROM @levels)) + @word
	END
	ELSE IF @word = '':''
	BEGIN
		SET @value = 1
		SET @json += '': ''
	END
	ELSE IF @value = 1 OR @word = '',''
	BEGIN
		SET @value = 0
		SET @json += @word
	END
	ELSE
		SET @json += CHAR(10) + REPLICATE(CHAR(9), (SELECT COUNT(*) FROM @levels)) + @word

	SET @c += 1
END

RETURN LTRIM(@json)
END
')
END;

--============================================================--
/* Inline Table Function Creating */

-- ufn_duplicate_transactions
IF (OBJECT_ID('[finances].[ufn_duplicate_transactions]', 'IF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_duplicate_transactions](@threshold tinyint = 8)
RETURNS TABLE
/*
Version: 1.0
Author: JRA
Date: 2024-07-01

Description:
Checks for potential duplicate transactions.

Parameters:
- @threshold (tinyint): The number of pairwise discrepancies records must exceed to be considered.

Returns:
Pairs of transaction ids that could be duplicates.

Usage:
SELECT * FROM [finances].[ufn_duplicate_transactions](DEFAULT)
>>> #===========#============#
	| Potential | Duplicates |
	#===========#============#
	|         1 |          2 |
	+-----------+------------+
*/
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

')
END;

-- ufn_get_balances
IF (OBJECT_ID('[finances].[ufn_get_balances]', 'IF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_get_balances] (
	@date date = NULL,
	@location_type nvarchar(64) = NULL,
	@location nvarchar(64) = NULL
)
RETURNS table
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description: 
Retrieves balances of given location or location type at a given date.

Parameters:
- @date (date): The date to get balances at. Defaults to current date.
- @location_type (nvarchar(64)): The location type to get balances for. Defaults to all.
- @location (nvarchar(64)): The location to get the balance for. Defaults to all.

Prerequisites:
- [finances].[v_master_statement]: Virtual table that reformats the transactions table to a statement format.
- [finances].[ufn_location_id]: Maps location id to a location name.
- [finances].[ufn_location_type_id]: Maps location type id to a location type name.

Returns: 
Balances of given location or location type at a given date.

Usage:
SELECT * FROM [finances].[ufn_get_balances](DEFAULT, DEFAULT, 1)
>>> #=================#===================#============#==============#=========#============#
	| Account Type ID | Account Type Name | Account ID | Account Name | Balance | Date       |
	#=================#===================#============#==============#=========#============#
	|               1 |          Personal |          1 | Bank Account |  279.99 | 2017-03-03 |
	+-----------------+-------------------+------------+--------------+---------+------------+
*/
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
SELECT [Account Type ID],
	[Account Type Name],
	[Account ID],
	[Account Name],
	[Balance],
	[Date]
FROM [data] WHERE [R] = 1
')
END;

-- ufn_get_transactions
IF (OBJECT_ID('[finances].[ufn_get_transactions]', 'IF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_get_transactions] (
	@start_date date = ''2017-07-01'',
	@end_date date = NULL,
	@from nvarchar(64) = NULL,
	@to nvarchar(64) = NULL,
	@category nvarchar(64) = NULL,
	@description nvarchar(max) = ''%'',
	@source nvarchar(64) = NULL,
	@created_start datetime = NULL,
	@created_end datetime = NULL,
	@modified_start datetime = NULL,
	@modified_end datetime = NULL
)
RETURNS table
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Retrieves transactions data.

Parameters:
- @start_date (date): Earliest transaction date. Defaults to earliest possible.
- @end_date (date): Latest transaction date. Defaults to current date.
- @from (nvarchar(64)): Gets transactions with this sender. Defaults to all.
- @to (nvarchar(64)): Gets transactions with this recipient. Defaults to all.
- @category (nvarchar(64)): Gets transactions under this category. Defaults to all.
- @description (nvarchar(max)): Gets transactions with descriptions like this. Defaults to all.
- @source (nvarchar(64)): Gets transactions with this source. Defaults to all.
- @created_start (datetime): Gets records created after this date. Defaults to all.
- @created_end (datetime): Gets records before after this date. Defaults to all.
- @modified_start (datetime): Gets records modified after this date. Defaults to all.
- @modified_end (datetime): Gets records modified after this date. Defaults to all.

Prerequisites:
- [finances].[ufn_location_id]: Maps location name/id to location id.
- [finances].[ufn_category_id]: Maps category name/id to category id.
- [finances].[ufn_source_id]: Maps source name/id to source id.

Returns:
Transactions data subject to parametric conditions.

Usage:
SELECT * FROM [finances].[ufn_get_transactions](DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)
*/
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
	AND ISNULL([tr].[description], '''') LIKE @description
	AND COALESCE([tr].[source], 0) = COALESCE([finances].[ufn_source_id](@source), [tr].[source], 0)
	AND [tr].[created] BETWEEN ISNULL(@created_start, (SELECT MIN([created]) FROM [finances].[transactions])) AND ISNULL(@created_end, (SELECT MAX([created]) FROM [finances].[transactions]))
	AND [tr].[modified] BETWEEN ISNULL(@modified_start, (SELECT MIN([modified]) FROM [finances].[transactions])) AND ISNULL(@modified_end, (SELECT MAX([modified]) FROM [finances].[transactions]))

')
END;

-- ufn_location_statement
IF (OBJECT_ID('[finances].[ufn_location_statement]', 'IF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_location_statement](
	@location nvarchar(64), 
	@start date = ''2017-07-01'', 
	@end date = NULL
)
RETURNS table
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Provides a statement of transactions for a given location.

Parameters:
- @location (nvarchar(64)): The location to get the statement for.
- @start (date): Earliest date for the transactions. Defaults to the earliest possible date.
- @end (date): Latest date for the transactions. Defaults to current date.

Prerequisites:
- [finances].[ufn_location_id]: Maps location name/id to location id.

Returns:
Statement of transactions for a given location.

Usage:
SELECT * FROM [finances].[ufn_location_statement](1, DEFAULT, DEFAULT)
*/
RETURN
SELECT *
FROM [finances].[v_master_statement]
WHERE [Account ID] = [finances].[ufn_location_id](@location)
	AND [Date] BETWEEN @start AND ISNULL(@end, GETDATE())

')
END;

--============================================================--
/* Table-Valued Function Creating */

-- ufn_get_budget
IF (OBJECT_ID('[finances].[ufn_get_budget]', 'TF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [finances].[ufn_get_budget](@budget_name nvarchar(64))
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
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Displays budget information for a given budget.

Parameters:
- @budget_name (nvarchar(64)): The budget to get data for. Defaults to most recently started budget.

Prequisites:
- [finances].[ufn_budget_id]: Maps a budget name/id to a budget id.
- [finances].[ufn_budget_name]: Maps a budget id to a budget name.
- [finances].[v_master_statement]: Virtual table that reformats transactions data to a statement format.

Returns:
Budget information.

Usage:
SELECT * FROM [finances].[ufn_get_budget](DEFAULT)
*/
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
')
END;

-- ufn_hexcode_to_rgb
IF (OBJECT_ID('[jra].[ufn_hexcode_to_rgb]', 'TF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_hexcode_to_rgb] (@hexcode char(7))
RETURNS @Output TABLE ([rgb] char(1), [value] int)
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Converts a hexcode to RGB values.

Parameters:
- @hexcode (char(7)): Input hexcode to convert.

Returns:
- @Output (table([rgb] char(1), [value] int)): RGB values in tabulated format.

Usage:
[jra].[hexcode_to_rgb](''#181848'')
>>> #=====#=======#
	| rgb | value |
	#=====#=======#
	| r   |    24 |
	+-----+-------+
	| g   |    24 |
	+-----+-------+
	| b   |    72 |
	+-----+-------+
*/
AS
BEGIN
	IF LEN(@hexcode) = 4
		SET @hexcode = ''#'' + SUBSTRING(@hexcode, 2, 1) + SUBSTRING(@hexcode, 2, 1) + SUBSTRING(@hexcode, 3, 1) + SUBSTRING(@hexcode, 3, 1) + SUBSTRING(@hexcode, 4, 1) + SUBSTRING(@hexcode, 4, 1)
	ELSE IF LEN(@hexcode) <> 7
		RETURN

	INSERT INTO @Output
	SELECT *
	FROM (
		VALUES (''r'', CONVERT(int, CONVERT(binary(1), ''0x'' + SUBSTRING(@hexcode, 2, 2), 1))),
			(''g'', CONVERT(int, CONVERT(binary(1), ''0x'' + SUBSTRING(@hexcode, 4, 2), 1))),
			(''b'', CONVERT(int, CONVERT(binary(1), ''0x'' + SUBSTRING(@hexcode, 6, 2), 1)))
	) AS [Output]([rgb], [value])
	RETURN
END
')
END;

-- ufn_string_split
IF (OBJECT_ID('[jra].[ufn_string_split]', 'TF') IS NULL)
BEGIN
EXEC('CREATE   FUNCTION [jra].[ufn_string_split] (
    @string nvarchar(max),
    @separator nvarchar(max) = '',''
)
RETURNS @array table ([value] nvarchar(max))
/*
Version: 1.0
Author: JRA
Date: 2024-01-06

Description:
Splits a string into a tabulated column, with column name [value].

Parameters:
- @string (nvarchar(max)): The delimited string of data to turn into an array.
- @separator (nvarchar(max)): The delimiter. Defaults to '',''.

Returns:
- @array (table): Tabulated data from input string.

Usage:
[jra].[ufn_string_split](''Link,Zelda,Ganondorf'', DEFAULT)
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
        INSERT INTO @array SELECT [value] FROM STRING_SPLIT(@string, @separator)
    ELSE
    BEGIN
        DECLARE @c int = 1,
            @loc int
        SET @string += @separator
        SET @loc = CHARINDEX(@separator, @string, @c)
        WHILE @loc > 0
        BEGIN
            INSERT INTO @array
            VALUES (SUBSTRING(@string, @c, @loc - @c));
            SET @c = @loc + LEN(@separator)
            SET @loc = CHARINDEX(@separator, @string, @c)
        END
    END
    RETURN;
END
')
END;

--============================================================--
/* View Creating */

-- v_master_statement
IF (OBJECT_ID('[finances].[v_master_statement]', 'V') IS NULL)
BEGIN
EXEC('CREATE   VIEW [finances].[v_master_statement]
/*
Version: 1.0
Author: JRA
Date: 2024-01-07

Description:
Virtual table that presents transactional data in the form of a statement.

Prerequisites:
- [finances].[ufn_get_transactions]: Reformats the transactions table with lookups.

Returns:
- [finances].[v_master_statement]: Virtual table that presents transactional data in the form of a statement.

Usage:
SELECT * FROM [finances].[v_master_statement] WHERE [Account Type ID] IN (1, 18)
*/
AS
WITH [raw] AS (
	SELECT * FROM [finances].[ufn_get_transactions](''2017-07-01'', NULL, NULL, NULL, NULL, ''%'', NULL, NULL, NULL, NULL, NULL)
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

')
END;

END