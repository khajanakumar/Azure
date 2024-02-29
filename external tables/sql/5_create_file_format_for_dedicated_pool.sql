/* External table for Dedicated server pool */
/* Please note the TYPE = HADOOP */
/* when reverse engineered  from built in Serverless pool type will be native */
/* Native will not scan thru files in sub folders */


IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'dp203synapseprimarycontainer_dp203synapsecoursedl_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [dp203synapseprimarycontainer_dp203synapsecoursedl_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://dp203synapseprimarycontainer@<storageaccountname>.dfs.core.windows.net', 
		TYPE = HADOOP 
	)
GO

CREATE EXTERNAL TABLE dbo.employee_ext_99999 (
	[Emp_id] bigint,
	[Name] nvarchar(4000),
	[Gender] nvarchar(4000),
	[Salary] bigint,
	[Dept_id] bigint
	)
	WITH (
	LOCATION = 'employee.csv',
	DATA_SOURCE = [dp203synapseprimarycontainer_<storageaccountname>_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.employee_ext_99999
GO