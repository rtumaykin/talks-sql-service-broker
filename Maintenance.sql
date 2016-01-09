-- preparing the database from scratch
USE master;
GO
IF (DB_ID(N'SoCalCodeCamp2015') IS NOT NULL)
	DROP DATABASE SoCalCodeCamp2015;
GO
CREATE DATABASE SoCalCodeCamp2015;
GO
ALTER DATABASE SoCalCodeCamp2015 SET ENABLE_BROKER;
GO
USE SoCalCodeCamp2015;
GO
-- create necessary Service Broker objects
CREATE SCHEMA maintenance;
GO
CREATE CONTRACT [http://CodeCamp2015/JobSchedulerContract] (
[http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer] SENT BY INITIATOR);
GO
CREATE QUEUE [maintenance].[JobsQueue] WITH STATUS = ON; 
GO
CREATE SERVICE [http://CodeCamp2015/JobScheduler] AUTHORIZATION [dbo] 
ON QUEUE [maintenance].[JobsQueue] ([http://CodeCamp2015/JobSchedulerContract]);
GO
-- Create tables to keep the jobs definition and job history
CREATE TABLE [maintenance].[Job] (
	JobId uniqueidentifier NOT NULL,
	SqlStatement nvarchar(max) NOT NULL,
	RunPeriodInSeconds int NOT NULL,
	CONSTRAINT PK_Job PRIMARY KEY ([JobId])
);
GO
CREATE TABLE [maintenance].[JobLog] (
	JobId uniqueidentifier NOT NULL,
	JobLogId int NOT NULL IDENTITY (1, 1),
	ExecuteTime datetimeoffset NOT NULL,
	ErrorNumber int NULL,
	ErrorMessage nvarchar(max) NULL,
	CONSTRAINT PK_JobLog PRIMARY KEY CLUSTERED ([JobId], [JobLogId]),
	CONSTRAINT FK_JobLog_Job FOREIGN KEY ([JobId]) REFERENCES [maintenance].[Job] ([JobId])
);
GO
-- procedure to execute the scheduled task
CREATE PROCEDURE [maintenance].[RunJob]
AS
SET NOCOUNT ON;
DECLARE 
	@message_type_name nvarchar(max),
	@conversation_handle uniqueidentifier,
	@SqlStatement nvarchar(max),
	@RunPeriodInSeconds int;

BEGIN TRANSACTION;
BEGIN TRY;
	RECEIVE TOP (1) 
		@message_type_name = [message_type_name],
		@conversation_handle = [conversation_handle]
	FROM maintenance.JobsQueue;

	IF (@message_type_name = N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer')
	BEGIN;
		SELECT	@SqlStatement = [SqlStatement],
				@RunPeriodInSeconds = [RunPeriodInSeconds]
		FROM [maintenance].[Job]
		WHERE [JobId] = @conversation_handle;

		BEGIN CONVERSATION TIMER (@conversation_handle) TIMEOUT = @RunPeriodInSeconds;
	END;

	IF (@message_type_name = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
	BEGIN;
		END CONVERSATION (@conversation_handle);
	END;

	COMMIT TRANSACTION;

	-- can't execute backups inside of a transaction
	IF (@SqlStatement IS NOT NULL)
	BEGIN;
		EXEC [sys].[sp_executesql] @stmt = @SqlStatement;

		INSERT [maintenance].[JobLog] (
				[JobId],
				[ExecuteTime],
				[ErrorNumber],
				[ErrorMessage]
			)
		VALUES	(@conversation_handle, SYSDATETIMEOFFSET(), NULL, NULL);
	END;
END TRY BEGIN CATCH;
	IF (XACT_STATE() = -1)
		ROLLBACK;

	DECLARE 
		@ErrorNumber int = ERROR_NUMBER(),
		@ErrorMessage nvarchar(max) = ERROR_MESSAGE();

	INSERT [maintenance].[JobLog] (
			[JobId],
			[ExecuteTime],
			[ErrorNumber],
			[ErrorMessage]
		)
	VALUES	(@conversation_handle, SYSDATETIMEOFFSET(), @ErrorNumber, @ErrorMessage);

	IF (XACT_STATE() = 1)
		COMMIT;

END CATCH;
GO
-- procedure to create new scheduled job
CREATE PROCEDURE [maintenance].[AddJob]
	@SqlStatement nvarchar(max),
	@RunPeriodInSeconds int
AS
SET NOCOUNT ON;

DECLARE @conversation_handle uniqueidentifier;

BEGIN TRANSACTION;
BEGIN TRY;
	BEGIN DIALOG CONVERSATION @conversation_handle
	FROM SERVICE [http://CodeCamp2015/JobScheduler]
	TO SERVICE N'http://CodeCamp2015/JobScheduler', 'CURRENT DATABASE'
	ON CONTRACT [http://CodeCamp2015/JobSchedulerContract]
	WITH ENCRYPTION = OFF;

	BEGIN CONVERSATION TIMER (@conversation_handle) TIMEOUT = @RunPeriodInSeconds;

	INSERT [maintenance].[Job] (
				[JobId],
				[SqlStatement],
				[RunPeriodInSeconds]
			)
	VALUES	(@conversation_handle, @SqlStatement, @RunPeriodInSeconds);

	COMMIT TRANSACTION;
END TRY BEGIN CATCH;
	IF (XACT_STATE() != 0)
		ROLLBACK TRANSACTION;

	THROW;
END CATCH;
GO
-- procedure to delete the scheduled job
CREATE PROCEDURE [maintenance].DeleteJob
	@JobId uniqueidentifier
AS
SET NOCOUNT ON;
BEGIN TRANSACTION;
BEGIN TRY;
	END CONVERSATION (@JobId);

	INSERT [maintenance].[JobLog]
			(
				[JobId],
				[ExecuteTime],
				[ErrorNumber],
				[ErrorMessage]
			)
	VALUES	(@JobId, SYSDATETIMEOFFSET(), NULL, 'Job Terminated at a user request');

	COMMIT TRANSACTION;
END TRY BEGIN CATCH;
	IF (XACT_STATE() != 0)
		ROLLBACK;

	DECLARE 
		@ErrorNumber int = ERROR_NUMBER(),
		@ErrorMessage nvarchar(max) = 'Job Termination failed with message: ' + ERROR_MESSAGE();

	INSERT [maintenance].[JobLog]
			(
				[JobId],
				[ExecuteTime],
				[ErrorNumber],
				[ErrorMessage]
			)
	VALUES	(@JobId, SYSDATETIMEOFFSET(), @ErrorNumber, @ErrorMessage);
END CATCH;
GO
-- replace the backup path with the one that exists in your environment
EXEC [maintenance].[AddJob]
	@SqlStatement = '
DECLARE @BackupFilePath nvarchar(max) = 
	N''D:\SQLDATA\MSSQL12.MSSQLSERVER\MSSQL\Backup\SoCalCodeCamp2015_'' + 
	CONVERT(nvarchar(max), GETDATE(), 112) + N''_'' + 
	REPLACE(CONVERT(nvarchar(max), GETDATE(), 108), '':'', '''');
	 
BACKUP DATABASE SoCalCodeCamp2015 TO DISK=@backupFilePath;',
	@RunPeriodInSeconds = 10;
GO
-- this is to demonstrate what is in the queue tables
SELECT * FROM [maintenance].[JobsQueue];
GO
SELECT * FROM [maintenance].[Job]
GO
SELECT * FROM [maintenance].[JobLog] ORDER BY [ExecuteTime] DESC;
GO
SELECT * FROM sys.[conversation_endpoints]
GO
ALTER QUEUE [maintenance].[JobsQueue] WITH STATUS = ON, ACTIVATION (
STATUS=ON, PROCEDURE_NAME = [maintenance].[RunJob], MAX_QUEUE_READERS = 20, EXECUTE AS 'dbo' ); 
GO

-- execute this part to stop the job after you are done.
/*
DECLARE @JobId uniqueidentifier;
SELECT TOP 1 @JobId = [JobId]
FROM [maintenance].[JobLog]
ORDER BY [ExecuteTime] DESC;

EXEC [maintenance].[DeleteJob] @JobId = @JobId;
*/


	
