-- prep
USE master;
GO
IF (DB_ID(N'SoCalCodeCamp2015') IS NOT NULL)
	DROP DATABASE SoCalCodeCamp2015;
GO
-- create a partial database so that we don't pollute the server with users. 
-- Please note that this will only work with SQL 2012. With earlier versions you
-- will need to create a standard user in master database and grant permissions to the 
-- SoCalCodeCamp2015 database
CREATE DATABASE SoCalCodeCamp2015 CONTAINMENT=PARTIAL;
GO
ALTER DATABASE SoCalCodeCamp2015 SET ENABLE_BROKER;
GO
USE SoCalCodeCamp2015;
GO
CREATE USER foobaruser WITH PASSWORD = 'foobar123#';
GO 
-- service broker
CREATE MESSAGE TYPE [foo] VALIDATION=WELL_FORMED_XML;
GO
CREATE MESSAGE TYPE [bar] VALIDATION=WELL_FORMED_XML;
GO
CREATE CONTRACT [foobar] (
	[foo] SENT BY INITIATOR,
	[bar] SENT BY TARGET
);
GO
CREATE QUEUE foobarQueue WITH STATUS=ON, RETENTION=OFF;
GO
CREATE QUEUE barfooQueue WITH STATUS=ON,RETENTION=OFF;
GO
GRANT RECEIVE ON barfooqueue TO [foobaruser];
GO
CREATE SERVICE foobarservice ON QUEUE [dbo].[foobarQueue] ([foobar]);
GO
CREATE SERVICE barfooservice ON QUEUE [dbo].[barfooQueue] ([foobar]);
GO




IF (OBJECT_ID(N'fooActivator') IS NOT NULL)
	DROP PROC fooActivator;
GO
CREATE PROCEDURE fooActivator 
AS
SET NOCOUNT ON;
DECLARE @tr sysname = CONVERT(sysname, NEWID());

DECLARE
	@ConversationHandle uniqueidentifier,
	@MessageTypeName nvarchar(200),
	@MessageBody varbinary(max),
	@Message xml,
	@Reply xml;

BEGIN TRAN;
SAVE TRAN @tr;

BEGIN TRY;
	WAITFOR (
		RECEIVE TOP (1)
			@ConversationHandle = conversation_handle,
			@MessageTypeName = message_type_name,
			@MessageBody = message_body
		FROM [dbo].[barfooQueue]
	), TIMEOUT 2000;

	IF (@ConversationHandle IS NOT NULL)
	BEGIN;
		IF (@MessageTypeName = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
			END CONVERSATION @ConversationHandle;
		ELSE  BEGIN;
			SET @Message = CONVERT(xml, @MessageBody);
			SET @Reply = N'<reply>' + CONVERT(nvarchar(MAX), @Message.value('(/message)[1]', 'int') * 100) + N'</reply>';

			--RAISERROR (@MessageTypeName, 10, 1) WITH LOG;

			SEND ON CONVERSATION @ConversationHandle MESSAGE TYPE [bar] (@Reply);
		END;
	END;

	COMMIT TRAN;
END TRY BEGIN CATCH;
	IF (XACT_STATE() = 1)
	BEGIN;
		ROLLBACK TRANSACTION @tr;
		COMMIT TRANSACTION;
	END;
	ELSE IF (XACT_STATE() = -1)
	BEGIN;
		ROLLBACK TRANSACTION;
	END;
	THROW;
	RETURN;
END CATCH;
GO
-- this sets the internal activation procedure on this queue
ALTER QUEUE [dbo].[barfooQueue] WITH ACTIVATION (EXECUTE AS OWNER, MAX_QUEUE_READERS=10, STATUS=ON, PROCEDURE_NAME=[dbo].[fooActivator]), RETENTION=OFF, STATUS=ON;
GO
IF (OBJECT_ID(N'foobarSend') IS NOT NULL)
	DROP PROC foobarSend;
GO
CREATE PROCEDURE foobarSend
AS
SET NOCOUNT ON;
DECLARE @tr sysname = CONVERT(sysname, NEWID());

DECLARE
	@ConversationHandle uniqueidentifier,
	@MessageTypeName nvarchar(200),
	@MessageBody varbinary(max),
	@Message xml,
	@cnt int = 0;

WHILE (@cnt < 1000)
BEGIN;
	BEGIN TRAN;
	SAVE TRAN @tr;

	BEGIN TRY;
		BEGIN DIALOG CONVERSATION @ConversationHandle
		FROM SERVICE [foobarservice]
		TO SERVICE N'barfooservice'
		ON CONTRACT [foobar]
		WITH ENCRYPTION=OFF;

		SET @Message = N'<message>' + CONVERT(nvarchar(max), @cnt) + N'</message>';

		PRINT CONVERT(nvarchar(MAX), @Message);

		SEND ON CONVERSATION @ConversationHandle MESSAGE TYPE [foo] (@Message);

		COMMIT TRAN;
	END TRY BEGIN CATCH;
		IF (XACT_STATE() = 1)
		BEGIN;
			ROLLBACK TRANSACTION @tr;
			COMMIT TRANSACTION;
		END;
		ELSE IF (XACT_STATE() = -1)
		BEGIN;
			ROLLBACK TRANSACTION;
		END;
		THROW;
		RETURN;
	END CATCH;

	SET @cnt += 1;
END;
GO
IF (OBJECT_ID(N'foobarReceiveReply') IS NOT NULL)
	DROP PROC [dbo].[foobarReceiveReply];
GO
CREATE PROCEDURE foobarReceiveReply
AS
SET NOCOUNT ON;
DECLARE @tr sysname = CONVERT(sysname, NEWID());

DECLARE
	@ConversationHandle uniqueidentifier,
	@MessageTypeName nvarchar(200),
	@MessageBody varbinary(max),
	@Message xml,
	@cnt int = 0,
	@result nvarchar(100),
	@dataHasBeenReceived bit = 0,
	@shouldExit bit = 0;

WHILE (@shouldExit = 0)
BEGIN;
	SET @tr = CONVERT(sysname, NEWID());

	BEGIN TRAN;
	SAVE TRAN @tr;

	BEGIN TRY;
		SELECT
			@ConversationHandle = NULL,
			@MessageTypeName = NULL,
			@MessageBody =  NULL;
		WAITFOR (
			RECEIVE TOP (1)
				@ConversationHandle = conversation_handle,
				@MessageTypeName = message_type_name,
				@MessageBody = message_body
			FROM foobarQueue
		), TIMEOUT 2000;

		IF (@ConversationHandle IS NULL)
		BEGIN;
			IF (@dataHasBeenReceived = 1)
				SET @shouldExit = 1;
			ELSE
				RAISERROR ('No messages have been received in the past 2 seconds', 0, 1) WITH NOWAIT;
		END;

		IF (@MessageTypeName = N'bar')
		BEGIN;
			SET @dataHasBeenReceived = 1;

			SET @Message = CONVERT(xml, @MessageBody);

			SET @result = @Message.value('(/reply)[1]', 'varchar(100)');

			RAISERROR(@result, 0, 1) WITH NOWAIT;

			END CONVERSATION @ConversationHandle;
		END;

		COMMIT TRAN;
	END TRY BEGIN CATCH;
		IF (XACT_STATE() = 1)
		BEGIN;
			ROLLBACK TRANSACTION @tr;
			COMMIT TRANSACTION;
		END;
		ELSE IF (XACT_STATE() = -1)
		BEGIN;
			ROLLBACK TRANSACTION;
		END;
		PRINT ERROR_MESSAGE();
		THROW;
		RETURN;
	END CATCH;
END;


GO


