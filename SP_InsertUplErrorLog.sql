IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SP_InsertUplErrorLog]') AND type in (N'P'))
DROP PROCEDURE [dbo].[SP_InsertUplErrorLog]
GO

CREATE PROCEDURE [dbo].[SP_InsertUplErrorLog]
    @ErrorMsg nvarchar(max),
	@Even nvarchar(255)
AS
BEGIN
INSERT INTO dbo.Upl_ErrorLog
(
    ErrorMsg,
    Event,
    ErrorDate
)
VALUES
(   @ErrorMsg, -- ErrorMsg - varchar(max)
    @Even, -- Even - varchar(250)
    GETDATE()  -- ErrorDate - varchar(520)
    );
END
GO

