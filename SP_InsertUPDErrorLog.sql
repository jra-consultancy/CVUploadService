SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[SP_InsertUPDErrorLog]
    @ErrorMsg nvarchar(max),
	@Even nvarchar(255)
AS
BEGIN
INSERT INTO dbo.UPD_ErrorLog
(
    ErrorMsg,
    Even,
    ErrorDate
)
VALUES
(   @ErrorMsg, -- ErrorMsg - varchar(max)
    @Even, -- Even - varchar(250)
    GETDATE()  -- ErrorDate - varchar(520)
    );
END
GO

