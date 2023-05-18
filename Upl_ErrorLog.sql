IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Upl_ErrorLog]') AND type IN (N'U'))
DROP TABLE [dbo].[Upl_ErrorLog]
GO
GO

CREATE TABLE [dbo].[Upl_ErrorLog](
	[ErrorLogId] [INT] IDENTITY(1,1) NOT NULL,
	[ErrorMsg] [VARCHAR](MAX) NULL,
	[Event] [VARCHAR](250) NULL,
	[ErrorDate] [DATETIME] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

