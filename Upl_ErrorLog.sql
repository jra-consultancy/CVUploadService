IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Upl_ErrorLog]') AND type in (N'U'))
DROP TABLE [dbo].[Upl_ErrorLog]
GO
GO

CREATE TABLE [dbo].[Upl_ErrorLog](
	[ErrorLogId] [int] IDENTITY(1,1) NOT NULL,
	[ErrorMsg] [varchar](max) NULL,
	[Even] [varchar](250) NULL,
	[ErrorDate] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

