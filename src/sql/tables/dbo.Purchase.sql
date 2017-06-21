CREATE TABLE [dbo].[Purchase]
(
	[datetime] [datetime] NULL,
	[customerID] [varchar](50) NULL,
	[ProductID] [varchar](50) NULL,
	[amount_spent] [numeric](18, 0) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
