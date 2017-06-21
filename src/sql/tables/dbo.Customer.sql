CREATE TABLE [dbo].[Customer]
(
	[customerID] [varchar](50) NULL,
	[age] [numeric](18, 0) NULL,
	[Gender] [varchar](50) NULL,
	[Income] [varchar](50) NULL,
	[HeadofHousehold] [varchar](50) NULL,
	[number_household] [numeric](18, 0) NULL,
	[months_residence] [numeric](18, 0) NULL
) WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
