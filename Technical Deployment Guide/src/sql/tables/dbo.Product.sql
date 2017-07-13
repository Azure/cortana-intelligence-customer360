CREATE TABLE [dbo].[Product]
(
	[ProductID] [varchar](50) NULL,
	[Product_description] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
