#load "Inputs.csx"

public class Assets {
  const string EnrichedProfileBlob = "/data/final_enriched_profiles/enriched_customer_profile.csv";
  const string ProductBlob = "/data/product.csv";
  const string PurchaseBlob = "/data/purchase.csv";
  const string CustomerBlob = "/data/customer.csv";

  // tuple: <name, destContainer, destPath>
  public Tuple<string, string, string>[] CopyBlobs { get; private set; }
  public string[] ExecSql { get; private set; }
  public string[] HdfsFolders { get; private set; }

  public Assets(Inputs inputs) {
    this.CopyBlobs = new []
    {
      Tuple.Create("etl_and_feature_engineering.py", inputs.HdiContainer, "scripts"),
      Tuple.Create("Scoring.R", inputs.HdiContainer, "scripts"),
      Tuple.Create("com.adf.appsample.jar", inputs.HdiContainer, "scripts"),
      Tuple.Create("com.adf.adfjobonhdi.jar", inputs.HdiContainer, "scripts"),
      Tuple.Create("enriched_customer_profile.csv", inputs.HdiContainer, "data/final_enriched_profiles"),
      Tuple.Create("product.csv", inputs.HdiContainer, "data"),
      Tuple.Create("purchase.csv", inputs.HdiContainer, "data"),
      Tuple.Create("customer.csv", inputs.HdiContainer, "data"),
      Tuple.Create("Tree1", inputs.HdiContainer, String.Empty),
    };

    this.HdfsFolders = new []
    {
      "data/final_enriched_profiles",
    };

    this.ExecSql = new []
    {
      $@"
IF NOT EXISTS ( SELECT * FROM sys.symmetric_keys WHERE symmetric_key_id = 101 )
BEGIN
  CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{inputs.Password}';
END",

      $@"
IF NOT EXISTS ( SELECT * FROM sys.database_credentials WHERE name = 'BlobStorageCredential' )
BEGIN
  CREATE DATABASE SCOPED CREDENTIAL BlobStorageCredential WITH 
    Identity = '{inputs.Username}',
    Secret = '{inputs.StorageKey}'
END",

      $@"
IF NOT EXISTS (	SELECT * FROM sys.external_data_sources WHERE name = 'AzureBlob' )
BEGIN
  CREATE EXTERNAL DATA SOURCE [AzureBlob] WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://{inputs.HdiContainer}@{inputs.Storage}.blob.core.windows.net',
    CREDENTIAL = [BlobStorageCredential]
  )
END",

      @"
IF NOT EXISTS (	SELECT * FROM sys.external_file_formats WHERE name = 'CSVFormat' )
BEGIN
  CREATE EXTERNAL FILE FORMAT [CSVFormat] WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
      FIELD_TERMINATOR = ',',
      STRING_DELIMITER = '""',
      USE_TYPE_DEFAULT = True
    )
  )
END",
      $@"
IF NOT EXISTS (	SELECT * FROM sys.external_tables WHERE name = 'Enriched_Customer_Profile_Blob' )
BEGIN
  CREATE EXTERNAL TABLE [dbo].[Enriched_Customer_Profile_Blob]
  (
    [customerid] [varchar](10) NULL,
    [age] [varchar](10) NULL,
    [gender] [varchar](10) NULL,
    [income] [varchar](100) NULL,
    [headofhousehold] [varchar](20) NULL,
    [number_household] [varchar](10) NULL,
    [months_residence] [varchar](10) NULL,
    [t1count_30d] [varchar](100) NULL,
    [t2count_30d] [varchar](100) NULL,
    [t3count_30d] [numeric](18, 0) NULL,
    [t1spend_30d] [numeric](18, 0) NULL,
    [t2spend_30d] [numeric](18, 0) NULL,
    [t3spend_30d] [numeric](18, 0) NULL,
    [t1count_10d] [numeric](18, 0) NULL,
    [t2count_10d] [numeric](18, 0) NULL,
    [t3count_10d] [numeric](18, 0) NULL,
    [t1spend_10d] [numeric](18, 0) NULL,
    [t2spend_10d] [numeric](18, 0) NULL,
    [t3spend_10d] [numeric](18, 0) NULL,
    [t1count_3d] [numeric](18, 0) NULL,
    [t2count_3d] [numeric](18, 0) NULL,
    [t3count_3d] [numeric](18, 0) NULL,
    [t1spend_3d] [numeric](18, 0) NULL,
    [t2spend_3d] [numeric](18, 0) NULL,
    [t3spend_3d] [numeric](18, 0) NULL,
    [r_60d] [numeric](18, 0) NULL,
    [f_60d] [numeric](18, 0) NULL,
    [t1_m_60d] [numeric](18, 0) NULL,
    [t2_m_60d] [numeric](18, 0) NULL,
    [t3_m_60d] [numeric](18, 0) NULL,
    [label] [numeric](18, 0) NULL,
    [prob_categoryT0] [float] NULL,
    [prob_categoryT1] [float] NULL,
    [prob_categoryT2] [float] NULL,
    [prob_categoryT3] [float] NULL
  ) WITH (
    DATA_SOURCE = [AzureBlob],
    LOCATION = '{EnrichedProfileBlob}',
    FILE_FORMAT = [CSVFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
  )
END",

      $@"
IF NOT EXISTS (	SELECT * FROM sys.external_tables WHERE name = 'CustomerX' )
BEGIN
  CREATE EXTERNAL TABLE [dbo].[CustomerX]
  (
    [customerID] [varchar](50) NULL,
    [age] [numeric](18, 0) NULL,
    [Gender] [varchar](50) NULL,
    [Income] [varchar](50) NULL,
    [HeadofHousehold] [varchar](50) NULL,
    [number_household] [numeric](18, 0) NULL,
    [months_residence] [numeric](18, 0) NULL
  ) WITH (
    DATA_SOURCE = [AzureBlob],
    LOCATION = '{CustomerBlob}',
    FILE_FORMAT = [CSVFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
  )
END",

      $@"
IF NOT EXISTS (	SELECT * FROM sys.external_tables WHERE name = 'ProductX' )
BEGIN
  CREATE EXTERNAL TABLE [dbo].[ProductX]
  (
    [ProductID] [varchar](50) NULL,
    [Product_description] [varchar](50) NULL
  ) WITH (
    DATA_SOURCE = [AzureBlob],
    LOCATION = '{ProductBlob}',
    FILE_FORMAT = [CSVFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
  )
END",
      $@"
IF NOT EXISTS (	SELECT * FROM sys.external_tables WHERE name = 'PurchaseX' )
BEGIN
  CREATE EXTERNAL TABLE [dbo].[PurchaseX]
  (
    [datetime] [datetime] NULL,
    [customerID] [varchar](50) NULL,
    [ProductID] [varchar](50) NULL,
    [amount_spent] [numeric](18, 0) NULL
  ) WITH (
    DATA_SOURCE = [AzureBlob],
    LOCATION = '{PurchaseBlob}',
    FILE_FORMAT = [CSVFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
  )
END",

      @"
IF NOT EXISTS (	SELECT * FROM sys.tables WHERE name = 'Customer' AND is_external = 0 )
BEGIN
  CREATE TABLE [dbo].[Customer]
  WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )
  AS SELECT * FROM [dbo].[CustomerX]
END",

      @"
IF NOT EXISTS (	SELECT * FROM sys.tables WHERE name = 'Product' AND is_external = 0 )
BEGIN
  CREATE TABLE [dbo].[Product]
  WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )
  AS SELECT * FROM [dbo].[ProductX]
END",

      @"
IF NOT EXISTS (	SELECT * FROM sys.tables WHERE name = 'Purchase' AND is_external = 0 )
BEGIN
  CREATE TABLE [dbo].[Purchase]
  WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )
  AS SELECT * FROM [dbo].[PurchaseX]
END",

    };
  }
}