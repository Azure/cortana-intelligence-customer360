CREATE EXTERNAL DATA SOURCE [AzureBlob] WITH (
  TYPE = HADOOP,
  LOCATION = N'wasbs://$(HDI_STORAGE_CONTAINER)@$(STORAGE_ACCOUNT).blob.core.windows.net',
  CREDENTIAL = [BlobStorageCredential]
)
