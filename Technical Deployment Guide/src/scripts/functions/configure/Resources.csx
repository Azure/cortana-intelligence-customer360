#r "System.Data"
#load "Inputs.csx"

using System;
using System.Data.SqlClient;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;

public class Resources {
  const string ConnectionString = "Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
  public CloudBlobClient BlobClient { get; private set; }
  public Uri SourceBaseUri { get; private set; }
  public SqlConnection Connection { get; private set; }

  public Resources(Inputs inputs) {
    if (!inputs.PatternAssetBaseUrl.EndsWith("/")) {
      inputs.PatternAssetBaseUrl += "/";
    }
    var cred = new StorageCredentials(inputs.Storage, inputs.StorageKey);
    var storage = new CloudStorageAccount(cred, true);
    this.BlobClient = storage.CreateCloudBlobClient();
    this.SourceBaseUri = new Uri(inputs.PatternAssetBaseUrl);
    this.Connection = new SqlConnection(String.Format(ConnectionString, inputs.SqlHost, inputs.SqlDatabase, inputs.Username, inputs.Password));
  }
}