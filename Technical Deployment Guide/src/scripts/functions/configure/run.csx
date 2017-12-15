#r "Microsoft.WindowsAzure.Storage"
#load "Inputs.csx"
#load "Assets.csx"
#load "Resources.csx"

using System.Data.SqlClient;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

public async static Task<object> Run(Inputs req, TraceWriter log) {
  log.Info(JsonConvert.SerializeObject(req));
  var resources = new Resources(req);
  var assets = new Assets(req);
  var emptyBytes = new byte[0];
  var hdiContainer = resources.BlobClient.GetContainerReference(req.HdiContainer);

  await resources.Connection.OpenAsync();
  await hdiContainer.CreateIfNotExistsAsync();

  foreach (var copy in assets.CopyBlobs) {
    var source = new CloudBlockBlob(new Uri(resources.SourceBaseUri, copy.Item1));
    var container = resources.BlobClient.GetContainerReference(copy.Item2);
    var blobName = String.IsNullOrWhiteSpace(copy.Item3) ? copy.Item1 : $"{copy.Item3}/{copy.Item1}";
    await container.CreateIfNotExistsAsync();
    var target = container.GetBlockBlobReference(blobName);
    log.Info(String.Format("Copying blob {0} to {1}", source.Uri, target.Uri));
    await target.StartCopyAsync(source);
  }

  foreach (var hdfsDir in assets.HdfsFolders) {
    var parts = hdfsDir.Split('/');
    
    for (var i = 0; i < parts.Length; i++) {
      var hdfsDirPath = String.Join("/", parts.Take(i + 1));
      var hdfsDirBlob = hdiContainer.GetBlockBlobReference(hdfsDirPath);
      hdfsDirBlob.Metadata["hdi_isfolder"] = "true";
      hdfsDirBlob.Metadata["hdi_permission"] = $@"{{""owner"":""{req.Username}"",""group"":""supergroup"",""permissions"":""rwxr-xr-x""}}";
      log.Info(String.Format("Creating HDFS directory stub at {0}", hdfsDirPath));
      await hdfsDirBlob.UploadFromByteArrayAsync(emptyBytes, 0, emptyBytes.Length);
    }
  }

  using (resources.Connection) {
    foreach (var sql in assets.ExecSql) {
      var command = new SqlCommand(sql, resources.Connection);
      log.Info(String.Format("Executing SQL {0}", sql));
      await command.ExecuteNonQueryAsync();
    }
  }

  // outputs
  var start = DateTime.UtcNow.Add(TimeSpan.FromMinutes(2));
  var end = start.Add(TimeSpan.FromHours(1));
  var profileBlob = resources.BlobClient
    .GetContainerReference(req.HdiContainer)
    .GetBlobReference("data/final_enriched_profiles/enriched_customer_profile.csv");
  var profileSAS = profileBlob.GetSharedAccessSignature(new SharedAccessBlobPolicy
  {
      Permissions = SharedAccessBlobPermissions.Read,
      SharedAccessExpiryTime = DateTime.UtcNow.AddYears(100),
  });

  return new {
    adfStartTime = start.ToString("o"),
    adfEndTime = end.ToString("o"),
    outputCsvUrl = profileBlob.Uri.ToString() + profileSAS,
  };
}