[CmdletBinding()]
Param(

  [Parameter(Mandatory=$True, Position=1)]
  [string]$SqlServerHost,

  [Parameter(Mandatory=$True, Position=2)]
  [string]$SqlServerUser,

  [Parameter(Mandatory=$True, Position=3)]
  [string]$SqlServerPassword,

  [Parameter(Mandatory=$True, Position=4)]
  [string]$CredentialIdentity,
	
  [Parameter(Mandatory=$True, Position=5)]
  [string]$CredentialSecret,

  [Parameter(Mandatory=$True, Position=6)]
  [string]$HDIStorageContainer,

  [Parameter(Mandatory=$True, Position=7)]
  [string]$StorageAccount,

  [Parameter(Mandatory=$True, Position=8)]
  [string]$Database,

  [Parameter(Mandatory=$True, Position=9)]
  [string]$MasterKey,

  [string]$CustomerProfileBlobPath = "/data/final_enriched_profiles/enriched_customer_profile.csv"
)

$vars = @{
  CREDENTIAL_IDENTITY = $CredentialIdentity;
  CREDENTIAL_SECRET = $CredentialSecret;
  HDI_STORAGE_CONTAINER = $HDIStorageContainer;
  STORAGE_ACCOUNT = $StorageAccount;
  CUSTOMER_PROFILE_BLOB_PATH = $CustomerProfileBlobPath;
  MASTER_KEY = $MasterKey;
}

function Replace-Vars([string]$text) {
  [regex]$pattern = "\$\(([^)]+)\)"
  $replace = { $vars[$args[0].Groups[1].Value] }
  return $pattern.Replace($text, $replace)
}

function Exec-Sql([string]$sql) {
  sqlcmd -S $SqlServerHost -U $SqlServerUser -P $SqlServerPassword -d $Database -I -Q "$sql"
}

function Exec-SqlFile([string]$path) {
  $sql = Replace-Vars (Get-Content $path)
  $temp = [IO.Path]::GetTempFileName()
  $sql | Out-File $temp
  sqlcmd -S $SqlServerHost -U $SqlServerUser -P $SqlServerPassword -d $Database -I -i $temp
  Remove-Item $temp
}

Exec-SqlFile "./credentials/MasterKey.sql"
Exec-SqlFile "./credentials/BlobStorageCredential.sql"
Exec-SqlFile "./external-data-sources/AzureBlob.sql"
Exec-SqlFile "./external-file-formats/CSVFormat.sql"
Exec-SqlFile "./external-tables/dbo.Enriched_Customer_Profile_Blob.sql"
Exec-SqlFile "./tables/dbo.Customer.sql"
Exec-SqlFile "./tables/dbo.Product.sql"
Exec-SqlFile "./tables/dbo.Purchase.sql"
