# Deployment
You will be creating the following resources. 

- **Microsoft R Server** - Highly scalable platform that extends the analytic capabilities of R.

- **Azure SQL Data Warehouse (DW)** - Store large and fast growing referential datasets.

- **Azure HDInsight (HDI) Cluster** - Perform distributed computations at massive scale.

- **Azure Storage Blob** - Use HDFS compatible storage for HDInsight cluster.

- **Azure EventHub** - Time-based event buffer for near real-time events.

- **Azure Stream Analytics (ASA)** - On demand real-time Advanced Analytics service.

- **Azure Data Factory (ADF)** - Data pipeline orchestrator, connects to both on-prem and in-cloud sources and allows data lineage visualizations.

- **Azure Functions/WebApp** - Process events with serverless code.

- **PowerBI** - Fully interactive data visualizations.

**Entire manual set-up takes about 40 minutes.**

### Manual Deployment
**All scripts used in the following deployment steps are available in the github repository, [src/scripts][LINK_SCRIPTS]** 

Create, configure and deploy all required Azure resources **(ARM)** using PowerShell.

> Follow this tutorial to install [PowerShell][LINK_PS]

This deployment has six steps. Due to ADF dependencies on pre-loaded sample data and jar files (for HDInsight activities for ETL and Scoring) you will need to create **two** configuration parameters for all resource login credentials.

>  Admin username and password. 
 
>  -  **Admin username and password** 

- **STEP 1** - Checkout [this repository][LINK_GH] to a location on your machine. 

- **STEP 2** - Launch Windows PowerShell application and navigate to the location the code was checked out. 

- **STEP 3** - Deploy Azure Storage and HDInsight clusters.
Azure Storage (HDFS compatible storage) will be the backing storage for the HDInsight cluster. The deployed HDInsight cluster will be a Spark and MRS cluster.

    ```PowerShell
    $adminUsername = <your_admin_username>
    $adminPassword = convertto-securestring <your_password_string> -asplaintext -force
    $rg = "customer-profile-enrichment"
    $loc = "EastUS"
    New-AzureRmResourceGroup $rg $loc -Force 
    New-AzureRmResourceGroupDeployment -Name CreateStorageAndHDI -ResourceGroupName $rg -TemplateFile src/scripts/azuredeploy_init.json -admin-username $adminUsername -admin-password $adminPassword -verbose
    ```  
    Note the following outputs:
    - Storage name
    - Storage key
    - HDInsight blob container name

- **STEP 4** - Copy sample data and dependencies libraries to blob using outputs from first deployment. 
	```PowerShell
	$storage = <storage_account_name>
	$storagekey = <storage_account_key>
	$hdicontainer = <hdicontainer_name>
    
    .src/scripts/copy-blobs $storage $storagekey $hdicontainer
    ```

- **STEP 5** - Deploy remaining ARM resources like Azure SQL DW, ADF and so on 
	```PowerShell
    $start_time = [DateTime]::UtcNow
	$end_time = $start_time.add([TimeSpan]::FromHours(2))
	$params['ADFStartTime'] = $start_time.ToString('o')
	$params['ADFEndTime'] = $end_time.ToString('o')	
    
    New-AzureRmResourceGroupDeployment -Name DeployAllResources -ResourceGroupName $rg -TemplateFile src/scripts/azuredeploy_sll.json -admin-username $adminUsername -admin-password $adminPassword -TemplateParameterObject $params -verbose
    ```

- **STEP 6** - Create Azure SQL DW internal tables for referential data and external table for final enriched profiles. 
	
    ```PowerShell  
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

        Exec-SqlFile "src/scripts/credentials/MasterKey.sql"
        Exec-SqlFile "src/scripts/credentials/BlobStorageCredential.sql"
        Exec-SqlFile "src/scripts/external-data-sources/AzureBlob.sql"
        Exec-SqlFile "src/scripts/external-file-formats/CSVFormat.sql"
        Exec-SqlFile "src/scripts/external-tables/dbo.Enriched_Customer_Profile_Blob.sql"
        Exec-SqlFile "src/scripts/tables/dbo.Customer.sql"
        Exec-SqlFile "src/scripts/tables/dbo.Product.sql"
        Exec-SqlFile "src/scripts/tables/dbo.Purchase.sql"
    ```
    
 <!-- Links -->
[LINK_PS]: https://docs.microsoft.com/en-us/powershell/azure/install-azurerm-ps?view=azurermps-3.8.0
[LINK_SCRIPTS]: https://github.com/Azure/cortana-intelligence-customer-profile-enrichment-solution/tree/master/src
[LINK_GH]: https://github.com/Azure/cortana-intelligence-customer-profile-enrichment-solution 
 
