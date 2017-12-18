# Manual Deployment
You will be creating the following resources:

- **Microsoft R Server** - Highly scalable platform that extends the analytic capabilities of R.

- **Azure SQL Data Warehouse (DW)** - Store large and fast growing referential datasets.

- **Azure HDInsight (HDI) Cluster** - Perform distributed computations at massive scale.

- **Azure Storage Blob** - Use HDFS compatible storage for HDInsight cluster.

- **Azure EventHub** - Time-based event buffer for near real-time events.

- **Azure Stream Analytics (ASA)** - On demand real-time Advanced Analytics service.

- **Azure Data Factory (ADF)** - Data pipeline orchestrator, connects to both on-prem and in-cloud sources and allows data lineage visualizations.

- **Azure Functions App/WebApp** - Process events with serverless code.

- **PowerBI** - Fully interactive data visualizations.

**Entire manual set-up takes about 20-30 minutes.**

### Deployment Steps
**All scripts used in the following deployment steps are available in the GitHub repository, [src/scripts][LINK_SCRIPTS]** 

Create, configure and deploy all required Azure resources **(ARM)** using PowerShell.

> Follow this tutorial to install [PowerShell][LINK_PS]

Due to ADF dependencies on pre-loaded sample data and jar files (HDInsight activities for ETL and Scoring) you will need to create **two** configuration parameters for all resource login credentials.

1. Admin username
1. Password 
 

- **STEP 1** - Clone [this repository][LINK_GH] to a location on your machine. 

- **STEP 2** - Launch Windows PowerShell application and navigate to the **script/arm** folder of the repository.
 
- **STEP 3** - Connect to Azure, set what subscription to use and create a resource group to deploy your azure resources.   
	
   ```Powershell
    Login-AzureRmAccount
    Select-AzureRmSubscription -SubscriptionId <subscription_id>
    New-AzureRmResourceGroup -Name $ResourceGroupName -Location <location_of_your_choice>  
    ```  
    
> NOTE: Ensure the chosen location is valid for all resources highlighted above. Find information [here](https://azure.microsoft.com/en-us/status/)  
	
- **STEP 4** - Deploy an Azure Storage Account   

```Powershell
$templateFilePath = "storage.json"
New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $templateFilePath -Verbose
```  

A successful deployment will output the storage account name and key. Make a note of these two keys.  
```Powershell
DeploymentName          : storage
ResourceGroupName       : <resource_group_name>
ProvisioningState       : Succeeded
Timestamp               : 12/18/2017 6:48:04 PM
Mode                    : Incremental
TemplateLink            :
Parameters              :
Outputs                 :
                          Name             Type                       Value
                          ===============  =========================  ==========
                          storageAccountName  String                   <storage_name>
                          storageAccountKey  String                    <storage_key>

DeploymentDebugLogLevel :
```  

This is the storage for the entire solution.   

- **STEP 5** - Deploy an Azure Function App  
Azure Functions will be used for setup and orchestration of your entire solution. 
	- Open `functionparameters.json` and replace **AccountName** and **AccountKey** with the values copied from the Azure Storage Account deployment step. Save file and close.  
	- Deploy the Function App using the parameters file that contains information for the Webfarm which will be set to `alwaysOn`.   
	
	```Powershell
	$templateFilePath = "functionapp.json"
	$parametersFilePath = "functionparameters.json"
	New-AzureRmResourceGroupDeployment -ResourceGroupName $resourceGroupName -TemplateFile $templateFilePath -TemplateParameterFile $parametersFilePath -Verbose
	```  
The function app should be successfully deployed now. 

	```Powershell
	DeploymentName          : functionapp
	ResourceGroupName       : <resource_group_name>
	ProvisioningState       : Succeeded
	Timestamp               : 12/18/2017 9:13:51 PM
	Mode                    : Incremental
	TemplateLink            :
	Parameters              :
				  Name             Type                       Value
				  ===============  =========================  ==========
				  siteConfig       Object                     {
				    "alwaysOn": true,
				    "use32BitWorkerProcess": true,
				    "appSettings": [
				      {
					"Name": "FUNCTIONS_EXTENSION_VERSION",
					"Value": "latest"
				      },
				      {
					"Name": "AzureWebJobsStorage",
					"Value": "DefaultEndpointsProtocol=https;AccountName=<storage_account_name>;AccountKey=<storage_account_key>"
				      },
				      {
					"Name": "AzureWebJobsDashboard",
					"Value": "DefaultEndpointsProtocol=https;AccountName=<storage_account_name>;AccountKey=<storage_account_key>"
				      }
				    ],
				    "connectionStrings": null
				  }
				  servicePlanSku   String                     S1
				  servicePlanTier  String                     Standard

	Outputs                 :
				  Name             Type                       Value
				  ===============  =========================  ==========
				  functionAppName  String                     <function_name>
				  functionAppBaseUrl  String                  https://<function_name>.azurewebsites.net/api/
				  servicePlanName  String                     <hosting_plan_name>

	DeploymentDebugLogLevel :
	```  
	
    **MANUAL STEPS AFTER FUNCTION DEPLOY**  

    1. Copy over the contents of the **function** directory, i.e. **configure** folder into the Azure Functions Web App file system.
        - Go to [Azure](https://portal.azure.com)
        - Locate your resource group.
        - Click the newly created function app (**App Service**).
        - Under Functions, click the **+** sign to create a new custom function.
        - Select **HTTP trigger** function. Change language to C#, change the name to **configure** and set **Authorization level** to Anonymous. 
        - Delete `run.csx` and replace the contents of `function.json` with that on your local machine as it cannot be deleted. 
        - Upload all the rest of the files under **configure** directory to the Function App.  

    2. Call the Function app, via HTTP POST, using JSON input that conforms to the object found inside `input.csx` as shown below.  

        ```Powershell
        public class Inputs {  
              public string PatternAssetBaseUrl { get; set; }
              public string Username { get; set; }
              public string Password { get; set; }
              public string Storage { get; set; }
              public string StorageKey { get; set; }
              public string HdiContainer { get; set; }
              public string SqlHost { get; set; }
              public string SqlDatabase { get; set; }
        }
        ```  

    > NOTE: The value for **PatternAssetBaseUrl** should be https://ciqsdatastorage.blob.core.windows.net/customer-360  

- **STEP 5** - Deploy other resources  
Using the Powershell `New-AzureRmResourceGroupDeployment` cmdlet, deploy the following JSON templates in the following order:  
	> NOTE: The deployment Cmdlet will ask you for some required parameters like ResourceGroupName, Useradmin and Password, ADF start and end times, Pattern Base Url, WebFarm and Website names (from the deployed Function App).   

   - **01.json**      
    ```Powershell
    $templatePath = "01.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Step 1" -TemplateUri $templatePath 
    ```    
    - **02.json**   
    ```Powershell
    $templatePath = "02.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Step 2" -TemplateUri $templatePath 
    ```  
    
 <!-- Links -->
[LINK_PS]: https://docs.microsoft.com/en-us/powershell/azure/install-azurerm-ps?view=azurermps-3.8.0
[LINK_SCRIPTS]: https://github.com/Azure/cortana-intelligence-customer360/tree/master/Technical%20Deployment%20Guide/src/scripts
[LINK_GH]: https://github.com/Azure/cortana-intelligence-customer360 
 
