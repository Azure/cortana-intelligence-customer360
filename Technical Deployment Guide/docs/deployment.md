# Manual Deployment
These steps describe how you can manually deploy the Customer 360 solution. This will enable you to customize the solution as needed.  

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
 

##### STEP 1
Clone [this repository][LINK_GH] to a location on your machine. 

##### STEP 2
Launch Windows PowerShell application and navigate to the **Technical Deployment Guide\src\scripts\arm** folder of the repository.
 
##### STEP 3
Connect to Azure, set what subscription to use and create a resource group to deploy your azure resources.   
	
```Powershell
Login-AzureRmAccount
Get-AzureRmSubscription # Get a list of your subscriptions
Select-AzureRmSubscription -SubscriptionId <subscription_id>
$ResourceGroupName = <your_resource_group>
New-AzureRmResourceGroup -Name $ResourceGroupName -Location <location_of_your_choice>  
```  

> NOTE: Ensure the chosen location is valid for all resources highlighted above. Find information [here](https://azure.microsoft.com/en-us/status/)   

##### STEP 4
Deploy an Azure Storage Account   

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
##### STEP 5
Deploy resources  
Using the Powershell `New-AzureRmResourceGroupDeployment` cmdlet, deploy the following JSON templates in the following order:  
	
> NOTE: The deployment Cmdlet will ask you for some required parameters like username and password.  

###### Deploy Resources      
This steps deploys a couple of resources, including an Azure HDInsight Cluster. Please ensure you have enough cores for this and note that deployment of a cluster takes up to 20mins. The `Verbose` flag shows the deployment progress.

```Powershell
$templateFilePath = "deployresources.json"
New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $templateFilePath -Verbose 
```      
> **NOTE:** Uppercase characters are not allowed in HDInsight cluster username.
 
##### STEP 6
Deploy an Azure Function App  
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
    - Browse [Azure](https://portal.azure.com)
    - Go to your resource group.
    - Click the newly created function app (**App Service**).
    - Click Functions **>** + **>** Customer Function **>** HTTP Trigger 
    	- Change the name to **configure**
    	- Change language to C#
    	- Set **Authorization level** to **Anonymous**.  
    	- Click **Create**
    - On the right hand side of the page, expand the **View Files** tab.
    - Click on **Upload** to copy over the function assets. 
        - Browse your local machine repository for **src/scripts/functions/configure**
        - Select all the files and click **Open** to upload them to the Azure Function file system.  

2. To activate the HTTP triggered function, send a HTTP POST request, using JSON input that conforms to the object found inside `input.csx` as shown below.  

```cs
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

- Expand the **Test** tab.  

- Select **HTTP** protocol with **POST** method
- Fill out sample JSON below and paste it into **Request Body**. 

    ```json
    {
        "PatternAssetBaseUrl": "https://ciqsdatastorage.blob.core.windows.net/customer-360",
        "Username": "your username",
        "Password": "your password",
        "Storage": "your storage account name",
        "StorageKey" : "your storage account key",
        "HdiContainer": "your hdi container",
        "SqlHost": "sql local host",
        "SqlDatabase": "Customer360" 

    }
    ```
- Click on **Save and Run** to trigger the function that configures the solution.  

- Copy the following outputs from the successful HTTP response JSON:
	- **adfStartTime** - Pipeline start time
	- **adfEndTime** - Pipeline end time
	- **outputCsvUrl** - Link to Csv for PowerBI visualizations
> NOTE: All these values are outputs of the previous ARM deployment steps.  

##### STEP 7
Deploy the ADF pipelines
    
- **Deploy ADF Pipelines**   
```Powershell
$templateFilePath = "deploypipelines.json"
New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $templateFilePath -Verbose 
```  

> NOTE: The deployment Cmdlet will ask you for some required parameters username, password, ADF start and end times (obtained from step above). These parameters must be the same as what was provided during resource creation.  
    
### Next steps
Visualize the data using PowerBI. Find the post deployment steps for this [here][LINK_TO_PBI].  


 <!-- Links -->
[LINK_PS]: https://docs.microsoft.com/en-us/powershell/azure/install-azurerm-ps?view=azurermps-3.8.0
[LINK_SCRIPTS]: https://github.com/Azure/cortana-intelligence-customer360/tree/master/Technical%20Deployment%20Guide/src/scripts
[LINK_GH]: https://github.com/Azure/cortana-intelligence-customer360 
[LINK_TO_PBI]: https://github.com/Azure/cortana-intelligence-customer360/blob/master/Technical%20Deployment%20Guide/docs/pbi_steps.md
