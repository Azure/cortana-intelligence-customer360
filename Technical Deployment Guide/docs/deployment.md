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
	
- **STEP 4** - Deploy an Azure Function App
Azure Functions will be used for setup and orchestration of your entire solution. 

    ```PowerShell
    $templatePath = "functionapp.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Create Function App" -TemplateUri $templatePath 
    ```  
    ** MANUAL STEPS AFTER FUNCTION DEPLOY**  
    1. Copy over the contents of the **function** directory, i.e. **configure** folder into the Azure Functions Web App File System. 
    2. Call the Function app, via HTTP POST, using JSON input that conforms to the object found inside `input.csx` as shown below.
    
    ```csharp
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
    > NOTE: The value for **PatternAssetBaseUrl** is  https://ciqsdatastorage.blob.core.windows.net/customer-360`  

- **STEP 5** - Deploy other resources. 
Using the Powershell `New-AzureRmResourceGroupDeployment` cmdlet, deploy the following JSON templates in the following order:  
	> NOTE: The deployment Cmdlet will ask you for some required parameters like ResourceGroupName, Useradmin and Password, ADF start and end times, Pattern Base Url, WebFarm and Website names (from the deployed Function App).   

	- **01.json**  
    ```PowerShell
    $templatePath = "01.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Create Function App" -TemplateUri $templatePath 
    ```
    
    - **02.json**  
    ```PowerShell
    $templatePath = "02.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Create Function App" -TemplateUri $templatePath 
    ```
    
    - **03.json** 
    ```PowerShell
    $templatePath = "03.json"
    New-AzureRmResourceGroupDeployment -ResourceGroupName $ResourceGroupName -Name "Create Function App" -TemplateUri $templatePath 
    ```
    
 <!-- Links -->
[LINK_PS]: https://docs.microsoft.com/en-us/powershell/azure/install-azurerm-ps?view=azurermps-3.8.0
[LINK_SCRIPTS]: https://github.com/Azure/cortana-intelligence-customer360/tree/master/Technical%20Deployment%20Guide/src/scripts
[LINK_GH]: https://github.com/Azure/cortana-intelligence-customer360 
 
