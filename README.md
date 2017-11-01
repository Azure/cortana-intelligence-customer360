# Customer 360 using Machine Learning on Azure 
The Customer 360 solution provides you a scalable way to build a customer profile enriched by machine learning. It also allows you to uniformly access and operate on data across disparate data sources (while minimizing raw data movement) and leverage the power of Microsoft R Server for scalable modelling and accurate predictions.

This technical guide walks you through the steps to implement a customer 360 solution using Cortana Intelligence Suite on Microsoft Azure. The solution involves:

* **Ingestion and Pre-processing**: Ingest, prepare, and aggregate live user activity data. 

* **Integration of Data Sources**: Integrate and federate customer profile data that’s embedded across disparate data sources. 

* **Feature engineering and ETL**: Segment customers into [RFM][LINK_RFM] segments based on their browsing and purchase behavior. 
 
* **Machine Learning (ML)**):  Build an ML model to predict how likely a customer will purchase in the next few days and from which product category. The ML model enriches existing customer profiles based on their most recent activities.

### 1. Definition and Benefits 
To improve market positioning and profitability, it is important to deeply understand the connection between customer interests and buying patterns. That’s where Customer 360 comes in. 

Customer 360 is an advanced solution to enrich customer profiles using machine learning. A 360-degree enriched customer profile is key to derive actionable insights and smarter data-driven decisions. 

Customer profile enrichment can be applied across multiple business use cases:

1.	Design targeted sales events.
	
2.	Personalize promotions and offers
	
3.	Proactively reduce customer churn
	
4.	Predict customer buying behavior 
	
5.	Forecast demands for better pricing and inventory

6.	Gain actionable insights for better sales and opportunity leads

Some of the benefits of customer profile enrichment includes:
* A 360-degree representation of the customer. 
* Holistic view of your ideal customers’ purchasing behaviors and patterns.
* Allows you to locate, understand and better connect to your ideal customers. 

#### What is under the hood  
Today businesses amass customer data through various channels, such as web browsing patterns, purchasing behaviors, demographic information, and other session-based web footprints. While some of the data is first party to the organization, others are derived from external sources, such as, public domain, partners, manufactures and so on. 

In many cases, only basic customer information can be derived from first party data, while other valuable data remains embedded in different locations and schemas.  

Traditionally most businesses leverage only a small set of this data. However, to boost ROI with enriched customer profile, you need to integrate relevant data from all sources, perform distributed ETL (Extract, Transform, Load) across data processing engines, and use predictive analytics and machine learning to enrich customer information. 

An effective profile enrichment ML model must have access to large datasets even though those datasets are geographically dispersed and schematically heterogenous. Often, these data systems are resource constrained as these are used by multiple main-stream applications and processes simultaneously.

That's why to prepare the data for ML, the feature engineering and ETL processes need to be off-loaded to more powerful computational systems, without having to move raw data physically.

Physically moving raw data to compute not only adds to the cost of the solution but also adds network latency and reduces throughput.

In this technical guide, we will walk you through the steps to create a customer 360 by uniformly accessing data from a variety of data sources (both on-prem and in-cloud) while minimizing data movement and system complexity to boost performance.


### 2. Architecture
An effective profile enrichment ML model creates a holistic customer profile utilizing large datasets from disparate sources. These sources individually hold partial information directly or indirectly about the customer. The data, usually accumulated over time, needs to be prepared for the ML. Most of the data needed for enrichment reside on various heterogeneous, shared, and resource constrained systems which are used by multiple main-stream applications and processes.

Thus, the feature engineering and ETL processes need to be off-loaded to more powerful computation systems without having to physically move the data. Physically moving the data to compute not only adds to the cost of the solution but also adds to network latency and reduces throughput. 

Cross system query execution described in this guide provide ways to minimize data movement, incorporate data from heterogenous data sources (both on-prem and in-cloud), reduce system complexities and improve performance. 

![Architecture Diagram][IMG_ARCH]

**Figure 1: End to End Architecture to create a Customer 360.**

In figure 1, the end to end pipeline is based on a hypothetical scenario that represents a retail company.

Customers are assumed to browse through three online product categories, namely, Technology, Apparel and Cosmetics. 

These categories and timestamp of simulated user actions are generated and streamed to the EventHub. 

Aggregation of total time spent on each category is done by Azure Stream Analytics and the aggregated data is then stored in Azure Storage blobs.

Using SparkSQL on Azure HDInsight the solution can perform feature engineering related steps like data wrangling, cross system query execution to bring in the referential data, and perform ETL to build a consolidated dataset. This consolidated dataset is then stored in the underlying Azure Storage (via a WASB connection) of the cluster, in preparation for ML.

A pre-trained ML model is invoked into a Microsoft R Server instance which is also on the same HDInsight cluster, to classify what category of product a user is likely to purchase from. Since Microsoft R Server runs on the HDI cluster, the movement of raw data is minimized.


This solution contains materials to help both technical and business audiences understand the solution. All components are can be deployed and built on [Azure][4]. 

## Business Audiences
In this repository you will find a folder called [**Solution Overview for Business Audiences**][1]. This folder contains a PowerPoint deck that covers the benefits of using this solution and the ways that customer profile enrichment can help businesses better understand and focus on their customers' need.

For more information on how to tailor Cortana Intelligence to your needs [connect with one of our partners][2].

## Technical Audiences
See the [**Technical Manual Deployment Guide**][3] folder for a full set of instructions on how to customize and deploy this solution on Azure. For technical problems or questions about deploying this solution, please create an issue in this repository.

## Disclaimer
©2017 Microsoft Corporation. All rights reserved.  This information is provided "as-is" and may change without notice. Microsoft makes no warranties, express or implied, with respect to the information provided here. Synthesized data was used to generate the solution and not representative of real world situations.  You are responsible for respecting the rights of others, including procuring and complying with relevant licenses in order to create similar datasets and solution.

<!-- Links -->
[1]: ./Solution%20Overview%20for%20Business%20Audiences
[2]: http://aka.ms/CISFindPartner
[3]: ./Technical%20Deployment%20Guide
[4]: https://azure.microsoft.com/en-us/
[LINK_RFM]: https://en.wikipedia.org/wiki/RFM_%28customer_value%29
[IMG_ARCH]: ./Technical%20Deployment%20Guide/assets/img/architecture.png

