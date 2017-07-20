import sys, datetime
from pyspark import SparkContext
from pyspark.sql import *
from subprocess import CalledProcessError, check_output, call 

# TODO rename to etl_and_feature_engineering.py

def wasb_folder_not_exist(cmd_list):
    """ Check if the folder exists in the underlying wasb storage """
    print('[INFO] -  Validating folder exits. Running hdfs command: {}'.format(' '.join(cmd_list)))
    returncode = 0
    try:
        std_output = check_output(cmd_list)
    except CalledProcessError as e:
        returncode = e.returncode
    return returncode # code 1 means it does not exist

# Read input arguments passed in from ADF parameters
# Path to wasb folder is expected as input 
dataset_exists = False
backup_folder_exists = False 
NUM_ARGS = 6
if len(sys.argv) == NUM_ARGS:
    adf_input_timeslice_path = sys.argv[1]
    adf_input_timeslice_list = adf_input_timeslice_path.split('/')
    wasb_dataset_path = '/'.join(adf_input_timeslice_list[:-1]) # Required Input arg Format - 'wasb:///yyyy/MM/dd/HH'
    wasb_etl_backups = 'wasb:///data/etl_backups'
    cmd_check_dataset = ['hdfs', 'dfs', '-test', '-d', str(wasb_dataset_path)]

    if wasb_folder_not_exist(cmd_check_dataset):
        raise NotADirectoryError('Folder does not exist')
    dataset_exists = True

    # Read SQL Referential Data Via JDBC Connection
    url = "jdbc:sqlserver://{2};database={3};user={4};password={5}".format(*sys.argv)
else:
    raise ValueError('Expecting {} input arguments. Got {}.'.format(NUM_ARGS, len(sys.argv)))

print('[ETL PATHS] - \nWasb DS Path: {}\nADF Path: {} '.format(wasb_dataset_path, adf_input_timeslice_path))
sc = SparkContext()
spark = HiveContext(sc)

if dataset_exists:

    # Load data as dataframes
    #   1. User Browsing Data
    #   2. Purchase Data
    #   3. Customer Demographics
    #### Read Userbrowsing Data as csv from blob and drop partitionid from eventhub
    dfBrowsing = spark.read.csv('{}/{}'.format(str(wasb_dataset_path), '*.csv'), header=True) \
        .drop('partitionid')
    
    dfPurchase = spark.read.jdbc(url=url, table="dbo.Purchase")
    dfCustomer = spark.read.jdbc(url=url,table="dbo.Customer")

    # Register temp views for Spark SQL
    dfPurchase.createOrReplaceTempView("Purchase")
    dfCustomer.createOrReplaceTempView("Customer")
    dfBrowsing.createOrReplaceTempView("BrowsingTPSTable")

    # SQL Aggregation 
    # Aggregate users total browsing time in all categories
    # represented as a single entry
    spark.sql('''
        SELECT 
            userDatetime, 
            customerID, 
            SUM(category_T1) AS category_T1, 
            SUM(category_T2) AS category_T2, 
            SUM(category_T3) AS category_T3 
        FROM BrowsingTPSTable 
        GROUP BY customerID, userDatetime
    ''').createOrReplaceTempView('UserBrowsingTable')

    # Temp - Purchase data will null replacements
    spark.sql('''
        SELECT datetime, customerID, ProductID, 
            CASE WHEN ProductID = 'T1' THEN amount_spent ELSE 0 END AS T1_amount_spent,
            CASE WHEN ProductID = 'T2' THEN amount_spent ELSE 0 END AS T2_amount_spent,
            CASE WHEN ProductID = 'T3' THEN amount_spent ELSE 0 END AS T3_amount_spent
        FROM purchase
    ''').createOrReplaceTempView('temp')

    spark.sql('''
    SELECT 
        a.userdatetime,
        a.customerID, 
        a.category_T1,
        a.category_T2,
        a.category_T3,
        CASE WHEN (b.T1_amount_spent IS NULL) THEN 0 ELSE b.T1_amount_spent END AS amount_spent_T1,
        CASE WHEN (b.T2_amount_spent IS NULL) THEN 0 ELSE b.T2_amount_spent END AS amount_spent_T2,
        CASE WHEN (b.T3_amount_spent IS NULL) THEN 0 ELSE b.T3_amount_spent END AS amount_spent_T3
    FROM UserBrowsingTable a LEFT JOIN temp b
    ON datediff(a.userdatetime, b.datetime)=1 
    AND a.customerID=b.customerID 
    ORDER by customerID asc, a.userdatetime asc''').createOrReplaceTempView('merge_temp')
    spark.sql("SELECT count(*) FROM merge_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()

    # Join Purchase and Browsing. 
    # Return results for every customer that browsed and
    # made a purchase within one day
    spark.sql('''
            SELECT * FROM merge_temp
            WHERE userDatetime >= '2017-02-26 05:59:59.000'
            ORDER BY customerID ASC, userDatetime ASC
    ''').createOrReplaceTempView('subset_temp')

    # DEBUG - Subset of purchase/browsing for the past 3 days. 
    spark.sql("SELECT count(*) FROM subset_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()

    # Find the customers activity for the past three days
    spark.sql('''
        SELECT
            customerID, 
            SUM(category_T1) AS T1count_3d,
            SUM(category_T2) AS T2count_3d,
            SUM(category_T3) AS T3count_3d, 
            SUM(amount_spent_T1) AS T1spend_3d,
            SUM(amount_spent_T2) AS T2spend_3d,
            SUM(amount_spent_T3) AS T3spend_3d
        FROM subset_temp
        GROUP BY customerID
    ''').createOrReplaceTempView('activity_3d')

    # DEBUG - Activity 3d
    spark.sql("SELECT * FROM activity_3d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_3d WHERE t1spend_3d > 0 or t2spend_3d > 0 or t3spend_3d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_3d WHERE t1count_3d > 0 or t2count_3d > 0 or t3count_3d > 0 LIMIT 10").show()

    # Find the customers activity for the past 30 days
    spark.sql('''
        SELECT * FROM merge_temp
        WHERE userDatetime >= '2017-02-06 05:59:59.000' AND userDatetime < '2017-03-08 05:59:59.000'
        ORDER BY customerID ASC, userDatetime ASC
    ''').createOrReplaceTempView('subset_temp')
    
    # DEBUG - subset_temp 30d
    spark.sql("SELECT count(*) FROM subset_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()
    spark.sql("SELECT count(*) FROM subset_temp").show()

    # The customers' activity for the past 30d
    spark.sql('''
        SELECT 
            customerID, 
            SUM(category_T1) AS T1count_30d,
            SUM(category_T2) AS T2count_30d,
            SUM(category_T3) AS T3count_30d, 
            SUM(amount_spent_T1) AS T1spend_30d,
            SUM(amount_spent_T2) AS T2spend_30d,
            SUM(amount_spent_T3) AS T3spend_30d
        FROM subset_temp
        GROUP BY customerID
    ''').createOrReplaceTempView('activity_30d')

    spark.sql("SELECT * FROM activity_30d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_30d WHERE t1spend_30d > 0 or t2spend_30d > 0 or t3spend_30d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_30d WHERE t1count_30d > 0 or t2count_30d > 0 or t3count_30d > 0 LIMIT 10").show()

    # Generate user activity for the past 10 days
    spark.sql('''
        SELECT * FROM merge_temp
        WHERE userDatetime >= '2017-02-26 05:59:59.000' and userDatetime < '2017-03-08 05:59:59.000'
        ORDER BY customerID ASC, userDatetime ASC
    ''').createOrReplaceTempView('subset_temp')

    spark.sql('''
        SELECT 
            customerID, 
            SUM(category_T1) AS T1count_10d,
            SUM(category_T2) AS T2count_10d,
            SUM(category_T3) AS T3count_10d, 
            SUM(amount_spent_T1) AS T1spend_10d,
            SUM(amount_spent_T2) AS T2spend_10d,
            SUM(amount_spent_T3) AS T3spend_10d
        FROM subset_temp
        GROUP BY customerID
    ''').createOrReplaceTempView('activity_10d')
    
    # DEBUG - Activity 10 days
    spark.sql("SELECT * FROM activity_10d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_10d WHERE t1spend_10d > 0 or t2spend_10d > 0 or t3spend_10d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_10d WHERE t1count_10d > 0 or t2count_10d > 0 or t3count_10d > 0 LIMIT 10").show()
    
    # Generate activities for 10d, and 30d
    spark.sql('''
        SELECT 
            a.customerID, 
            a.T1count_30d, a.T2count_30d, a.T3count_30d, 
            a.T1spend_30d, a.T2spend_30d, a.T3spend_30d,
            b.T1count_10d, b.T2count_10d, b.T3count_10d, 
            b.T1spend_10d, b.T2spend_10d, b.T3spend_10d
        FROM activity_30d a LEFT JOIN activity_10d b ON a.customerID = b.customerID
    ''').createOrReplaceTempView('activity_temp')
    
    # DEBUG - Customer table with activities for 10d, and 30days
    spark.sql("SELECT COUNT(*) FROM activity_temp LIMIT 10").show()

    # Generate activities for 3d, 10d, and 30d
    spark.sql('''
            SELECT 
                a.customerID, 
                a.T1count_30d, a.T2count_30d, a.T3count_30d, 
                a.T1spend_30d, a.T2spend_30d, a.T3spend_30d,
                a.T1count_10d, a.T2count_10d, a.T3count_10d, 
                a.T1spend_10d, a.T2spend_10d, a.T3spend_10d,
                b.T1count_3d, b.T2count_3d, b.T3count_3d, 
                b.T1spend_3d, b.T2spend_3d, b.T3spend_3d 
            FROM activity_temp a LEFT JOIN activity_3d b ON a.customerID = b.customerID
    ''').createOrReplaceTempView('activity_all')

    # DEBUG - Customer profile table with all activities - , 3d, 10d, 30d 
    spark.sql("SELECT COUNT(*) FROM activity_all LIMIT 10").show()

    spark.sql('''
        SELECT * FROM MERGE_TEMP
        WHERE userDatetime >= '2017-01-07 05:59:59.000' AND userDatetime < '2017-03-08 05:59:59.0000'
        ORDER BY customerID ASC, userDatetime ASC
    ''').createOrReplaceTempView('subset_temp')

    spark.sql('''
            SELECT 
                customerID, 
                DATEDIFF('2017-03-08 06:00:00.000', MAX(userDatetime)) AS r_60d,
                COUNT(*) AS f_60d,
                SUM(amount_spent_T1) AS T1_m_60d,
                SUM(amount_spent_T2) AS T2_m_60d,
                SUM(amount_spent_T3) AS T3_m_60d
            FROM subset_temp
            GROUP BY customerID
    ''').createOrReplaceTempView('rfm_60d')

    # DEBUG - rfm for the past 60 days
    spark.sql('SELECT * FROM rfm_60d').show()

    spark.sql('''
        SELECT 
            a.customerID, 
            a.age, 
            a.Gender, 
            a.Income, 
            a.HeadofHousehold, 
            a.number_household, 
            a.months_residence, 
            CASE WHEN (b.T1count_30d IS NULL) THEN 0 ELSE b.T1count_30d END AS T1count_30d,
            CASE WHEN (b.T2count_30d IS NULL) THEN 0 ELSE b.T2count_30d END AS T2count_30d,
            CASE WHEN (b.T3count_30d IS NULL) THEN 0 ELSE b.T3count_30d END AS T3count_30d,
            CASE WHEN (b.T1spend_30d IS NULL) THEN 0 ELSE b.T1spend_30d END AS T1spend_30d,
            CASE WHEN (b.T2spend_30d IS NULL) THEN 0 ELSE b.T2spend_30d END AS T2spend_30d,
            CASE WHEN (b.T3spend_30d IS NULL) THEN 0 ELSE b.T3spend_30d END AS T3spend_30d,

            CASE WHEN (b.T1count_10d IS NULL) THEN 0 ELSE b.T1count_10d END AS T1count_10d,
            CASE WHEN (b.T2count_10d IS NULL) THEN 0 ELSE b.T2count_10d END AS T2count_10d,
            CASE WHEN (b.T3count_10d IS NULL) THEN 0 ELSE b.T3count_10d END AS T3count_10d,
            CASE WHEN (b.T1spend_10d IS NULL) THEN 0 ELSE b.T1spend_10d END AS T1spend_10d,
            CASE WHEN (b.T2spend_10d IS NULL) THEN 0 ELSE b.T2spend_10d END AS T2spend_10d,
            CASE WHEN (b.T3spend_10d IS NULL) THEN 0 ELSE b.T3spend_10d END AS T3spend_10d,

            CASE WHEN (b.T1count_3d IS NULL) THEN 0 ELSE b.T1count_3d END AS T1count_3d,
            CASE WHEN (b.T2count_3d IS NULL) THEN 0 ELSE b.T2count_3d END AS T2count_3d,
            CASE WHEN (b.T3count_3d IS NULL) THEN 0 ELSE b.T3count_3d END AS T3count_3d,
            CASE WHEN (b.T1spend_3d IS NULL) THEN 0 ELSE b.T1spend_3d END AS T1spend_3d,
            CASE WHEN (b.T2spend_3d IS NULL) THEN 0 ELSE b.T2spend_3d END AS T2spend_3d,
            CASE WHEN (b.T3spend_3d IS NULL) THEN 0 ELSE b.T3spend_3d END AS T3spend_3d
        FROM Customer a LEFT JOIN activity_all b
        ON a.customerID = b.customerID 
        ORDER BY customerID ASC
    ''').createOrReplaceTempView('customer_profile_temp')

    spark.sql('''
        SELECT 
            a.customerID, 
            a.age, 
            a.Gender, 
            a.Income, 
            a.HeadofHousehold, 
            a.number_household, 
            a.months_residence, 
            CASE WHEN (a.T1count_30d IS NULL) THEN 0 ELSE a.T1count_30d END AS T1count_30d,
            CASE WHEN (a.T2count_30d IS NULL) THEN 0 ELSE a.T2count_30d END AS T2count_30d,
            CASE WHEN (a.T3count_30d IS NULL) THEN 0 ELSE a.T3count_30d END AS T3count_30d,
            CASE WHEN (a.T1spend_30d IS NULL) THEN 0 ELSE a.T1spend_30d END AS T1spend_30d,
            CASE WHEN (a.T2spend_30d IS NULL) THEN 0 ELSE a.T2spend_30d END AS T2spend_30d,
            CASE WHEN (a.T3spend_30d IS NULL) THEN 0 ELSE a.T3spend_30d END AS T3spend_30d,

            CASE WHEN (a.T1count_10d IS NULL) THEN 0 ELSE a.T1count_10d END AS T1count_10d,
            CASE WHEN (a.T2count_10d IS NULL) THEN 0 ELSE a.T2count_10d END AS T2count_10d,
            CASE WHEN (a.T3count_10d IS NULL) THEN 0 ELSE a.T3count_10d END AS T3count_10d,
            CASE WHEN (a.T1spend_10d IS NULL) THEN 0 ELSE a.T1spend_10d END AS T1spend_10d,
            CASE WHEN (a.T2spend_10d IS NULL) THEN 0 ELSE a.T2spend_10d END AS T2spend_10d,
            CASE WHEN (a.T3spend_10d IS NULL) THEN 0 ELSE a.T3spend_10d END AS T3spend_10d,

            CASE WHEN (a.T1count_3d IS NULL) THEN 0 ELSE a.T1count_3d END AS T1count_3d,
            CASE WHEN (a.T2count_3d IS NULL) THEN 0 ELSE a.T2count_3d END AS T2count_3d,
            CASE WHEN (a.T3count_3d IS NULL) THEN 0 ELSE a.T3count_3d END AS T3count_3d,
            CASE WHEN (a.T1spend_3d IS NULL) THEN 0 ELSE a.T1spend_3d END AS T1spend_3d,
            CASE WHEN (a.T2spend_3d IS NULL) THEN 0 ELSE a.T2spend_3d END AS T2spend_3d,
            CASE WHEN (a.T3spend_3d IS NULL) THEN 0 ELSE a.T3spend_3d END AS T3spend_3d,

            CASE WHEN (b.r_60d IS NULL) THEN 0 ELSE b.r_60d END AS r_60d,
            CASE WHEN (b.f_60d IS NULL) THEN 0 ELSE b.f_60d END AS f_60d,
            CASE WHEN (b.T1_m_60d IS NULL) THEN 0 ELSE b.T1_m_60d END AS T1_m_60d,
            CASE WHEN (b.T2_m_60d IS NULL) THEN 0 ELSE b.T2_m_60d END AS T2_m_60d,
            CASE WHEN (b.T3_m_60d IS NULL) THEN 0 ELSE b.T3_m_60d END AS T3_m_60d
        FROM customer_profile_temp a LEFT JOIN rfm_60d b
        ON a.customerID = b.customerID 
        ORDER BY customerID ASC
    ''').createOrReplaceTempView('customer_profile')
    
    # Label generation: if T1spend_3d or T2spend_3d or T3spend_3d > 0, then label = 1, 2, 3
    # All other customers have label = 0

    # lABEL = 1
    spark.sql('''
        SELECT 
            customerID, 
            age, 
            Gender, 
            Income, 
            HeadofHousehold, 
            number_household, 
            months_residence, 
            T1count_30d, 
            T2count_30d, 
            T3count_30d, 
            T1spend_30d, 
            T2spend_30d, 
            T3spend_30d,
            T1count_10d, 
            T2count_10d, 
            T3count_10d, 
            T1spend_10d, 
            T2spend_10d, 
            T3spend_10d,
            T1count_3d, 
            T2count_3d, 
            T3count_3d, 
            T1spend_3d, 
            T2spend_3d, 
            T3spend_3d,
            r_60d,
            f_60d,
            T1_m_60d,
            T2_m_60d,
            T3_m_60d,
            1 as label
        FROM customer_profile 
        WHERE T1spend_3d > 0
    ''').createOrReplaceTempView('customer_profile_subset_temp1')

    # lABEL = 2
    spark.sql('''
        SELECT 
            customerID, 
            age, 
            Gender, 
            Income, 
            HeadofHousehold, 
            number_household, 
            months_residence, 
            T1count_30d, 
            T2count_30d, 
            T3count_30d, 
            T1spend_30d, 
            T2spend_30d, 
            T3spend_30d,
            T1count_10d, 
            T2count_10d, 
            T3count_10d, 
            T1spend_10d, 
            T2spend_10d, 
            T3spend_10d,
            T1count_3d, 
            T2count_3d, 
            T3count_3d, 
            T1spend_3d, 
            T2spend_3d, 
            T3spend_3d,
            r_60d,
            f_60d,
            T1_m_60d,
            T2_m_60d,
            T3_m_60d,
            2 as label
        FROM customer_profile 
        WHERE T2spend_3d > 0
    ''').createOrReplaceTempView('customer_profile_subset_temp2')

    # lABEL = 3
    spark.sql('''
        SELECT 
            customerID, 
            age, 
            Gender, 
            Income, 
            HeadofHousehold, 
            number_household, 
            months_residence, 
            T1count_30d, 
            T2count_30d, 
            T3count_30d, 
            T1spend_30d, 
            T2spend_30d, 
            T3spend_30d,
            T1count_10d, 
            T2count_10d, 
            T3count_10d, 
            T1spend_10d, 
            T2spend_10d, 
            T3spend_10d,
            T1count_3d, 
            T2count_3d, 
            T3count_3d, 
            T1spend_3d, 
            T2spend_3d, 
            T3spend_3d,
            r_60d,
            f_60d,
            T1_m_60d,
            T2_m_60d,
            T3_m_60d,
            3 as label
        FROM customer_profile 
        WHERE T3spend_3d > 0
    ''').createOrReplaceTempView('customer_profile_subset_temp3')

    # COMBINING 1, 2, 3 Subsets
    spark.sql('''
        SELECT * FROM customer_profile_subset_temp1
            UNION
        SELECT * FROM customer_profile_subset_temp2
            UNION
        SELECT * FROM customer_profile_subset_temp3

    ''').createOrReplaceTempView('customer_profile_subset_temp123')


    # LABEL = 0
    # customer_profile_subset_temp0
    spark.sql('''
        SELECT a.*, 0 as label
        FROM customer_profile a
        LEFT JOIN customer_profile_subset_temp123 b
        ON a.customerID = b.customerID
        WHERE b.customerID IS NULL
    ''').createOrReplaceTempView('customer_profile_subset_temp0')

    # GENERATE FINAL TABLE FOR CUSTOMER PROFILE 
    spark.sql('''
        SELECT * FROM  customer_profile_subset_temp0
            UNION
        SELECT * FROM customer_profile_subset_temp123
    ''').createOrReplaceTempView('customer_profile_label')

    # Save final ETL customer profile data
    # Flush hdfs folder first, if it exists and then coalesce results into final ETL file. 
    # Save final result in ASA created folder and general ETL backups timestamped. 

    # columns must be in lowercase for the pre-trained model. 
    dfCustomer_profile_label = spark \
        .sql('''
            SELECT  
                customerID as customerid, 
                age, 
                Gender as gender, 
                Income as income, 
                HeadofHousehold as headofhousehold, 
                number_household, 
                months_residence, 
                T1count_30d as t1count_30d, 
                T2count_30d as t2count_30d, 
                T3count_30d as t3count_30d, 
                T1spend_30d as t1spend_30d, 
                T2spend_30d as t2spend_30d, 
                T3spend_30d as t3spend_30d,
                T1count_10d as t1count_10d, 
                T2count_10d as t2count_10d, 
                T3count_10d as t3count_10d, 
                T1spend_10d as t1spend_10d, 
                T2spend_10d as t2spend_10d, 
                T3spend_10d as t3spend_10d,
                T1count_3d  as t1count_3d, 
                T2count_3d  as t2count_3d, 
                T3count_3d  as t3count_3d , 
                T1spend_3d  as t1spend_3d, 
                T2spend_3d  as t2spend_3d, 
                T3spend_3d  as t3spend_3d,
                r_60d,
                f_60d,
                T1_m_60d as t1_m_60d,
                T2_m_60d as t2_m_60d,
                T3_m_60d as t3_m_60d,
                label
            FROM customer_profile_label''')

    call(['hdfs', 'dfs', '-rm', '-r', 'wasb:///data/staging_etl_customer_profile']) 
    dfCustomer_profile_label \
        .coalesce(1) \
        .write \
        .option('header', 'true') \
        .mode("overwrite") \
        .format('csv') \
        .save("wasb:///data/staging_etl_customer_profile/")

    return_code = call(['hdfs', 'dfs', '-mv', 'wasb:///data/staging_etl_customer_profile/part-*.csv', '/data/staging_etl_customer_profile/etl_customer_profile.csv'])
    if return_code:
        raise FileNotFoundError('Could not rename the final etl file')

    timeslice_backup_successful = False
    etl_backup_successful = False
    backup_filename = 'customer_profile_etl_{}.csv'.format(datetime.datetime.now().strftime('%Y_%m_%dT%H_%M_%S_%fZ'))
    
    '''
    Save ETL file to ASA time slice and full ETL back up
    files are saved into timestamped folders
    wasb_dataset_path format - wasb:///data/YYYY/MM/DD/HH
    wasb_etl_backups format - wasb:///data/etl_backups

    Hdfs will fail if there is : in path name. So remove wasb:
    and replace // with underscores for archival referencing. 
    '''
    time_stamp_full = str(wasb_dataset_path).replace('/', '_') 
    time_stamp_full = time_stamp_full.replace('wasb:', '')
    time_stamp_full += datetime.datetime.now().strftime('_%M_%S')
    etl_asa_bkp_path = adf_input_timeslice_path.replace('wasb:', '')
    etl_full_bkp_path = adf_input_timeslice_path.replace('wasb:///', '')
    etl_full_bkp_path = '{}/{}'.format(str(wasb_etl_backups), etl_full_bkp_path)

    # Save to timeslice folder/hh_mm_ss
    # etl_customer_profile.csv is saved on timeslice root folder
    dfCustomer_profile_label \
        .coalesce(1) \
        .write \
        .option('header', 'true') \
        .mode("overwrite") \
        .format('csv') \
        .save(etl_asa_bkp_path)

    # Backup files to all ETL backup organized in folders by date time it was worked on 
    dfCustomer_profile_label \
         .coalesce(1) \
         .write \
         .option('header', 'true') \
         .mode("overwrite") \
         .format('csv') \
         .save(etl_full_bkp_path)

