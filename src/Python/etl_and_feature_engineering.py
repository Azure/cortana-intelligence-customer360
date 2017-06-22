import sys, datetime
from pyspark import SparkContext
from pyspark.sql import *
from subprocess import CalledProcessError, check_output, call 

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
    #### Read Userbrowsing Data as csv from blob and drop Eventhub columns
    dfBrowsingData = spark.read.csv('{}/{}'.format(str(wasb_dataset_path), '*.csv'), header=True)
    dfBrowsingData = dfBrowsingData[dfBrowsingData.userdatetime, dfBrowsingData.customerid, dfBrowsingData.category_t1, dfBrowsingData.category_t2, dfBrowsingData.category_t3]

    # Read Puchase Table 
    dfPurchase = spark.read.jdbc(url=url,table="dbo.purchase")

    # Read Puchase Table 
    dfCustomer = spark.read.jdbc(url=url,table="dbo.customer")

    # Register tables for Spark SQL Commands
    dfPurchase.registerTempTable("Purchase")
    dfCustomer.registerTempTable("Customer")
    dfBrowsingData.registerTempTable("BrowsingTPSTable")

    # SQL Aggregation (Preferred)
    agg_table_cmd = """
        SELECT 
            userDatetime, 
            customerID, 
            SUM(category_T1) AS category_T1, 
            SUM(category_T2) AS category_T2, 
            SUM(category_T3) AS category_T3 
        FROM BrowsingTPSTable 
        GROUP BY customerID, userDatetime
    """
    users_browsing_collapsed = spark.sql(agg_table_cmd)
    users_browsing_collapsed.registerTempTable("UserBrowsingTable")

    spark.sql('DROP TABLE IF EXISTS temp')
    create_temp_table = '''
        CREATE TABLE IF NOT EXISTS temp 
        (
            userDatetime STRING,
            customerID VARCHAR(10),
            ProductID VARCHAR(10),
            T1_amount_spent DECIMAL(18, 2),
            T2_amount_spent DECIMAL(18, 2),
            T3_amount_spent DECIMAL(18, 2)
        )
    '''
    spark.sql(create_temp_table)

    insert_into_temp = '''
        INSERT INTO temp
        SELECT datetime, customerID, ProductID, 
            CASE WHEN ProductID = 'T1' THEN amount_spent ELSE 0 END AS T1_amount_spent,
            CASE WHEN ProductID = 'T2' THEN amount_spent ELSE 0 END AS T2_amount_spent,
            CASE WHEN ProductID = 'T3' THEN amount_spent ELSE 0 END AS T3_amount_spent
        FROM purchase
    '''
    spark.sql(insert_into_temp)

    spark.sql('DROP TABLE IF EXISTS merge_temp')
    create_merge_temp = '''
        CREATE TABLE merge_temp
        (
            userDatetime TIMESTAMP,
            customerID STRING,
            category_T1 INT,
            category_T2 INT,
            category_T3 INT,
            amount_spent_T1 DECIMAL(18, 2),
            amount_spent_T2 DECIMAL(18, 2),
            amount_spent_T3 DECIMAL(18, 2)
        )

    '''
    spark.sql(create_merge_temp)


    insert_into_merge_temp = '''
        INSERT INTO merge_temp
            SELECT 
                b.userDatetime, 
                a.customerID, 
                a.category_T1, 
                a.category_T2, 
                a.category_T3,
                CASE WHEN (b.T1_amount_spent IS NULL) THEN 0 ELSE b.T1_amount_spent END AS amount_spent_T1,
                CASE WHEN (b.T2_amount_spent IS NULL) THEN 0 ELSE b.T2_amount_spent END AS amount_spent_T2,
                CASE WHEN (b.T3_amount_spent IS NULL) THEN 0 ELSE b.T3_amount_spent END AS amount_spent_T3
            FROM UserBrowsingTable a LEFT JOIN temp b
            ON a.customerID = b.customerID 
            ORDER BY customerID ASC, a.userDatetime ASC
    '''
    spark.sql(insert_into_merge_temp)
    spark.sql("SELECT count(*) FROM merge_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()



    spark.sql('DROP TABLE IF EXISTS subset_temp')
    create_subset_temp = '''
        CREATE TABLE subset_temp
        (
            userDatetime TIMESTAMP,
            customerID VARCHAR(10),
            category_T1 DECIMAL(18, 2),
            category_T2 DECIMAL(18, 2),
            category_T3 DECIMAL(18, 2),
            amount_spent_T1 DECIMAL(18, 2),
            amount_spent_T2 DECIMAL(18, 2),
            amount_spent_T3 DECIMAL(18, 2) 
        )

    '''
    spark.sql(create_subset_temp)



    insert_into_subset_temp = '''
        INSERT INTO subset_temp
            SELECT * FROM merge_temp
            WHERE userDatetime >= '2017-02-26 05:59:59.000'
            ORDER BY customerID ASC, userDatetime ASC
    '''
    spark.sql(insert_into_subset_temp)
    spark.sql("SELECT count(*) FROM subset_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()



    spark.sql('DROP TABLE IF EXISTS activity_3d')
    create_activity_3d_temp = '''
        CREATE TABLE activity_3d
        (
            customerID STRING,
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_activity_3d_temp)

    insert_into_activity_3d = '''
        INSERT INTO activity_3d
            SELECT
                customerID, 
                SUM(category_T1) AS T1count_3d,
                SUM(category_T2) AS T2count_3d,
                SUM(category_T3) AS T3count_3d, 
                SUM(amount_spent_T1) AS T1spend_3d,
                SUM(amount_spent_T2) AS T2spend_3d,
                SUM(amount_spent_T3) AS T3spend_3d
            FROM SUBSET_TEMP
            GROUP BY customerID
    '''
    spark.sql(insert_into_activity_3d)
    spark.sql("SELECT * FROM activity_3d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_3d WHERE t1spend_3d > 0 or t2spend_3d > 0 or t3spend_3d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_3d WHERE t1count_3d > 0 or t2count_3d > 0 or t3count_3d > 0 LIMIT 10").show()



    spark.sql("TRUNCATE TABLE subset_temp")



    insert_into_subset_30d = '''
        INSERT INTO subset_temp
            SELECT * FROM merge_temp
            WHERE userDatetime >= '2017-01-26 05:59:59.000' AND userDatetime < '2017-02-26 05:59:59.000'
            ORDER BY customerID ASC, userDatetime ASC
    '''
    spark.sql(insert_into_subset_30d)
    spark.sql("SELECT count(*) FROM subset_temp where amount_spent_t1 > 0 or amount_spent_t2 > 0 or amount_spent_t3 > 0").show()
    spark.sql("SELECT count(*) FROM subset_temp").show()



    spark.sql('DROP TABLE IF EXISTS activity_30d')
    create_activity_30d_temp = '''
        CREATE TABLE activity_30d
        (
            customerID STRING,
            T1count_30d DECIMAL(18, 2),
            T2count_30d DECIMAL(18, 2),
            T3count_30d DECIMAL(18, 2),
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_activity_30d_temp)

    insert_into_activity_30d = '''
        INSERT INTO activity_30d
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
    '''
    spark.sql(insert_into_activity_30d)
    spark.sql("SELECT * FROM activity_30d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_30d WHERE t1spend_30d > 0 or t2spend_30d > 0 or t3spend_30d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_30d WHERE t1count_30d > 0 or t2count_30d > 0 or t3count_30d > 0 LIMIT 10").show()


    spark.sql("TRUNCATE TABLE subset_temp")


    insert_into_subset_10d = '''
        INSERT INTO subset_temp
            SELECT * FROM MERGE_TEMP
            WHERE userDatetime >= '2017-02-16 05:59:59.000' and userDatetime < '2017-02-26 05:59:59.000'
            ORDER BY customerID ASC, userDatetime ASC
    '''
    spark.sql(insert_into_subset_10d)

    spark.sql('DROP TABLE IF EXISTS activity_10d')
    create_activity_10d_temp = '''
        CREATE TABLE activity_10d
        (
            customerID STRING,
            T1count_10d DECIMAL(18, 2),
            T2count_10d DECIMAL(18, 2),
            T3count_10d DECIMAL(18, 2),
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_activity_10d_temp)

    insert_into_activity_10d = '''
        insert into activity_10d
            select 
                customerID, 
                SUM(category_T1) AS T1count_10d,
                SUM(category_T2) AS T2count_10d,
                SUM(category_T3) AS T3count_10d, 
                SUM(amount_spent_T1) AS T1spend_10d,
                SUM(amount_spent_T2) AS T2spend_10d,
                SUM(amount_spent_T3) AS T3spend_10d
            FROM subset_temp
            GROUP BY customerID
    '''
    spark.sql(insert_into_activity_10d)
    spark.sql("SELECT * FROM activity_10d LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_10d WHERE t1spend_10d > 0 or t2spend_10d > 0 or t3spend_10d > 0 LIMIT 10").show()
    spark.sql("SELECT COUNT(*) FROM activity_10d WHERE t1count_10d > 0 or t2count_10d > 0 or t3count_10d > 0 LIMIT 10").show()
    

    spark.sql('DROP TABLE IF EXISTS activity_temp')
    create_activity_temp = '''
        CREATE TABLE activity_temp
        (
            customerID STRING,
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_activity_temp)

    insert_into_activity_temp = '''
        INSERT INTO activity_temp
            SELECT 
                a.customerID, 
                a.T1count_30d, a.T2count_30d, a.T3count_30d, 
                a.T1spend_30d, a.T2spend_30d, a.T3spend_30d,
                b.T1count_10d, b.T2count_10d, b.T3count_10d, 
                b.T1spend_10d, b.T2spend_10d, b.T3spend_10d
            FROM activity_30d a LEFT JOIN activity_10d b ON a.customerID = b.customerID
    '''
    spark.sql(insert_into_activity_temp)
    spark.sql("SELECT COUNT(*) FROM activity_temp LIMIT 10").show()



    spark.sql('DROP TABLE IF EXISTS activity_all')
    create_activity_all = '''
        CREATE TABLE activity_all
        (
            customerID STRING,
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_activity_all)

    insert_into_activity_all = '''
        INSERT INTO activity_all
            SELECT 
                a.customerID, 
                a.T1count_30d, a.T2count_30d, a.T3count_30d, 
                a.T1spend_30d, a.T2spend_30d, a.T3spend_30d,
                a.T1count_10d, a.T2count_10d, a.T3count_10d, 
                a.T1spend_10d, a.T2spend_10d, a.T3spend_10d,
                b.T1count_3d, b.T2count_3d, b.T3count_3d, 
                b.T1spend_3d, b.T2spend_3d, b.T3spend_3d 
            FROM activity_temp a LEFT JOIN activity_3d b ON a.customerID = b.customerID
    '''
    spark.sql(insert_into_activity_all)
    spark.sql("SELECT COUNT(*) FROM activity_all LIMIT 10").show()



    spark.sql("TRUNCATE TABLE subset_temp")

    insert_into_subset_60d = '''
        INSERT INTO subset_temp
            SELECT * FROM MERGE_TEMP
            WHERE userDatetime >= '2017-01-01 05:59:59.000' AND userDatetime < '2017-02-26 05:59:59.0000'
            ORDER BY customerID ASC, userDatetime ASC
    '''
    spark.sql(insert_into_subset_60d)



    spark.sql('DROP TABLE IF EXISTS rfm_60d')
    create_rfm_60d = '''
        CREATE TABLE rfm_60d
        (
            customerID STRING,
            r_60d  INT,
            f_60d  INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2)
        )

    '''
    spark.sql(create_rfm_60d)

    insert_into_rfm_60d = '''
        INSERT INTO rfm_60d
            SELECT 
                customerID, 
                DATEDIFF('2017-02-26 06:00:00.000', MAX(userDatetime)) AS r_60d,
                COUNT(*) AS f_60,
                SUM(amount_spent_T1) AS T1_m_60d,
                SUM(amount_spent_T2) AS T2_m_60d,
                SUM(amount_spent_T3) AS T3_m_60d
            FROM subset_temp
            GROUP BY customerID
    '''
    spark.sql(insert_into_rfm_60d)



    spark.sql('DROP TABLE IF EXISTS customer_profile_temp')
    create_customer_profile_temp = '''
        CREATE TABLE customer_profile_temp
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2)
        )

    '''
    spark.sql(create_customer_profile_temp)

    insert_into_customer_profile_temp = '''
        INSERT INTO customer_profile_temp
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
    '''
    spark.sql(insert_into_customer_profile_temp)

    spark.sql('DROP TABLE IF EXISTS customer_profile')
    create_customer_profile = '''
        CREATE TABLE customer_profile
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d  INT,
            f_60d  INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2)
        )

    '''
    spark.sql(create_customer_profile)

    insert_into_customer_profile = '''
        INSERT INTO customer_profile
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
    '''
    spark.sql(insert_into_customer_profile)



    # Label generation: if T1spend_3d or T2spend_3d or T3spend_3d > 0, then label = 1, 2, 3
    # All other customers have label = 0

    # lABEL = 1
    spark.sql('DROP TABLE IF EXISTS customer_profile_subset_temp1')
    create_customer_profile_subset_temp1 = '''
        CREATE TABLE customer_profile_subset_temp1
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_subset_temp1)

    insert_into_customer_profile_subset_temp1 = '''
        INSERT INTO customer_profile_subset_temp1
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
    '''
    spark.sql(insert_into_customer_profile_subset_temp1)

    # lABEL = 2
    spark.sql('DROP TABLE IF EXISTS customer_profile_subset_temp2')
    create_customer_profile_subset_temp2 = '''
        CREATE TABLE customer_profile_subset_temp2
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_subset_temp2)

    insert_into_customer_profile_subset_temp2 = '''
        INSERT INTO customer_profile_subset_temp2
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
    '''
    spark.sql(insert_into_customer_profile_subset_temp2)

    # lABEL = 3
    spark.sql('DROP TABLE IF EXISTS customer_profile_subset_temp3')
    create_customer_profile_subset_temp3 = '''
        CREATE TABLE customer_profile_subset_temp3
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_subset_temp3)

    insert_into_customer_profile_subset_temp3 = '''
        INSERT INTO customer_profile_subset_temp3
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
    '''
    spark.sql(insert_into_customer_profile_subset_temp3)



    # COMBINING 1, 2, 3 Subsets
    spark.sql('DROP TABLE IF EXISTS customer_profile_subset_temp123')
    create_customer_profile_subset_temp123 = '''
        CREATE TABLE customer_profile_subset_temp123
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_subset_temp123)

    insert_into_customer_profile_subset_temp123 = '''
        INSERT INTO customer_profile_subset_temp123
            SELECT * FROM customer_profile_subset_temp1
                UNION
            SELECT * FROM customer_profile_subset_temp2
                UNION
            SELECT * FROM customer_profile_subset_temp3

    '''
    spark.sql(insert_into_customer_profile_subset_temp123)



    # LABEL = 0
    spark.sql('DROP TABLE IF EXISTS customer_profile_subset_temp0')
    create_customer_profile_subset_temp0 = '''
        CREATE TABLE customer_profile_subset_temp0
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_subset_temp0)

    # insert_into_customer_profile_subset_temp0 = '''
    #     INSERT INTO customer_profile_subset_temp0
    #         SELECT a.*, 0 as label
    #         FROM customer_profile a
    #         WHERE customerID NOT IN (SELECT customerID FROM customer_profile_subset_temp123)

    # '''

    # Optimization - Register a new temporary table for the customer subset123 to avoid a cross join error
    df_temp_table = spark.sql("SELECT * FROM customer_profile_subset_temp123")
    df_temp_table.registerTempTable("temp_customer_profile_subset123")

    insert_into_customer_profile_subset_temp0 = '''
        INSERT INTO customer_profile_subset_temp0
            SELECT a.*, 0 as label
            FROM customer_profile a
            LEFT JOIN temp_customer_profile_subset123 b
            ON a.customerID = b.customerID
            WHERE b.customerID IS NULL

    '''
    spark.sql(insert_into_customer_profile_subset_temp0)

    # GENERATE FINAL TABLE FOR CUSTOMER PROFILE 
    spark.sql('DROP TABLE IF EXISTS customer_profile_label')
    create_customer_profile_label = '''
        CREATE TABLE customer_profile_label
        (
            customerID STRING,
            age INT, 
            Gender STRING,
            Income STRING,
            HeadofHousehold STRING,
            number_household INT, 
            months_residence INT, 
            T1count_30d INT,
            T2count_30d INT,
            T3count_30d INT,
            T1spend_30d DECIMAL(18, 2),
            T2spend_30d DECIMAL(18, 2),
            T3spend_30d DECIMAL(18, 2),
            T1count_10d INT,
            T2count_10d INT,
            T3count_10d INT,
            T1spend_10d DECIMAL(18, 2),
            T2spend_10d DECIMAL(18, 2),
            T3spend_10d DECIMAL(18, 2),
            T1count_3d INT,
            T2count_3d INT,
            T3count_3d INT,
            T1spend_3d DECIMAL(18, 2),
            T2spend_3d DECIMAL(18, 2),
            T3spend_3d DECIMAL(18, 2),
            r_60d INT,
            f_60d INT,
            T1_m_60d  DECIMAL(18, 2),
            T2_m_60d  DECIMAL(18, 2),
            T3_m_60d  DECIMAL(18, 2),
            label INT
        )

    '''
    spark.sql(create_customer_profile_label)

    insert_into_customer_profile_label = '''
        INSERT INTO customer_profile_label
            SELECT * FROM  customer_profile_subset_temp0
                UNION
            SELECT * FROM customer_profile_subset_temp123
    '''
    spark.sql(insert_into_customer_profile_label)

    dfCustomer_profile_label = spark.sql("SELECT * FROM customer_profile_label")

    # Save final ETL customer profile data
    # Flush hdfs folder first, if it exists and then coalesce results into final ETL file. 
    # Save final result in ASA created folder and general ETL backups timestamped. 
    call(['hdfs', 'dfs', '-rm', '-r', 'wasb:///data/staging_etl_customer_profile']) 
    dfCustomer_profile_label.coalesce(1).write.option('header', 'true').format('csv').save("wasb:///data/staging_etl_customer_profile/")
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
    if wasb_folder_not_exist(['hdfs', 'dfs', '-test', '-d', adf_input_timeslice_path]):
        dfCustomer_profile_label.coalesce(1).write.option('header', 'true').format('csv').save(etl_asa_bkp_path)
        print('[ETL/ASA BACKUP] - SUCCESSFUL!')
    else:
        print('[ETL/ASA BACKUP] - BACKUP FOLDER ALREADY EXISTS. NO ACTION PERFOMED! - {}'.format(adf_input_timeslice_path))

    # Backup files to all ETL backup organized in folders by date time it was worked on 
    if wasb_folder_not_exist(['hdfs', 'dfs', '-test', '-d', etl_full_bkp_path]):
        dfCustomer_profile_label.coalesce(1).write.option('header', 'true').format('csv').save(etl_full_bkp_path)
        print('[ETL GENERAL BACKUP] - SUCCESSFUL!')
    else:
        print('[ETL GENERAL BACKUP] - BACKUP FOLDER ALREADY EXISTS. NO ACTION PERFOMED! - {}'.format(etl_full_bkp_path))

    # Clean up temporary tables
    spark.sql("DROP TABLE IF EXISTS customer_profile")
    spark.sql("DROP TABLE IF EXISTS Purchase")
    spark.sql("DROP TABLE IF EXISTS Product")
    spark.sql("DROP TABLE IF EXISTS Customer")
    spark.sql("DROP TABLE IF EXISTS BrowsingTPSTable")
    spark.sql("DROP TABLE IF EXISTS UserBrowsingTable")
    spark.sql("DROP TABLE IF EXISTS activity_all")
    spark.sql("DROP TABLE IF EXISTS activity_10d")
    spark.sql("DROP TABLE IF EXISTS activity_30d")
    spark.sql("DROP TABLE IF EXISTS activity_3d")
    spark.sql("DROP TABLE IF EXISTS activity_temp")
    spark.sql("DROP TABLE IF EXISTS merge_temp")
    spark.sql("DROP TABLE IF EXISTS subset_temp")
    spark.sql("DROP TABLE IF EXISTS customer_profile_temp")
    spark.sql("DROP TABLE IF EXISTS rfm_60d")
    spark.sql("DROP TABLE IF EXISTS temp")
    spark.sql("DROP TABLE IF EXISTS customer_profile_subset_temp0")
    spark.sql("DROP TABLE IF EXISTS customer_profile_subset_temp1")
    spark.sql("DROP TABLE IF EXISTS customer_profile_subset_temp2")
    spark.sql("DROP TABLE IF EXISTS customer_profile_subset_temp3")
    spark.sql("DROP TABLE IF EXISTS customer_profile_subset_temp123")


