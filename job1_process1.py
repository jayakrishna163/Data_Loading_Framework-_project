import datetime
import getpass
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark_llap import HiveWarehouseSession
from pyspark.sql.functions import col, sum
from managed_etl_logger_success_error_sak import ETLLogger



if __name__ == "__main__":
    # Main script logic

    # Hive configuration
    hive_metastore_uri = "thrift://cdp1node1.infra.alephys.com:9083"
    hive_warehouse_dir = "/warehouse/tablespace/external/hive"
    hive_warehouse_dir_logger = "/warehouse/tablespace/managed/hive"
    #process_name = "etl-job"

    # Track start time and run date
    start_time = datetime.datetime.now()

   # Main process variables
    hive_database = "etl_test"
    acl_table = "check3"
    rpt_table = "check4"
    process_name = "etl-job_test_1"
    as_of_date = datetime.date.today()
   # Instantiate ETLLogger
    etl_logger = ETLLogger(hive_metastore_uri, hive_warehouse_dir_logger, process_name)



##############


    try:
        # Build Spark session for ETL job
        spark = SparkSession.builder \
                .appName(process_name) \
                .config("hive.metastore.uris", hive_metastore_uri) \
                .config("spark.sql.warehouse.dir", hive_warehouse_dir) \
                .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://cdp1node1.infra.alephys.com:10000/default") \
                .config("spark.jars", "/opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.7.1.7.0-551.jar") \
                .enableHiveSupport() \
                .getOrCreate()

        # Main process variables
        #hive_database = "etl_test"
        #acl_table = "acl3"
        #rpt_table = "rpt2"

        # Capture initial row count before the ETL process
        initial_row_count = spark.sql("SELECT COUNT(*) AS row_count FROM {}.{}".format(hive_database, rpt_table)).collect()[0][0]

        ##### Start of ETL

        # Read data from acl table
        acl_df = spark.sql("SELECT * FROM {}.{}".format(hive_database, acl_table))

        # Transform data: subtract 2 from the age column to create age_2years_ago
        acl_df.createOrReplaceTempView("rpt_table_temp")

        rpt_df = spark.sql("""
            SELECT *, age - 2 AS age_2years_ago
           FROM rpt_table_temp 
        """)
	
        # Write transformed data to rpt table
        rpt_df.write.mode("append").format("hive").saveAsTable("{}.{}".format(hive_database, rpt_table))

        ##### End of ETL

##########

        # Calculate row count and sum of metric1 and metric2
        rows_inserted_count = rpt_df.count()
        metric1_sum = rpt_df.agg(sum(col("metric1"))).collect()[0][0]
        metric2_sum = rpt_df.agg(sum(col("metric2"))).collect()[0][0]
        sample_data = rpt_df.select("name") if rpt_df.count() > 0 else None


        # End time
        end_time = datetime.datetime.now()

        # Instantiate ETLLogger
       # etl_logger = ETLLogger(hive_metastore_uri, hive_warehouse_dir_logger, process_name)

        # Logging the operation details
        etl_logger.log_success(
            hive_database,
            rpt_table,
            initial_row_count,
            rows_inserted_count,
            start_time,
            end_time
        )

        print("Successfully logged the operation details to success_table.")




        ############### Anomaly Detection ################




        # Call detect_anomalies to show latest_metrics
        etl_logger.detect_anomalies(rpt_table, rows_inserted_count,metric1_sum,metric2_sum,sample_data,end_time)


        ####################

    except Exception as e:
        # Log error if any exception occurs
        etl_logger.log_error(str(e))
        print("Error occurred: {}".format(str(e)))

    finally:
        # Close the Spark sessions
        spark.stop()
        etl_logger.close()


