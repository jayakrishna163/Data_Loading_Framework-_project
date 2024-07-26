from pyspark.sql import SparkSession
from pyspark_llap import HiveWarehouseSession
from pyspark.sql.types import *
from decimal import Decimal
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
import getpass
import datetime

class ETLLogger:
    def __init__(self, hive_metastore_uri, hive_warehouse_dir_logger, process_name):
        self.spark = SparkSession.builder \
            .appName(process_name) \
            .config("hive.metastore.uris", hive_metastore_uri) \
            .config("spark.sql.warehouse.dir", hive_warehouse_dir_logger) \
            .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://cdp1node1.infra.alephys.com:10000/default") \
            .config("spark.jars", "/opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.7.1.7.0-551.jar") \
            .enableHiveSupport() \
            .getOrCreate()

        self.hwc = HiveWarehouseSession.session(self.spark).build()
        self.process_name = process_name
        self.update_user = getpass.getuser()

    def log_success(self, out_db_name, out_object_name, initial_row_count, rows_inserted_count, start_ts, end_ts):
        rows_updated_count = 0
        rows_deleted_count = 0

        schema = StructType([
            StructField("process_name", StringType(), True),
            StructField("business_date", DateType(), True),
            StructField("out_db_name", StringType(), True),
            StructField("out_object_name", StringType(), True),
            StructField("rows_inserted", IntegerType(), True),
            StructField("rows_updated", IntegerType(), True),
            StructField("rows_deleted", IntegerType(), True),
            StructField("run_date", DateType(), True),
            StructField("start_ts", TimestampType(), True),
            StructField("end_ts", TimestampType(), True),
            StructField("update_date", DateType(), True),
            StructField("update_user", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        success_data = [{
            'process_name': self.process_name,
            'business_date': datetime.datetime.now().date(),
            'out_db_name': out_db_name,
            'out_object_name': out_object_name,
            'rows_inserted': rows_inserted_count,
            'rows_updated': rows_updated_count,
            'rows_deleted': rows_deleted_count,
            'run_date': start_ts.date(),
            'start_ts': start_ts,
            'end_ts': end_ts,
            'update_date': datetime.datetime.now().date(),
            'update_user': self.update_user,
            'update_ts': datetime.datetime.now()
        }]

        try:
            success_df = self.spark.createDataFrame(success_data, schema)
	    success_df.show()
            success_df.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR) \
                .option("table", "log_test.managed_success_table") \
                .mode("append") \
                .save()
        except Exception as e:
            self.log_error("Error logging success metrics: {}".format(str(e)))
            raise e

    def log_error(self, error_msg):
        truncated_error_msg = error_msg[:255] if len(error_msg) > 255 else error_msg

        schema = StructType([
            StructField("logger_name", StringType(), True),
            StructField("error_msg", StringType(), True),
            StructField("update_date", DateType(), True),
            StructField("update_user", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        error_data = [{
            'logger_name': self.process_name,
            'error_msg': truncated_error_msg,
            'update_date': datetime.datetime.now().date(),
            'update_user': self.update_user,
            'update_ts': datetime.datetime.now()
        }]

        try:
            error_df = self.spark.createDataFrame(error_data, schema)
	    error_df.show()
            error_df.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR) \
                .option("table", "log_test.managed_error_table") \
                .mode("append") \
                .save()
        except Exception as e:
            print("Error logging error message: {}".format(str(e)))

    def log_quality_control_metrics(self, table_name, metric_name, metric_flag, metric_numeric, sample_data, as_of_date, is_anomaly):
        schema = StructType([
            StructField("row_number", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_flag", StringType(), True),
            StructField("metric_numeric",IntegerType(), True),
            StructField("sample_data", StringType(), True),
            StructField("as_of_date", DateType(), True),
            StructField("is_anomaly", StringType(), True),
            StructField("update_ts", TimestampType(), True)
        ])

        try:
            existing_records = self.hwc.table("log_test.managed_row_number") \
                .where((col("table_name") == table_name) & (col("metric_name") == metric_name) & (col("as_of_date") == as_of_date)) \
                .orderBy(col("update_ts").desc()) \
                .select("row_number") \
                .limit(1) \
                .collect()

            row_number = existing_records[0]["row_number"] + 1 if existing_records else 1
	    
	    # Calculate metric_numeric for varchar metrics
            if metric_flag == 'varchar':
                metric_numeric = len(sample_data) if sample_data else None
	    elif isinstance(metric_numeric, (Decimal, float)):
		# Ensure metric_numeric is int if it is a numeric type
                metric_numeric = int(metric_numeric)

            data = [{
                'row_number': row_number,
                'table_name': table_name,
                'metric_name': metric_name,
                'metric_flag': metric_flag,
                'metric_numeric': metric_numeric,
                'sample_data': sample_data,
                'as_of_date': as_of_date,
                'is_anomaly': is_anomaly,
                'update_ts': datetime.datetime.now()
            }]

            df = self.spark.createDataFrame(data, schema)
	    df.show()
            df.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR) \
                .option("table", "log_test.managed_row_number") \
                .mode("append") \
                .save()
        except Exception as e:
            self.log_error("Error logging quality control metrics: {}".format(str(e)))
            raise e

    def detect_anomalies(self, rpt_table, rows_inserted_count,metric1_sum, metric2_sum, sample_data, end_time):
        try:
            anomaly_metrics = self.hwc.table("log_test.managed_row_number") \
                .where((col("metric_name").isin('row_count', 'metric1_sum', 'metric2_sum', 'sample_data')) & (col("table_name") == rpt_table)) \
                .orderBy(col("update_ts").desc())

            if anomaly_metrics.count() == 0:
                self.log_quality_control_metrics(rpt_table, "row_count", "numeric", rows_inserted_count, None, end_time.date(), "Yes" if rows_inserted_count == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric1_sum", "numeric", metric1_sum if rows_inserted_count != 0 else 0, None, end_time.date(), "Yes" if metric1_sum is None or metric1_sum == 0 else "No")
                self.log_quality_control_metrics(rpt_table, "metric2_sum", "numeric",metric2_sum if rows_inserted_count != 0 else 0 , None, end_time.date(), "Yes" if metric2_sum is None or metric2_sum == 0 else "No")

                # Process sample_data for varchar metrics
                sample_data_list = sample_data.select("name").rdd.flatMap(lambda x: x).collect() if sample_data else []
		#if sample_data:
		if not sample_data_list:   ##new 
                # Log anomaly for null sample_data
                   self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, None, end_time.date(), "Yes")
		else:
                    anomaly_logged = False
                    for data in sample_data_list:
                        is_anomaly = "No" if len(data) < 5 else "Yes"
                        if is_anomaly == "Yes":
                            self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, data, end_time.date(), is_anomaly)
                            anomaly_logged = True
                            break

                    if not anomaly_logged and sample_data_list:
                        # Log the first sample data if no anomalies found
                        data = sample_data_list[0]
                        self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, data, end_time.date(), "No")
            else:
                window_spec = Window.partitionBy("metric_name").orderBy(col("update_ts").desc())
                latest_metrics = anomaly_metrics.withColumn("rank", dense_rank().over(window_spec)) \
                                                .filter(col("rank") == 1) \
                                                .drop("rank")

                metrics_mapping = {
                    "row_count": rows_inserted_count,
                    "metric1_sum": metric1_sum if metric1_sum is not None else 0,
                    "metric2_sum": metric2_sum if metric1_sum is not None else 0
                }


                for row in latest_metrics.collect():
                    metric_name = row["metric_name"]
                    metric_flag = row["metric_flag"]
                    metric_numeric = row["metric_numeric"] if row["metric_numeric"] is not None else 0
                    current_sample_data = row["sample_data"]

                    if metric_name in metrics_mapping:
                        etl_value = metrics_mapping[metric_name]
                        is_anomaly = "No"

                        if metric_flag == "numeric":
                            if etl_value == 0:
                                is_anomaly = "Yes"

                            fifty_percent_more = metric_numeric * 1.5
                            fifty_percent_less = metric_numeric * 0.5

                            if etl_value > fifty_percent_more or etl_value < fifty_percent_less:
                                is_anomaly = "Yes"


                        self.log_quality_control_metrics(rpt_table, metric_name, metric_flag, etl_value if metric_flag == "numeric" else None, etl_value if metric_flag == "varchar" else None, end_time.date(), is_anomaly)

                # Process sample_data for varchar metrics
                sample_data_list = sample_data.select("name").rdd.flatMap(lambda x: x).collect() if sample_data else []
                #if sample_data:
                if not sample_data_list:   ##new 
                # Log anomaly for null sample_data
                   self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, None, end_time.date(), "Yes")
                else:
                    anomaly_logged = False
                    for data in sample_data_list:
                        is_anomaly = "No" if len(data) < 2 else "Yes"
                        if is_anomaly == "Yes":
                            self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, data, end_time.date(), is_anomaly)
                            anomaly_logged = True
                            break

                    if not anomaly_logged and sample_data_list:
                        # Log the first sample data if no anomalies found
                        data = sample_data_list[0]
                        self.log_quality_control_metrics(rpt_table, "sample_data", "varchar", None, data, end_time.date(), "No")



        except Exception as e:
            self.log_error("Error detecting anomalies: {}".format(str(e)))
            raise e

    def close(self):
        self.spark.stop()

