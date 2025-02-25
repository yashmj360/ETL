import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

temp_dir = args['TempDir']

MYSQL_CONNECTION = "Aurora connection"
REDSHIFT_CONNECTION = "Redshift connection"

# Create dynamic frames from source tables
accounts_df = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": True,
                "dbtable": "accounts",
                "connectionName": MYSQL_CONNECTION
            }
        )

orders_df = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": True,
                "dbtable": "orders",
                "connectionName": MYSQL_CONNECTION
            }
        )
sales_reps_df = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": True,
                "dbtable": "sales_reps",
                "connectionName": MYSQL_CONNECTION
            }
        )

region_df = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": True,
                "dbtable": "region",
                "connectionName": MYSQL_CONNECTION
            }
        )
     
web_events_df = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": True,
                "dbtable": "web_events",
                "connectionName": MYSQL_CONNECTION
            }
        )

# Convert to Spark DataFrames for easier transformation
accounts = accounts_df.toDF()
orders = orders_df.toDF()
sales_reps = sales_reps_df.toDF()
region = region_df.toDF()
web_events = web_events_df.toDF()

# Join to create the "top_customers_by_revenue" table
customer_revenue = orders.join(accounts, orders.account_id == accounts.id) \
    .groupby("account_id", "name") \
    .agg(spark_sum("total_amt_usd").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

# Join to create the "highest_glossy_purchasers" table
glossy_customers = orders.join(accounts, orders.account_id == accounts.id) \
    .groupby("account_id", "name") \
    .agg(spark_sum("gloss_amt_usd").alias("glossy_spending")) \
    .orderBy(col("glossy_spending").desc())

# Convert back to DynamicFrames
customer_revenue_dyf = DynamicFrame.fromDF(customer_revenue, glueContext, "customer_revenue_dyf")
glossy_customers_dyf = DynamicFrame.fromDF(glossy_customers, glueContext, "glossy_customers_dyf")

table_name = "customer_revenue_report"

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=customer_revenue_dyf,
    catalog_connection="Redshift connection",
    connection_options={
        "dbtable": table_name,
        "database": "dev",
        "redshiftTmpDir": temp_dir
    }
)

table_name = "glossy_paper_customers"
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=glossy_customers_dyf,
    catalog_connection="Redshift connection",
    connection_options={
        "dbtable": table_name,
        "database": "dev",
        "redshiftTmpDir": temp_dir
    }
)

job.commit()
