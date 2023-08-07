import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1691000425774 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1691000425774",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Customer Trusted To Accelerometer Landing
JoinCustomerTrustedToAccelerometerLanding_node1691000372841 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1691000425774,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerTrustedToAccelerometerLanding_node1691000372841",
)

# Script generated for node Drop Fields
DropFields_node1691000897451 = DropFields.apply(
    frame=JoinCustomerTrustedToAccelerometerLanding_node1691000372841,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1691000897451",
)

# Script generated for node Customer Curated
CustomerCurated_node1691000552669 = glueContext.getSink(
    path="s3://mm-udacity-de-nano/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1691000552669",
)
CustomerCurated_node1691000552669.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1691000552669.setFormat("json")
CustomerCurated_node1691000552669.writeFrame(DropFields_node1691000897451)
job.commit()
