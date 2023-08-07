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
CustomerTrusted_node1691000425774 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mm-udacity-de-nano/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1691000425774",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mm-udacity-de-nano/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Customer Privacy Filter
JoinCustomerPrivacyFilter_node1691000372841 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1691000425774,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerPrivacyFilter_node1691000372841",
)

# Script generated for node Drop Fields
DropFields_node1691000897451 = DropFields.apply(
    frame=JoinCustomerPrivacyFilter_node1691000372841,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1691000897451",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1691000552669 = glueContext.getSink(
    path="s3://mm-udacity-de-nano/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1691000552669",
)
AccelerometerTrusted_node1691000552669.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1691000552669.setFormat("json")
AccelerometerTrusted_node1691000552669.writeFrame(DropFields_node1691000897451)
job.commit()
