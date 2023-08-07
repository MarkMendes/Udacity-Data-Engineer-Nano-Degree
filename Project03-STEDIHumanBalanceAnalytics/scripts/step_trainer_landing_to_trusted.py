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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mm-udacity-de-nano/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1691168895569 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mm-udacity-de-nano/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1691168895569",
)

# Script generated for node Join Customer Curated to Step Trainer
JoinCustomerCuratedtoStepTrainer_node1691168921910 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1691168895569,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinCustomerCuratedtoStepTrainer_node1691168921910",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node1691169172841 = DropFields.apply(
    frame=JoinCustomerCuratedtoStepTrainer_node1691168921910,
    paths=[
        "email",
        "phone",
        "`.serialNumber`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "lastUpdateDate",
        "customerName",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropCustomerFields_node1691169172841",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1691169225247 = glueContext.getSink(
    path="s3://mm-udacity-de-nano/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1691169225247",
)
StepTrainerTrusted_node1691169225247.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1691169225247.setFormat("json")
StepTrainerTrusted_node1691169225247.writeFrame(DropCustomerFields_node1691169172841)
job.commit()
