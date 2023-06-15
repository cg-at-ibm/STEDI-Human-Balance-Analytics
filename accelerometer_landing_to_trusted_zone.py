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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-analytics-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1686805697107 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-analytics-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1686805697107",
)

# Script generated for node Privacy Join
PrivacyJoin_node1686805280328 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1686805697107,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyJoin_node1686805280328",
)

# Script generated for node Drop Fields
DropFields_node1686807500870 = DropFields.apply(
    frame=PrivacyJoin_node1686805280328,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "customerName",
        "shareWithResearchAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1686807500870",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686807500870,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-analytics-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
