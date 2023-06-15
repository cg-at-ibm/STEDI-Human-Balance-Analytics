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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-analytics-project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1686810137615 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-analytics-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1686810137615",
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

# Script generated for node Join
Join_node1686810818658 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=CustomerTrusted_node1686805697107,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1686810818658",
)

# Script generated for node Drop Fields
DropFields_node1686810964962 = DropFields.apply(
    frame=Join_node1686810818658,
    paths=["z", "user", "y", "x", "serialNumber"],
    transformation_ctx="DropFields_node1686810964962",
)

# Script generated for node Join
Join_node1686811005686 = Join.apply(
    frame1=DropFields_node1686810964962,
    frame2=StepTrainerLanding_node1686810137615,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1686811005686",
)

# Script generated for node Drop Fields
DropFields_node1686811078395 = DropFields.apply(
    frame=Join_node1686811005686,
    paths=[
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "timeStamp",
    ],
    transformation_ctx="DropFields_node1686811078395",
)

# Script generated for node Step Trainer Trusted
AmazonS3_node1686811239583 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686811078395,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-analytics-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1686811239583",
)

job.commit()
