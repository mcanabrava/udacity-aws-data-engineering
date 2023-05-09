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
CustomerTrusted_node1683557320877 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-aws-test-bucket/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1683557320877",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-aws-test-bucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1683557320877,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Columns
DropColumns_node1683557442894 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropColumns_node1683557442894",
)

# Script generated for node Amazon S3
AmazonS3_node1683557511904 = glueContext.write_dynamic_frame.from_options(
    frame=DropColumns_node1683557442894,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-spark-aws-test-bucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1683557511904",
)

job.commit()
