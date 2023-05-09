import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
        "paths": ["s3://udacity-spark-aws-test-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customers Curated
CustomersCurated_node1683642330675 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-aws-test-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1683642330675",
)

# Script generated for node Renamed keys for join
Renamedkeysforjoin_node1683652864227 = ApplyMapping.apply(
    frame=CustomersCurated_node1683642330675,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "bigint"),
        ("registrationDate", "long", "registrationDate", "bigint"),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "bigint"),
        ("phone", "string", "`(right) phone`", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "bigint"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "bigint"),
    ],
    transformation_ctx="Renamedkeysforjoin_node1683652864227",
)

# Script generated for node join
join_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=Renamedkeysforjoin_node1683652864227,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="join_node2",
)

# Script generated for node Filtering Columns
FilteringColumns_node1683642939612 = DropFields.apply(
    frame=join_node2,
    paths=[
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) phone`",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "lastUpdateDate",
    ],
    transformation_ctx="FilteringColumns_node1683642939612",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1683651938379 = DynamicFrame.fromDF(
    FilteringColumns_node1683642939612.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1683651938379",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1683651938379,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-spark-aws-test-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
