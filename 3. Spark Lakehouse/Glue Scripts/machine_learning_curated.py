import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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
        "paths": ["s3://udacity-spark-aws-test-bucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1683654238666 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-spark-aws-test-bucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1683654238666",
)

# Script generated for node Join timestamp
Jointimestamp_node2 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1683654238666,
    keys1=["timeStamp"],
    keys2=["timeStamp"],
    transformation_ctx="Jointimestamp_node2",
)

# Script generated for node Drop Fields
DropFields_node1683654417414 = DropFields.apply(
    frame=Jointimestamp_node2,
    paths=[
        "`(right) customerName`",
        "`(right) email`",
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) phone`",
    ],
    transformation_ctx="DropFields_node1683654417414",
)

# Script generated for node Aggregate
Aggregate_node1683654563028 = sparkAggregate(
    glueContext,
    parentFrame=DropFields_node1683654417414,
    groups=["serialNumber", "timeStamp"],
    aggs=[["x", "sum"], ["y", "sum"], ["z", "sum"]],
    transformation_ctx="Aggregate_node1683654563028",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1683654563028,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-spark-aws-test-bucket/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
