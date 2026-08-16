import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1786133874524 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://mgmt-599-final-project/bronze/salaries/cap_info"], "recurse": True}, transformation_ctx="AmazonS3_node1786133874524")

# Script generated for node Change Schema
ChangeSchema_node1786133900747 = ApplyMapping.apply(frame=AmazonS3_node1786133874524, mappings=[("season_year", "string", "season_year", "int"), ("season", "string", "season", "string"), ("salary_cap", "string", "salary_cap", "bigint"), ("luxury_tax_line", "string", "luxury_tax_line", "bigint"), ("first_apron", "string", "first_apron", "bigint"), ("second_apron", "string", "second_apron", "bigint"), ("source_status", "string", "source_status", "string"), ("notes", "string", "notes", "string")], transformation_ctx="ChangeSchema_node1786133900747")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1786133900747, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786130451133", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786134016390 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1786133900747, connection_type="s3", format="glueparquet", connection_options={"path": "s3://mgmt-599-final-project/silver/cap/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1786134016390")

job.commit()