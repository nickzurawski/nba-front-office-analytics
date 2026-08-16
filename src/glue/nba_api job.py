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
AmazonS3_node1786110193772 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://mgmt-599-final-project/bronze/nba_api"], "recurse": True}, transformation_ctx="AmazonS3_node1786110193772")

# Script generated for node Select Fields
SelectFields_node1786110510563 = SelectFields.apply(frame=AmazonS3_node1786110193772, paths=["PLAYER_ID", "PLAYER_NAME", "SEASON", "TEAM_ABBREVIATION", "AGE", "PLAYER_HEIGHT_INCHES", "PLAYER_WEIGHT", "GP", "MIN", "MIN_PG", "PTS_PG", "REB_PG", "AST_PG", "STL_PG", "BLK_PG", "TOV_PG", "FG_PCT_PG", "FG3_PCT_PG", "FTA_PG", "FT_PCT_PG", "TS_PCT", "USG_PCT", "OFF_RATING", "DEF_RATING", "NET_RATING", "AST_PCT", "REB_PCT", "PIE", "E_TOV_PCT", "FG3M_PG"], transformation_ctx="SelectFields_node1786110510563")

# Script generated for node Change Schema
ChangeSchema_node1786111269636 = ApplyMapping.apply(frame=SelectFields_node1786110510563, mappings=[("PLAYER_ID", "string", "PLAYER_ID", "int"), ("PLAYER_NAME", "string", "PLAYER_NAME", "string"), ("SEASON", "string", "SEASON", "string"), ("TEAM_ABBREVIATION", "string", "TEAM_ABBREVIATION", "string"), ("AGE", "string", "AGE", "double"), ("PLAYER_HEIGHT_INCHES", "string", "PLAYER_HEIGHT_INCHES", "double"), ("PLAYER_WEIGHT", "string", "PLAYER_WEIGHT", "double"), ("GP", "string", "GP", "int"), ("MIN", "string", "MIN", "double"), ("MIN_PG", "string", "MIN_PG", "double"), ("PTS_PG", "string", "PTS_PG", "double"), ("REB_PG", "string", "REB_PG", "double"), ("AST_PG", "string", "AST_PG", "double"), ("STL_PG", "string", "STL_PG", "double"), ("BLK_PG", "string", "BLK_PG", "double"), ("TOV_PG", "string", "TOV_PG", "double"), ("FG_PCT_PG", "string", "FG_PCT_PG", "double"), ("FG3_PCT_PG", "string", "FG3_PCT_PG", "double"), ("FTA_PG", "string", "FTA_PG", "double"), ("FT_PCT_PG", "string", "FT_PCT_PG", "double"), ("TS_PCT", "string", "TS_PCT", "double"), ("USG_PCT", "string", "USG_PCT", "double"), ("OFF_RATING", "string", "OFF_RATING", "double"), ("DEF_RATING", "string", "DEF_RATING", "double"), ("NET_RATING", "string", "NET_RATING", "double"), ("AST_PCT", "string", "AST_PCT", "double"), ("REB_PCT", "string", "REB_PCT", "double"), ("PIE", "string", "PIE", "double"), ("E_TOV_PCT", "string", "E_TOV_PCT", "double"), ("FG3M_PG", "string", "FG3M_PG", "double")], transformation_ctx="ChangeSchema_node1786111269636")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1786111269636, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786110155971", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786116657874 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1786111269636, connection_type="s3", format="glueparquet", connection_options={"path": "s3://mgmt-599-final-project/silver/nba_api/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1786116657874")

job.commit()