import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import unicodedata
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Silver tables from the Glue Data Catalog

nba = glueContext.create_dynamic_frame.from_catalog(
    database="nba_salary_project",
    table_name="player_stats_silver"
).toDF()

bref = glueContext.create_dynamic_frame.from_catalog(
    database="nba_salary_project",
    table_name="basketball_reference_silver"
).toDF()

salaries = glueContext.create_dynamic_frame.from_catalog(
    database="nba_salary_project",
    table_name="player_salaries_silver"
).toDF()

cap = glueContext.create_dynamic_frame.from_catalog(
    database="nba_salary_project",
    table_name="cap_silver"
).toDF()

def normalize_name(name):
    if name is None:
        return None

    # Separate accented characters into base character + accent
    name = unicodedata.normalize("NFKD", name)

    # Remove accent marks
    name = "".join(
        char for char in name
        if not unicodedata.combining(char)
    )

    # Convert to lowercase
    name = name.lower()

    # Remove punctuation
    name = re.sub(r"[^a-z0-9\s]", "", name)

    # Remove common suffixes
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", name)

    # Collapse extra spaces
    name = re.sub(r"\s+", " ", name).strip()

    return name


# Make the Python function usable on a Spark DataFrame column
normalize_name_udf = F.udf(normalize_name, StringType())

# Normalize Basketball Reference player names
bref = bref.withColumn(
    "normalized_name",
    normalize_name_udf(F.col("player"))
)

# Rank Basketball Reference rows within each player-season
bref_window = Window.partitionBy(
    "normalized_name",
    "season"
).orderBy(
    F.when(
        F.col("team").rlike(r"^[2-9]TM$"),
        0
    ).otherwise(1)
)

# Keep one Basketball Reference row per player-season
bref = (
    bref
    .withColumn(
        "row_rank",
        F.row_number().over(bref_window)
    )
    .filter(
        F.col("row_rank") == 1
    )
    .drop("row_rank")
)

# Normalize salary player names

salaries = salaries.withColumn(
    "normalized_name",
    normalize_name_udf(F.col("player"))
)


# Aggregate multiple salary records to one player-season

salaries = (
    salaries
    .groupBy(
        "normalized_name",
        "season",
        "season_year"
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.first("player").alias("salary_player_name")
    )
)

nba = (
    nba
    .withColumn(
        "canonical_name",
        normalize_name_udf(F.col("PLAYER_NAME"))
    )
    .withColumn(
        "season_end_year",
        F.substring(F.col("SEASON"), 1, 4).cast("int") + 1
    )
    .withColumn(
        "TOTAL_MINUTES",
        F.col("GP") * F.col("MIN_PG")
    )
)

# Create a Basketball Reference-specific match key

nba = nba.withColumn(
    "bref_match_name",
    F.when(F.col("canonical_name") == "cam reynolds", "cameron reynolds")
     .when(F.col("canonical_name") == "matt hurt", "matthew hurt")
     .when(F.col("canonical_name") == "nigel hayesdavis", "nigel hayes")
     .when(F.col("canonical_name") == "ronald holland", "ron holland")
     .when(F.col("canonical_name") == "vincent hunter", "vince hunter")
     .when(F.col("canonical_name") == "omer asik", "omer ask")
     .when(F.col("canonical_name") == "adama bal", "adamaalpha bal")
     .when(F.col("canonical_name") == "cui cui", "cui yongxi")
     .when(F.col("canonical_name") == "nate mensah", "nathan mensah")
     .when(F.col("canonical_name") == "mitchell creek", "mitch creek")
     .when(F.col("canonical_name") == "egor demin", "egor dmin")
     .otherwise(F.col("canonical_name"))
)

# Start salary matching from the neutral canonical name

nba = nba.withColumn(
    "salary_match_name",
    F.when(F.col("canonical_name") == "brandon boston", "bj boston")
     .when(F.col("canonical_name") == "bub carrington", "carlton carrington")
     .when(F.col("canonical_name") == "cam whitmore", "cameron whitmore")
     .when(F.col("canonical_name") == "herbert jones", "herb jones")
     .when(F.col("canonical_name") == "ish smith", "ishmael smith")
     .when(F.col("canonical_name") == "jj barea", "jose juan barea")
     .when(F.col("canonical_name") == "joshua primo", "josh primo")
     .when(F.col("canonical_name") == "juancho hernangomez", "juan hernangomez")
     .when(F.col("canonical_name") == "lou williams", "louis williams")
     .when(F.col("canonical_name") == "maurice harkless", "moe harkless")
     .when(F.col("canonical_name") == "nic claxton", "nicolas claxton")
     .when(F.col("canonical_name") == "ish wainright", "ishmail wainright")
     .when(F.col("canonical_name") == "patty mills", "patrick mills")
     .when(F.col("canonical_name") == "ronald holland", "ron holland")
     .when(F.col("canonical_name") == "santi aldama", "santiago aldama")
     .when(F.col("canonical_name") == "svi mykhailiuk", "sviatoslav mykhailiuk")
     .when(F.col("canonical_name") == "wes iwundu", "wesley iwundu")
     .when(F.col("canonical_name") == "joe young", "joseph young")
     .when(F.col("canonical_name") == "mo williams", "maurice williams")

     # Scotty/Scottie Pippen spelling differs by season in salary data
     .when(
         (F.col("canonical_name") == "scotty pippen") &
         (F.col("SEASON") == "2023-24"),
         "scottie pippen"
     )

     .otherwise(F.col("canonical_name"))
)

nba_alias = nba.alias("nba")
bref_alias = bref.alias("bref")

nba_bref = nba_alias.join(
    bref_alias,
    (F.col("nba.bref_match_name") == F.col("bref.normalized_name")) &
    (F.col("nba.season_end_year") == F.col("bref.season")),
    how="left"
)

# Create a clean NBA + BRef dataset for downstream joins

nba_bref_clean = nba_bref.select(
    F.col("nba.*"),
    F.col("bref.player").alias("bref_player"),
    F.col("bref.per").alias("bref_per"),
    F.col("bref.ows").alias("bref_ows"),
    F.col("bref.dws").alias("bref_dws"),
    F.col("bref.ws").alias("bref_ws"),
    F.col("bref.ws_48").alias("bref_ws_48"),
    F.col("bref.obpm").alias("bref_obpm"),
    F.col("bref.dbpm").alias("bref_dbpm"),
    F.col("bref.bpm").alias("bref_bpm"),
    F.col("bref.vorp").alias("bref_vorp")
)

# Join salary data

nba_bref_salary = nba_bref_clean.alias("nb").join(
    salaries.alias("sal"),
    (F.col("nb.salary_match_name") == F.col("sal.normalized_name")) &
    (F.col("nb.SEASON") == F.col("sal.season")),
    how="left"
)

# Create a clean NBA + BRef + Salary dataset for downstream joins

nba_bref_salary_clean = nba_bref_salary.select(
    F.col("nb.*"),
    F.col("sal.total_salary").alias("total_salary"),
    F.col("sal.salary_player_name").alias("salary_player_name")
)

# Join salary cap data by season

nba_full_joined = nba_bref_salary_clean.alias("nbs").join(
    cap.alias("cap"),
    F.col("nbs.SEASON") == F.col("cap.season"),
    how="left"
)

# Create a clean NBA + BRef + Salary + Cap dataset
# without duplicate season fields

nba_full = nba_full_joined.select(
    F.col("nbs.*"),
    F.col("cap.salary_cap").alias("salary_cap"),
    F.col("cap.luxury_tax_line").alias("luxury_tax_line"),
    F.col("cap.first_apron").alias("first_apron"),
    F.col("cap.second_apron").alias("second_apron")
)

# Create normalized salary measure

nba_full = nba_full.withColumn(
    "salary_pct_cap",
    F.col("total_salary") / F.col("salary_cap")
)

# Create preliminary analysis sample

analysis_df = nba_full.filter(
    (F.col("TOTAL_MINUTES") >= 500) &
    F.col("total_salary").isNotNull() &
    F.col("salary_cap").isNotNull()
)

print("Rows before cap join:", nba_bref_salary_clean.count())
print("Rows after cap join:", nba_full.count())

print(
    "Unmatched cap rows:",
    nba_full.filter(F.col("salary_cap").isNull()).count()
)

print(
    "Analysis sample rows:",
    analysis_df.count()
)

print("Salary percent of cap spot checks:")

analysis_df.select(
    "PLAYER_NAME",
    "SEASON",
    "total_salary",
    "salary_cap",
    "salary_pct_cap"
).orderBy(
    F.col("salary_pct_cap").desc()
).show(25, truncate=False)

print("Duplicate player-seasons in analysis sample:")

analysis_df.groupBy(
    "PLAYER_ID",
    "SEASON"
).count().filter(
    F.col("count") > 1
).show(50, truncate=False)

# ============================================================
# Create final Gold analysis dataset
# ============================================================

gold = analysis_df.select(
    # Identifiers
    "PLAYER_ID",
    "PLAYER_NAME",
    "SEASON",
    "TEAM_ABBREVIATION",

    # Player characteristics and playing time
    "AGE",
    "PLAYER_HEIGHT_INCHES",
    "PLAYER_WEIGHT",
    "GP",
    "MIN_PG",
    "TOTAL_MINUTES",

    # Traditional production
    "PTS_PG",
    "REB_PG",
    "AST_PG",
    "STL_PG",
    "BLK_PG",
    "TOV_PG",
    "FG_PCT_PG",
    "FG3_PCT_PG",
    "FG3M_PG",
    "FTA_PG",
    "FT_PCT_PG",

    # NBA advanced metrics
    "TS_PCT",
    "USG_PCT",
    "OFF_RATING",
    "DEF_RATING",
    "NET_RATING",
    "AST_PCT",
    "REB_PCT",
    "PIE",
    "E_TOV_PCT",

    # Basketball Reference advanced metrics
    "bref_per",
    "bref_ows",
    "bref_dws",
    "bref_ws",
    "bref_ws_48",
    "bref_obpm",
    "bref_dbpm",
    "bref_bpm",
    "bref_vorp",

    # Salary and cap
    "total_salary",
    "salary_cap",
    "salary_pct_cap"
)


# ============================================================
# Validate Gold dataset
# ============================================================

print("Gold row count:", gold.count())
print("Gold column count:", len(gold.columns))

print("Gold schema:")
gold.printSchema()

print("Gold preview:")
gold.show(20, truncate=False)

print("Gold null counts:")

gold.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in gold.columns
]).show(truncate=False)


print("Duplicate player-seasons in Gold:")

gold.groupBy(
    "PLAYER_ID",
    "SEASON"
).count().filter(
    F.col("count") > 1
).show(50, truncate=False)


print("Gold salary sanity checks:")

gold.select(
    "PLAYER_NAME",
    "SEASON",
    "total_salary",
    "salary_cap",
    "salary_pct_cap"
).orderBy(
    F.col("salary_pct_cap").desc()
).show(25, truncate=False)


# ============================================================
# Write Gold dataset to S3
# ============================================================

gold_output_path = "s3://mgmt-599-final-project/gold/modeling/"

(
    gold
    .write
    .mode("overwrite")
    .parquet(gold_output_path)
)

print("Gold dataset written successfully to:", gold_output_path)

job.commit()