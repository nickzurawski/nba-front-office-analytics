CREATE DATABASE IF NOT EXISTS nba_salary_project;

CREATE EXTERNAL TABLE player_stats_silver (
    PLAYER_ID INT,
    PLAYER_NAME STRING,
    SEASON STRING,
    TEAM_ABBREVIATION STRING,
    AGE DOUBLE,
    PLAYER_HEIGHT_INCHES DOUBLE,
    PLAYER_WEIGHT_POUNDS DOUBLE,
    GP INT,
    MIN DOUBLE,
    MIN_PG DOUBLE,
    PTS_PG DOUBLE,
    REB_PG DOUBLE,
    AST_PG DOUBLE,
    STL_PG DOUBLE,
    BLK_PG DOUBLE,
    TOV_PG DOUBLE,
    FG_PCT DOUBLE,
    FG3_PCT DOUBLE,
    FT_PCT DOUBLE,
    TS_PCT DOUBLE,
    USG_PCT DOUBLE,
    OFF_RATING DOUBLE,
    DEF_RATING DOUBLE,
    NET_RATING DOUBLE,
    AST_PCT DOUBLE,
    REB_PCT DOUBLE,
    E_TOV_PCT DOUBLE,
    PIE DOUBLE
)
STORED AS PARQUET
LOCATION 's3://mgmt-599-final-project/silver/nba_api/';

SELECT COUNT(*)
FROM player_stats_silver;

SELECT
    season,
    COUNT(*) AS players
FROM player_stats_silver
GROUP BY season
ORDER BY season;

SELECT *
FROM player_stats_silver
LIMIT 10;

CREATE EXTERNAL TABLE basketball_reference_silver (
    season INT,
    player STRING,
    player_id STRING,
    team STRING,
    pos STRING,
    per DOUBLE,
    ows DOUBLE,
    dws DOUBLE,
    ws DOUBLE,
    ws_48 DOUBLE,
    obpm DOUBLE,
    dbpm DOUBLE,
    bpm DOUBLE,
    vorp DOUBLE
)
STORED AS PARQUET
LOCATION 's3://mgmt-599-final-project/silver/basketball_reference/';

SELECT COUNT(*)
FROM basketball_reference_silver;

SELECT 
    season,
    COUNT(*) AS rows
FROM basketball_reference_silver
GROUP BY season
ORDER BY season;

SELECT *
FROM basketball_reference_silver
LIMIT 10;

SELECT *
FROM basketball_reference_silver
WHERE player = 'LeBron James'
ORDER BY season;

CREATE EXTERNAL TABLE player_salaries_silver (
    season_year INT,
    season STRING,
    player_id DOUBLE,
    player STRING,
    salary BIGINT,
    team_id INT,
    team STRING,
    team_option BOOLEAN,
    player_option BOOLEAN,
    qualifying_offer BOOLEAN,
    two_way BOOLEAN,
    terminated BOOLEAN,
    notes STRING,
    source STRING
)
STORED AS PARQUET
LOCATION 's3://mgmt-599-final-project/silver/salaries/';

SELECT COUNT(*)
FROM player_salaries_silver;

SELECT * 
FROM player_salaries_silver
LIMIT 10;

SELECT 
    season,
    COUNT(*) AS rows
FROM player_salaries_silver
GROUP BY season
ORDER BY season;

SELECT 
    player,
    season,
    COUNT(*) AS salary_rows
FROM player_salaries_silver
GROUP BY player, season
HAVING COUNT(*) > 1
ORDER BY salary_rows DESC, player
LIMIT 50;

SELECT 
    player,
    season,
    COUNT(*) AS salary_rows,
    SUM(salary) AS total_salary
FROM player_salaries_silver
GROUP BY player, season
HAVING COUNT(*) > 1 
ORDER BY total_salary DESC 
LIMIT 50;

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN player IS NULL THEN 1 ELSE 0 END) AS missing_player,
    SUM(CASE WHEN season IS NULL THEN 1 ELSE 0 END) AS missing_season,
    SUM(CASE WHEN salary IS NULL THEN 1 ELSE 0 END) AS missing_salary
FROM player_salaries_silver;

SELECT *
FROM player_salaries_silver
WHERE player = 'John Wall'
  AND season = '2022-23';
  
CREATE EXTERNAL TABLE cap_silver (
    season_year INT,
    season STRING,
    salary_cap BIGINT,
    luxury_tax_line BIGINT,
    first_apron BIGINT,
    second_apron BIGINT,
    source_status STRING,
    notes STRING
)
STORED AS PARQUET
LOCATION 's3://mgmt-599-final-project/silver/cap/';

SELECT COUNT(*)
FROM cap_silver;

SELECT *
FROM cap_silver
ORDER BY season_year;

WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT *
    FROM bref_ranked
    WHERE rn = 1
),

nba_with_end_year AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year
    FROM player_stats_silver
)

SELECT
    COUNT(*) AS nba_player_seasons,
    COUNT(b.player) AS matched_player_seasons,
    COUNT(*) - COUNT(b.player) AS unmatched_player_seasons,
    ROUND(
        100.0 * COUNT(b.player) / COUNT(*),
        2
    ) AS match_rate_pct
FROM nba_with_end_year n
LEFT JOIN bref_one_row b
    ON n.player_name = b.player
    AND n.season_end_year = b.season;
    
WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT *
    FROM bref_ranked
    WHERE rn = 1
),

nba_with_end_year AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year
    FROM player_stats_silver
)

SELECT
    n.player_name,
    n.season,
    n.team_abbreviation
FROM nba_with_end_year n
LEFT JOIN bref_one_row b
    ON n.player_name = b.player
    AND n.season_end_year = b.season
WHERE b.player IS NULL
ORDER BY n.season, n.player_name;

WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT
        *,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\\b(jr|sr|ii|iii|iv)\\b',
                ''
            )
        ) AS normalized_name
    FROM bref_ranked
    WHERE rn = 1
),

nba_normalized AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player_name),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\\b(jr|sr|ii|iii|iv)\\b',
                ''
            )
        ) AS normalized_name
    FROM player_stats_silver
)

SELECT
    COUNT(*) AS nba_player_seasons,
    COUNT(b.player) AS matched_player_seasons,
    COUNT(*) - COUNT(b.player) AS unmatched_player_seasons,
    ROUND(
        100.0 * COUNT(b.player) / COUNT(*),
        2
    ) AS match_rate_pct
FROM nba_normalized n
LEFT JOIN bref_one_row b
    ON n.normalized_name = b.normalized_name
    AND n.season_end_year = b.season;
    
WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT
        *,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\\b(jr|sr|ii|iii|iv)\\b',
                ''
            )
        ) AS normalized_name
    FROM bref_ranked
    WHERE rn = 1
),

nba_normalized AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player_name),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\\b(jr|sr|ii|iii|iv)\\b',
                ''
            )
        ) AS normalized_name
    FROM player_stats_silver
)

SELECT DISTINCT
    n.player_name,
    n.normalized_name,
    n.season,
    n.team_abbreviation
FROM nba_normalized n
LEFT JOIN bref_one_row b
    ON n.normalized_name = b.normalized_name
    AND n.season_end_year = b.season
WHERE b.player IS NULL
ORDER BY n.player_name, n.season;

SELECT
    player_name,
    trim(
        regexp_replace(
            regexp_replace(
                lower(player_name),
                '[^a-z0-9 ]',
                ''
            ),
            '\b(jr|sr|ii|iii|iv)\b',
            ''
        )
    ) AS normalized_name
FROM player_stats_silver
WHERE player_name LIKE '%Butler%';

WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT
        *,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM bref_ranked
    WHERE rn = 1
),

nba_normalized AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player_name),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM player_stats_silver
)

SELECT
    COUNT(*) AS nba_player_seasons,
    COUNT(b.player) AS matched_player_seasons,
    COUNT(*) - COUNT(b.player) AS unmatched_player_seasons,
    ROUND(
        100.0 * COUNT(b.player) / COUNT(*),
        2
    ) AS match_rate_pct
FROM nba_normalized n
LEFT JOIN bref_one_row b
    ON n.normalized_name = b.normalized_name
    AND n.season_end_year = b.season;
    
WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT
        *,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM bref_ranked
    WHERE rn = 1
),

nba_normalized AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player_name),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM player_stats_silver
)

SELECT DISTINCT
    n.player_name,
    n.normalized_name,
    n.season,
    n.team_abbreviation
FROM nba_normalized n
LEFT JOIN bref_one_row b
    ON n.normalized_name = b.normalized_name
    AND n.season_end_year = b.season
WHERE b.player IS NULL
ORDER BY n.player_name, n.season;

WITH bref_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player, season
            ORDER BY
                CASE
                    WHEN regexp_like(team, '^[2-9]TM$') THEN 0
                    ELSE 1
                END
        ) AS rn
    FROM basketball_reference_silver
),

bref_one_row AS (
    SELECT
        *,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM bref_ranked
    WHERE rn = 1
),

nba_normalized AS (
    SELECT
        *,
        CAST(substr(season, 1, 4) AS INTEGER) + 1 AS season_end_year,
        trim(
            regexp_replace(
                regexp_replace(
                    lower(player_name),
                    '[^a-z0-9 ]',
                    ''
                ),
                '\b(jr|sr|ii|iii|iv)\b',
                ''
            )
        ) AS normalized_name
    FROM player_stats_silver
)

SELECT
    n.player_name,
    COUNT(*) AS unmatched_seasons
FROM nba_normalized n
LEFT JOIN bref_one_row b
    ON n.normalized_name = b.normalized_name
    AND n.season_end_year = b.season
WHERE b.player IS NULL
GROUP BY n.player_name
ORDER BY unmatched_seasons DESC, n.player_name;

SHOW TABLES IN nba_salary_project;