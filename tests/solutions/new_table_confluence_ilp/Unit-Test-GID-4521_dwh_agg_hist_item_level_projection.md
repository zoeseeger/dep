# Unit Test - GID-4521

## Overview
The test cases cover the following Jira ticket [GID-4521](https://jira.mecca.com.au/browse/GID-4521).

## DDL Unit Tests
### Structure of dwh.agg_hist_item_level_projection
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'dwh'
    AND table_name = 'agg_hist_item_level_projection';
```

### Table parameters for dwh.agg_hist_item_level_projection
```SQL
SELECT * FROM adm.table_parameters 
WHERE table_name = 'dwh.agg_hist_item_level_projection';
```

## DAG Unit Tests
### Rows of dwh.agg_hist_item_level_projection compared to source
```SQL
WITH dwh AS (
    SELECT count(*)
    FROM dwh.agg_hist_item_level_projection
),
src AS (
    SELECT count(*)
    FROM dwh.dim_date
)
SELECT
    dwh.count = src.count AS same_count
    , dwh.count AS count
    , src.count AS count
FROM 
    dwh, src
```

