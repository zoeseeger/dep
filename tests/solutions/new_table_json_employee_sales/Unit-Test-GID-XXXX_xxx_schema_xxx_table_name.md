# Unit Test - GID-XXXX

## Overview
The test cases cover the following Jira ticket [GID-XXXX](https://jira.mecca.com.au/browse/GID-XXXX).

## DDL Unit Tests
### Structure of dwh.fct_loss_prevention_sale
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'dwh'
    AND table_name = 'fct_loss_prevention_sale';
```

### Table parameters for dwh.fct_loss_prevention_sale
```SQL
SELECT * FROM adm.table_parameters 
WHERE table_name = 'dwh.fct_loss_prevention_sale';
```

### Structure of dwh.fct_loss_prevention_sale_v
```SQL
SELECT
    a.attname AS column_name
    , pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
FROM
    pg_attribute a
JOIN pg_class t ON
    a.attrelid = t.oid
JOIN pg_namespace s ON
    t.relnamespace = s.oid
WHERE
    a.attnum > 0
    AND NOT a.attisdropped
    AND t.relname = 'fct_loss_prevention_sale_v'
    AND s.nspname = 'dwh'
ORDER BY
    a.attnum;
```

## DAG Unit Tests
### Rows of dwh.fct_loss_prevention_sale compared to source
```SQL
WITH dwh AS (
    SELECT count(*)
    FROM dwh.fct_loss_prevention_sale
),
src AS (
    SELECT count(*)
    FROM dwh.fct_sale
)
SELECT
    dwh.count = src.count AS same_count
    , dwh.count AS count
    , src.count AS count
FROM 
    dwh, src
```

### Rows of dwh.fct_loss_prevention_sale_v compared to source.
```SQL
WITH dwh AS (
    SELECT count(*)
    FROM dwh.fct_loss_prevention_sale_v
),
src AS (
    SELECT count(*)
    FROM dwh.fct_loss_prevention_sale
)
SELECT
    dwh.count = src.count AS same_count
    , dwh.count AS count
    , src.count AS count
FROM 
    dwh, src
```

