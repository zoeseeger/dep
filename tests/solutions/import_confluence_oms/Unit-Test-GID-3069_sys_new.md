# Unit Test - GID-3069

## Overview
The test cases cover the following Jira ticket [GID-3069](https://jira.mecca.com.au/browse/GID-3069).

## DDL Unit Tests
### Structure of tmp.new_orderitemadjustmentreasons
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'tmp'
    AND table_name = 'new_orderitemadjustmentreasons';
```

### Structure of sys_new.orderitemadjustmentreasons
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'sys_new'
    AND table_name = 'orderitemadjustmentreasons';
```

### Structure of tmp.new_orderitems
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'tmp'
    AND table_name = 'new_orderitems';
```

### Structure of sys_new.orderitems
```SQL
SELECT
    column_name
    , data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'sys_new'
    AND table_name = 'orderitems';
```

### Table parameters for sys_new.orderitems
```SQL
SELECT * FROM adm.table_parameters 
WHERE table_name = 'sys_new.orderitems';
```

## DAG Unit Tests
### Rows of sys_new.orderitemadjustmentreasons
```SQL
SELECT COUNT(*)
FROM sys_new.orderitemadjustmentreasons
```

```SQL
SELECT COUNT(*)
FROM tmp.new_orderitemadjustmentreasons
```
### Rows of sys_new.orderitems
```SQL
SELECT COUNT(*)
FROM sys_new.orderitems
```

```SQL
SELECT COUNT(*)
FROM tmp.new_orderitems
```
