import sys
import datetime

from src.functions import writeLinesFile


class DagInput:
    """Class defining input of the DAG file."""

    def __init__(self, options: object):

        self.action = options.action
        self.user = options.user
        self.user_email = options.user_email
        self.table_structures = options.table_structures
        self.view_structures = options.view_structures
        self.staging_table = options.staging_table
        self.dag_file = options.dag_file
        self.output_dir = options.output_dir

        # defined in subsequent functions
        self.lines = []
        self.table = None
        self.view = None
        self.tasks = []
        self.queries = []

        self.run()

    def run(self):
        """Determine input for DAG."""

        self.header()

        # tables
        for self.table in self.table_structures:

            action = self.action
            method = self.table.import_method
            final = self.table.final_table
            staging = self.staging_table

            schema = self.table.schema
            table_name = self.table.table_name

            # new_table incremental
            if action == "new_table" and method == "incremental" and staging:

                # staging
                if not final:
                    self.addTaskAsFunction(
                        "insert",
                        f"Insert and incremental load from source -> {schema}.{table_name}.",
                    )
                    self.glindaHook()
                    self.getIncrementFromSQL()
                    self.saveIncrementToSQL()
                    self.truncateSQL()
                    self.insertSQL(where=True)
                    self.runQueryCommand()

                # main
                else:
                    self.addTaskAsFunction(
                        "upsert", f"Upsert from stg -> {schema}.{table_name}.", dependent_task=True
                    )
                    self.glindaHook()
                    self.upsertSQL(where=False)
                    self.incrementFrom2AsIncrementFromSQL()
                    self.runQueryCommand()

            # new_table incremental main no staging table
            elif action == "new_table" and method == "incremental" and not staging:

                self.addTaskAsFunction(
                    "upsert", f"Upsert from source -> {schema}.{table_name}."
                )
                self.glindaHook()
                self.getIncrementFromSQL()
                self.upsertSQL(where=True)
                self.updateIncrementFromSQL()
                self.runQueryCommand()

            # import incremental
            elif action == "import" and method == "incremental":

                # tmp
                if not final:
                    self.addTaskAsFunction(
                        "extract_to_tmp",
                        f"Extract and incremental load from source -> {schema}.{table_name}.",
                    )
                    self.glindaHook()
                    self.getIncrementFromSQL()
                    self.selectSQL(where=True)
                    self.glindaHelper()
                    self.saveIncrementToSQL()
                    self.runQueryCommand()

                # main
                else:
                    self.addTaskAsFunction(
                        "upsert_to_sys", f"Upsert from tmp -> {schema}.{table_name}.", dependent_task=True
                    )
                    self.glindaHook()
                    self.upsertSQL(where=False)
                    self.incrementFrom2AsIncrementFromSQL()
                    self.runQueryCommand()

            # import full load tmp
            elif action == "import" and method == "full":

                # tmp
                if not final:
                    self.addTaskAsFunction(
                        "extract_to_tmp",
                        f"Extract and full load from source -> {schema}.{table_name}.",
                    )
                    self.selectSQL(where=False)
                    self.glindaHelper()

                # main
                else:
                    self.addTaskAsFunction(
                        "upsert_to_sys",
                        f"Upsert from tmp -> {schema}.{table_name} with soft delete.", dependent_task=True
                    )
                    self.glindaHook()
                    self.upsertSQL(where=False)
                    self.softDeleteSQL()
                    self.runQueryCommand()

            # import from landing table to sys - assuming incremental update - no staging
            elif action == "import" and method == "fivetran legacy":

                self.addTaskAsFunction(
                    "insert_to_psa", f"Insert from landing -> {schema}.{table_name}."
                )
                self.glindaHook()
                self.getIncrementFromSQL()
                self.insertToPersistantStaging() ###
                self.updateIncrementFromSQL()
                self.runQueryCommand()

            else:
                sys.exit(
                    f"Combination of action={action}, import_method={method} has not been written."
                )

        # views
        for self.view in self.view_structures:

            if self.view.materialized:
                self.addTaskAsFunction(
                    "refresh_materialized_view",
                    f"Refresh materialized view -> {self.view.schema}.{self.view.view_name}.",
                )
                self.glindaHook()
                self.refreshMaterializedView()
                self.runQueryCommand()

        self.addTasksOfDAG()
        writeLinesFile(self.output_dir, self.dag_file, self.lines)

    def addTaskAsFunction(self, typ, description, dependent_task=False, table=True):
        """New task as python function in DAG."""

        if table:
            name = self.table.table_name
        else:
            name = self.view.view_name

        self.lines.extend(
            [
                f"",
                f"def {typ}_{name}(**kwargs):",
                f'    """{description}"""',
                f"",
            ]
        )

        # add to list of tasks
        if dependent_task:
            self.tasks[-1].append(f"{typ}_{name}")
        else:
            self.tasks.append([f"{typ}_{name}"])

    def glindaHook(self):
        """Add glinda hook."""

        self.lines.extend([f"    glinda_hook = PostgresHook(glinda_conn_id)", f""])

    def getIncrementFromSQL(self):
        """Add increment from line."""

        # use timestamp for use with mysql database
        if self.action == "import":
            date_type = "timestamp"
        else:
            date_type = "timestamptz"

        # table name of final table
        if self.table.final_table:
            name = self.table.schema_table
        else:
            name = self.table.base_schema_table

        self.lines.extend(
            [
                f'    increment_from, increment_to = glinda_hook.get_first(',
                f'        """',
                f"        SELECT",
                f"            increment_from::{date_type}-overlap::interval",
                f"            , now()::{date_type}",
                f"        FROM",
                f"            adm.table_parameters",
                f"        WHERE",
                f"            table_name = '{name}';",
                f'        """',
                f'    )',
                f"",
            ]
        )

    def saveIncrementToSQL(self):
        """SQL to save time to use as next increment_from."""

        if self.table.base_schema_table:
            schema, table_name = self.table.base_schema_table.split(".")
        else:
            schema, table_name = self.table.schema, self.table.table_name

        self.lines.extend(
            [
                f'    sql_save_increment_to = f"""',
                f"        SELECT",
                f"            adm.table_parameters_update('{schema}.{table_name}', 'increment_from_2', '{{increment_to}}');",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_save_increment_to")

    def truncateSQL(self):
        """Truncate table SQL."""

        self.lines.extend(
            [
                f'    sql_truncate = """',
                f"        TRUNCATE TABLE {self.table.schema}.{self.table.table_name}",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_truncate")

    def selectSQL(self, where):
        """Select SQL query for Glinda Helper."""

        # main source
        src_table = "xxx_table"
        src_alias = "xxx"
        first_alias = "xxx"
        for _, dict_ in self.table.sources.items():
            src_table = dict_["source_table"]
            src_alias = dict_["alias"]
            first_alias = src_alias
            break

        self.lines.extend(
            [
                f'    sql_select_{self.table.schema} = f"""',
                f"        SELECT",
            ]
        )

        self.addColumns("select_from_source")

        self.lines.extend(
            [
                f"        FROM",
                f"            {src_table} {src_alias} with (nolock)",
            ]
        )

        # where
        if not where:
            self.lines.extend([f'    ;"""', f""])
        else:
            self.lines.extend(
                [
                    f"        WHERE",
                    f"            COALESCE({first_alias}.ModificationDate, {first_alias}.CreatedDate) >= CAST('{{increment_from}}' AS DATETIME2)",
                    f"            AND COALESCE({first_alias}.ModificationDate, {first_alias}.CreatedDate) <= CAST('{{increment_to}}' AS DATETIME2);",
                    f'    """',
                    f"",
                ]
            )

    def fromSources(self):
        """From sources with joins."""

        self.lines.append("        FROM")

        if not self.table.sources:
            first_alias = "xxx"
            self.lines.extend(
                [
                    f"            xxx_schema.xxx_table xxx",
                    f"",
                ]
            )
        else:
            for i, (table, dict_) in enumerate(self.table.sources.items()):
                if i == 0:
                    self.lines.append(
                        f"            {dict_['source_table']} {dict_['alias']}"
                    )
                    first_alias = dict_["alias"]
                else:
                    self.lines.extend(
                        [
                            f"        LEFT JOIN",
                            f"            {dict_['source_table']} {dict_['alias']} ON {dict_['alias']}.xxx = {first_alias}.xxx",
                        ]
                    )

        return first_alias

    def insertSQL(self, where):
        """Insert SQL query."""

        self.lines.extend(
            [
                f'    sql_insert_{self.table.schema} = f"""',
                f"        INSERT INTO {self.table.schema}.{self.table.table_name} AS {self.table.schema} (",
            ]
        )

        self.addColumns("insert_into")

        self.lines.extend(
            [
                f"        )",
                f"        SELECT",
            ]
        )

        self.addColumns("select_from_source")

        first_alias = self.fromSources()

        # where
        if where:
            self.lines.extend(
                [
                    f"        WHERE",
                    f"            {first_alias}.record_updated >= '{{increment_from}}'::timestamptz",
                    f"            AND {first_alias}.record_updated < '{{increment_to}}'::timestamptz;",
                    f'        """',
                    f"",
                ]
            )
        else:
            self.lines.extend([f'    ;"""', f""])

        # add to list of sql queries
        self.queries.append(f"sql_insert_{self.table.schema}")

    def upsertSQL(self, where):

        self.lines.extend(
            [
                f'    sql_upsert_{self.table.schema} = f"""',
                f"        INSERT INTO {self.table.schema}.{self.table.table_name} AS {self.table.schema} (",
            ]
        )

        self.addColumns("insert_into")

        self.lines.extend(
            [
                f"        )",
                f"        SELECT",
            ]
        )

        self.addColumns("select_from_source")

        self.lines.append("        FROM")

        first_alias = self.fromSources()

        # where
        if where:
            self.lines.extend(
                [
                    f"        WHERE",
                    f"            {first_alias}.record_updated >= '{{increment_from}}'::timestamptz",
                    f"            AND {first_alias}.record_updated < '{{increment_to}}'::timestamptz",
                ]
            )

        # on conflict
        self.lines.extend(
            [
                f"        ON CONFLICT",
                f"            ({', '.join(self.table.primary_keys)})",
                f"        DO UPDATE SET",
            ]
        )

        # update set = excluded
        self.addColumns("update_set")

        self.lines.append(f"        WHERE (")

        # update set where new and old differ
        self.addColumns("update_where")

        self.lines.extend(
            [
                f"        );",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_upsert_{self.table.schema}")

    def softDeleteSQL(self):

        # delete query
        self.lines.extend(
            [
                f'    sql_delete_{self.table.schema} = f"""',
                f"        UPDATE",
                f"            {self.table.schema}.{self.table.table_name} AS {self.table.schema}_n",
                f"        SET",
                f"            record_deleted = now()",
                f"        FROM",
                f"            {self.table.schema}.{self.table.table_name} AS {self.table.schema}",
                f"        LEFT OUTER JOIN",
            ]
        )

        # join on all primary keys
        if self.table.primary_keys:
            for i, col in enumerate(self.table.primary_keys):
                if i < 1:
                    self.lines.append(
                        f"            tmp.{self.table.table_name} AS tmp ON tmp.{col} = {self.table.schema}.{col}"
                    )
                else:
                    self.lines.append(
                        f"            AND tmp.{col} = {self.table.schema}.{col}"
                    )
        else:
            self.lines.append(
                f"            tmp.{self.table.table_name} AS tmp ON tmp.xxx = {self.table.schema}.xxx"
            )

        self.lines.append(f"        WHERE")

        # join on all primary keys
        if self.table.primary_keys:
            for i, col in enumerate(self.table.primary_keys):
                if i < 1:
                    self.lines.append(
                        f"            {self.table.schema}_n.{col} = {self.table.schema}.{col}"
                    )
                else:
                    self.lines.append(
                        f"            AND {self.table.schema}_n.{col} = {self.table.schema}.{col}"
                    )
        else:
            self.lines.append(
                f"            {self.table.schema}_n.xxx = {self.table.schema}.xxx"
            )

        self.lines.append(f"            AND {self.table.schema}.record_deleted IS NULL")

        self.addColumns("delete_where")

        self.lines.extend(
            [
                f'    ;"""',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_delete_{self.table.schema}")

    def insertToPersistantStaging(self):
        """Insert SQL query of landing area to persistant staging area."""

        self.lines.extend(
            [
                f'    sql_insert_{self.table.schema} = f"""',
                f"        INSERT INTO {self.table.schema}.{self.table.table_name} AS {self.table.schema} (",
            ]
        )

        self.addColumns("psa_insert_into")

        self.lines.extend(
            [
                f"        )",
                f"        -- inserted and updated",
                f"        WITH insert_update AS (",
                f"            SELECT",
            ]
        )

        self.addColumns("psa_cte", tabs=4)

        self.lines.extend(
            [
                f"                , a._fivetran_deleted",
                f"            FROM",
                f"                {next(iter(self.table.sources.values()))['source_table']} a",
                f"            WHERE",
                f"                _fivetran_deleted = FALSE",
                f"                AND _fivetran_synced >= '{{increment_from}}'::timestamptz",
                f"                AND _fivetran_synced < '{{increment_to}}'::timestamptz",
                f"        ),",
                f"        -- replaced or deleted rows",
                f"        replaced_deleted AS (",
                f"            SELECT",
            ]
        )

        self.addColumns("psa_cte", tabs=4)

        self.lines.extend(
            [
                f"                , _fivetran_deleted",
                f"            FROM",
                f"                {next(iter(self.table.sources.values()))['source_table']} a",
                f"            WHERE",
                f"                _fivetran_deleted = TRUE",
                f"                AND _fivetran_synced >= '{{increment_from}}'::timestamptz",
                f"                AND _fivetran_synced < '{{increment_to}}'::timestamptz",
                f"        ),",
                f"        -- only deleted",
                f"        deleted AS (",
                f"            SELECT",
            ]
        )

        self.addColumns("psa_cte", tabs=4)

        self.lines.extend(
            [
                f"                , a._fivetran_deleted",
                f"            FROM",
                f"                replaced_deleted a",
                f"            LEFT JOIN",
            ]
        )

        first_col = "xxx"

        if self.table.business_keys:
            for i, col in enumerate(self.table.business_keys):

                # business key may be renamed in dest, find source name
                column_name = col
                for val, dest_col in enumerate(self.table.columns):
                    if dest_col == col:
                        if self.table.source_columns[val]:
                            column_name = self.table.source_columns[val]
                        break

                if i < 1:
                    first_col = column_name
                    self.lines.append(
                        f"                insert_update b ON b.{column_name} = a.{column_name}"
                    )
                else:
                    self.lines.append(
                        f"                    AND b.{column_name} = a.{column_name}"
                    )
        else:
            self.lines.append(
                f"                updated_deleted b ON b.xxx = a.xxx"
            )

        self.lines.extend(
            [
                f"            WHERE",
                f"                b.{first_col} IS NULL",
                f"        )",
                f"        -- inserted and updated",
                f"        SELECT",
            ]
        )

        self.addColumns("psa_select")

        self.lines.extend(
            [
                f"        FROM",
                f"            insert_update",
                f"        UNION ALL",
                f"        -- deleted",
                f"        SELECT",
            ]
        )

        self.addColumns("psa_select")

        self.lines.extend(
            [
                f"        FROM",
                f"            deleted;",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_insert_{self.table.schema}")

    def incrementFrom2AsIncrementFromSQL(self):
        """SQL query to update table parameters."""

        self.lines.extend(
            [
                f'    sql_update_increment_from = """',
                f"        UPDATE",
                f"            adm.table_parameters",
                f"        SET",
                f"            increment_from = increment_from_2",
                f"        WHERE",
                f"            table_name = '{self.table.schema}.{self.table.table_name}';",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_update_increment_from")

    def updateIncrementFromSQL(self):
        """SQL query to update table parameter increment_from."""

        self.lines.extend(
            [
                f'    sql_update_increment_from = f"""',
                f"        UPDATE",
                f"            adm.table_parameters",
                f"        SET",
                f"            increment_from = '{{increment_to}}'",
                f"        WHERE",
                f"            table_name = '{self.table.schema}.{self.table.table_name}';",
                f'    """',
                f"",
            ]
        )

        # add to list of sql queries
        self.queries.append(f"sql_update_increment_from")

    def refreshMaterializedView(self):

        self.lines.extend(
            [
                f'    sql_refresh_view = f"""',
                f"        REFRESH MATERIALIZED VIEW {self.view.schema}.{self.view.view_name}",
                f'    """',
                f"",
            ]
        )

    def addColumns(self, typ, tabs=3):
        """Lines for each column in table."""

        columns = self.table.columns
        source_columns = self.table.source_columns
        aliases = self.table.source_alias

        skip_columns = ["record_created", "record_updated", "record_deleted", "load_datetime"]

        # check column placement
        i = 0
        for j, column in enumerate(columns):

            if i > 0:
                space = " " * 4 * tabs + ", "
            else:
                space = " " * 4 * tabs

            # insert into lines
            if typ == "insert_into":

                if column in skip_columns:
                    continue

                self.lines.append(f"{space}{column}")

            elif typ == "select_from_source":

                if column in skip_columns:
                    continue

                # get alias
                alias = aliases[j]

                # use different column name
                if (source_columns and source_columns[j]) and source_columns[
                    j
                ] != columns[j]:
                    self.lines.append(f"{space}{alias}.{source_columns[j]} AS {column}")
                else:
                    self.lines.append(f"{space}{alias}.{column}")

            elif typ == "update_set":

                if column == "record_updated":
                    self.lines.append(f"{space}{column} = now()")
                elif column in ["record_created", "record_deleted"]:
                    continue
                elif column in self.table.primary_keys:
                    continue
                else:
                    self.lines.append(f"{space}{column} = EXCLUDED.{column}")

            elif typ == "update_where":

                # skip columns
                if column in skip_columns:
                    continue
                elif column in self.table.primary_keys:
                    continue

                space = space.replace(", ", "OR ")
                self.lines.append(
                    f"{space}{self.table.schema}.{column} is DISTINCT FROM EXCLUDED.{column}"
                )

            # where tmp columns are null
            elif typ == "delete_where":

                # skip columns
                if column in skip_columns:
                    continue

                space = space.replace(", ", "")
                self.lines.append(f"{space}AND tmp.{column} IS NULL")

            elif typ == "psa_insert_into":

                self.lines.append(f"{space}{column}")

            elif typ == "psa_cte":

                # skip columns or surrogate key
                if column in skip_columns or column.startswith('sur_'):
                    continue

                # use different column name
                # self.lines.append(f"column={column}, j={j}, columns[j]={columns[j]}, source_columns[i]={source_columns[i]}")
                if (source_columns and source_columns[j]) and source_columns[j] != columns[j]:
                    self.lines.append(f"{space}a.{source_columns[j]}")
                else:
                    self.lines.append(f"{space}a.{column}")


            elif typ == "psa_select":

                # surrogate key
                if column.startswith('sur_'):

                    # business key may be renamed in dest, find source name
                    business_keys_source = []
                    if self.table.business_keys:
                        for i, col in enumerate(self.table.business_keys):
                            column_name = col
                            for val, dest_col in enumerate(self.table.columns):
                                if dest_col == col:
                                    if self.table.source_columns[val]:
                                        column_name = self.table.source_columns[val]
                                    break

                            business_keys_source.append(column_name)

                        # id::text || now()::text
                        business_concat = ' || '.join([f'{x}::text' for x in business_keys_source]) + f' || now()::text'
                    else:
                        business_concat = "xxx"

                    self.lines.append(f"{space}encode(sha256(CAST({business_concat} AS bytea)), 'hex') AS {column}")
                elif column == "load_datetime":
                    self.lines.append(f"{space}now() AS load_datetime")
                elif column == "record_deleted":
                    self.lines.append(f"{space}_fivetran_deleted AS record_deleted")
                else:
                    # use different column name
                    if (source_columns and source_columns[j]) and source_columns[
                        j
                    ] != columns[j]:
                        self.lines.append(f"{space}{source_columns[j]} AS {column}")
                    else:
                        self.lines.append(f"{space}{column}")

            i += 1

    def glindaHelper(self):
        """Use glinda helper to retrive data from source."""

        self.lines.extend(
            [
                f'    tmp_table = "{self.table.schema}.{self.table.table_name}"',
                f"",
                f"    glinda_helper = GlindaHelper(",
                f'        source_type="MsSql",',
                f"        source_connection_id=source_conn_id,",
                f"        target_connection_id=glinda_conn_id",
                f"    )",
                f"",
                f"    glinda_helper.load_tmp_by_copy(",
                f"        source_sql=sql_select_{self.table.schema},",
                f"        tmp_table=tmp_table, ",
                f"        if_truncate_tmp=True",
                f"    )",
                f"",
            ]
        )

    def header(self):
        """Template beginning of DAG."""

        for table in self.table_structures:
            if table.final_table:
                schema = table.schema
                schema_table = table.schema_table
                break

        # pool
        if schema == "dwh":
            pool = "dwh"
        elif schema == "datamarts":
            pool = "dtm"
        else:
            pool = "default_pool"

        # dag id & description
        if self.action == "new_table":
            dag_id = schema_table.replace(".", "_")
            descr = f"Upsert into {schema_table}."

        elif self.action == "import":
            dag_id = f"import_{schema}"
            descr = f"Import tables into {schema}."

        # date
        n = datetime.datetime.utcnow()

        self.lines.extend(
            [
                f"from datetime import datetime",
                f"from datetime import timedelta",
                f"",
                f"from airflow import DAG",
                f"from airflow.hooks.postgres_hook import PostgresHook",
                f"from airflow.models import Variable",
                f"from airflow.operators.python_operator import PythonOperator",
                f"from pendulum import timezone",
                f"",
            ]
        )

        if self.action == "import":
            self.lines.extend(
                [
                    f"from utils.db_helper import GlindaHelper",
                    f"",
                ]
            )

        self.lines.extend(
            [
                f'glinda_conn_id = "glinda_etl_airflow"',
            ]
        )
        if self.action == "import":
            self.lines.append('source_conn_id = "xxx"')
        self.lines.append("")

        self.lines.extend(
            [
                f'p_dag_id = "{dag_id}"',
                f'p_description = "{descr}"',
                f'p_owner = "{self.user}"',
                f'p_timezone = timezone("Australia/Melbourne")',
                f'p_pool = "{pool}" # xxx to change',
                f'env = Variable.get("environment")',
                f"",
                f'if env == "prod":',
                f"    v_start_date = datetime({n.year}, {n.month}, {n.day}, 0, 0, tzinfo=p_timezone)",
                f'    v_schedule_interval = "X X X X xxx"',
                f'    v_email = ["dataeng@mecca.com.au", "{self.user_email}"]',
                f"    v_execution_timeout = timedelta(minutes=120)",
                f"    v_retry_delay = timedelta(minutes=1)",
                f"    v_retries = 0",
                f'elif env == "nonprod":',
                f"    v_start_date = datetime({n.year}, {n.month}, {n.day}, 0, 0, tzinfo=p_timezone)",
                f"    v_schedule_interval = None",
                f'    v_email = "{self.user_email}"',
                f"    v_execution_timeout = timedelta(minutes=120)",
                f"    v_retry_delay = timedelta(minutes=1)",
                f"    v_retries = 0",
                f"else:",
                f'    raise ValueError("Check Airflow environment variable exists")',
                f"",
                f"default_args = dict(",
                f"    owner=p_owner,",
                f"    email=v_email,",
                f"    email_on_failure=True,",
                f"    email_on_retry=True,",
                f"    retries=v_retries,",
                f"    retry_delay=v_retry_delay,",
                f"    sla=None,",
                f"    execution_timeout=v_execution_timeout,",
                f"    depends_on_past=False,",
                f"    task_concurrency=16,",
                f"    pool=p_pool,",
                f"    provide_context=True,",
                f")",
                f"",
            ]
        )

    def runQueryCommand(self):
        """Add hook to run queries."""

        if not self.queries: return

        self.lines.append("    glinda_hook.run([")

        for query in self.queries:
            self.lines.append(f"        {query},")
        self.lines.append("    ], autocommit=False)")
        self.lines.append("")

        # reset list
        self.queries = []

    def addTasksOfDAG(self):
        """Lines making each task for the DAG."""

        self.lines.extend(
            [
                f"with DAG(",
                f"    dag_id=p_dag_id,",
                f"    description=p_description,",
                f"    schedule_interval=v_schedule_interval,",
                f"    start_date=v_start_date,",
                f"    default_args=default_args,",
                f"    concurrency=16,",
                f"    max_active_runs=1,",
                f"    catchup=False,",
                f") as dag:",
            ]
        )

        for task_list in self.tasks:
            for task in task_list:
                self.lines.extend(
                    [
                        f"    t_{task} = PythonOperator(",
                        f"        task_id='{task}',",
                        f"        python_callable={task},",
                        f"    )",
                        f"",
                    ]
                )

        for task_list in self.tasks:
            self.lines.append("t_" + " >> t_".join(task_list))
