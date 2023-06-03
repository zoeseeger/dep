from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pendulum import timezone

from utils.db_helper import GlindaHelper

glinda_conn_id = "glinda_etl_airflow"
source_conn_id = "xxx"

p_dag_id = "import_sys_new"
p_description = "Import tables into sys_new."
p_owner = "Zoe Seeger"
p_timezone = timezone("Australia/Melbourne")
p_pool = "xxx"
env = Variable.get("environment")

if env == "prod":
    v_start_date = datetime(2023, 6, 2, 0, 0, tzinfo=p_timezone)
    v_schedule_interval = "X X X X xxx"
    v_email = ["dataeng@mecca.com.au", "zoe.seeger@mecca.com.au"]
    v_execution_timeout = timedelta(minutes=120)
    v_retry_delay = timedelta(minutes=1)
    v_retries = 0
elif env == "nonprod":
    v_start_date = datetime(2023, 6, 2, 0, 0, tzinfo=p_timezone)
    v_schedule_interval = None
    v_email = "zoe.seeger@mecca.com.au"
    v_execution_timeout = timedelta(minutes=120)
    v_retry_delay = timedelta(minutes=1)
    v_retries = 0
else:
    raise ValueError("Check Airflow environment variable exists")

default_args = dict(
    owner=p_owner,
    email=v_email,
    email_on_failure=True,
    email_on_retry=True,
    retries=v_retries,
    retry_delay=v_retry_delay,
    sla=None,
    execution_timeout=v_execution_timeout,
    depends_on_past=False,
    task_concurrency=16,
    pool=p_pool,
    provide_context=True,
)


def extract_to_tmp_new_orderitemadjustmentreasons(**kwargs):
    """Extract and full load from source -> tmp.new_orderitemadjustmentreasons."""

    sql_select_tmp = f"""
        SELECT
            o.orderitemadjustmentreasonid AS order_item_adjustment_reason_id
            , o.reason
        FROM
            dbo.orderitemadjustmentreasons o with (nolock)
    ;"""

    tmp_table = "tmp.new_orderitemadjustmentreasons"

    glinda_helper = GlindaHelper(
        source_type="MsSql",
        source_connection_id=source_conn_id,
        target_connection_id=glinda_conn_id
    )

    glinda_helper.load_tmp_by_copy(
        source_sql=sql_select_tmp,
        tmp_table=tmp_table, 
        if_truncate_tmp=True
    )


def upsert_to_sys_orderitemadjustmentreasons(**kwargs):
    """Upsert from tmp -> sys_new.orderitemadjustmentreasons with soft delete."""

    glinda_hook = PostgresHook(glinda_conn_id)

    sql_upsert_sys_new = f"""
        INSERT INTO sys_new.orderitemadjustmentreasons AS sys_new (
            order_item_adjustment_reason_id
            , reason
        )
        SELECT
            tmp.order_item_adjustment_reason_id
            , tmp.reason
        FROM
            tmp.new_orderitemadjustmentreasons tmp
        ON CONFLICT
            (order_item_adjustment_reason_id)
        DO UPDATE SET
            reason = EXCLUDED.reason
            , record_updated = now()
        WHERE (
            sys_new.reason is DISTINCT FROM EXCLUDED.reason
        );
    """

    sql_delete_sys_new = f"""
        UPDATE 
            sys_new.orderitemadjustmentreasons AS sys_new_n
        SET
            record_deleted = now()
        FROM
            sys_new.orderitemadjustmentreasons AS sys_new
        LEFT OUTER JOIN
            tmp.orderitemadjustmentreasons AS tmp ON tmp.order_item_adjustment_reason_id = sys_new.order_item_adjustment_reason_id
        WHERE
            sys_new_n.order_item_adjustment_reason_id = sys_new.order_item_adjustment_reason_id
            AND sys_new.record_deleted IS NULL
            AND tmp.order_item_adjustment_reason_id IS NULL
            AND tmp.reason IS NULL
    ;"""

    glinda_hook.run(sql_upsert_sys_new, autocommit=True)
    glinda_hook.run(sql_delete_sys_new, autocommit=True)


def extract_to_tmp_new_orderitems(**kwargs):
    """Extract and incremental load from source -> tmp.new_orderitems."""

    glinda_hook = PostgresHook(glinda_conn_id)

    increment_from, increment_to = glinda_hook.get_first("""
        SELECT
            (increment_from::timestamp-overlap::interval)::text
            , now()::timestamp
        FROM
            adm.table_parameters
        WHERE
            table_name = 'sys_new.orderitems';
    """)

    sql_select_tmp = f"""
        SELECT
            o.orderitemid AS order_item_id
            , o.orderid AS order_id
            , o.itemcode AS item_code
            , o.description
            , o.quantity
            , o.unitprice AS unit_price
            , o.adjustedquantity AS adjusted_quantity
            , o.orderitemadjustmentreasonid AS order_item_adjustment_reason_id
        FROM
            dbo.orderitems o with (nolock)
        WHERE
            COALESCE(o.ModificationDate, o.CreatedDate) >= CAST('{increment_from}' AS DATETIME2)
            AND COALESCE(o.ModificationDate, o.CreatedDate) <= CAST('{increment_to}' AS DATETIME2);
    """

    tmp_table = "tmp.new_orderitems"

    glinda_helper = GlindaHelper(
        source_type="MsSql",
        source_connection_id=source_conn_id,
        target_connection_id=glinda_conn_id
    )

    glinda_helper.load_tmp_by_copy(
        source_sql=sql_select_tmp,
        tmp_table=tmp_table, 
        if_truncate_tmp=True
    )

    sql_save_increment_to = f"""
        SELECT
            adm.table_parameters_update('sys_new.orderitems', 'increment_from_2', '{increment_to}');
    """

    glinda_hook.run(sql_save_increment_to, autocommit=True)


def upsert_to_sys_orderitems(**kwargs):
    """Upsert from tmp -> sys_new.orderitems."""

    glinda_hook = PostgresHook(glinda_conn_id)

    sql_upsert_sys_new = f"""
        INSERT INTO sys_new.orderitems AS sys_new (
            order_item_id
            , order_id
            , item_code
            , description
            , quantity
            , unit_price
            , adjusted_quantity
            , order_item_adjustment_reason_id
        )
        SELECT
            tmp.order_item_id
            , tmp.order_id
            , tmp.item_code
            , tmp.description
            , tmp.quantity
            , tmp.unit_price
            , tmp.adjusted_quantity
            , tmp.order_item_adjustment_reason_id
        FROM
            tmp.new_orderitems tmp
        ON CONFLICT
            (order_item_id)
        DO UPDATE SET
            order_id = EXCLUDED.order_id
            , item_code = EXCLUDED.item_code
            , description = EXCLUDED.description
            , quantity = EXCLUDED.quantity
            , unit_price = EXCLUDED.unit_price
            , adjusted_quantity = EXCLUDED.adjusted_quantity
            , order_item_adjustment_reason_id = EXCLUDED.order_item_adjustment_reason_id
            , record_updated = now()
        WHERE (
            sys_new.order_id is DISTINCT FROM EXCLUDED.order_id
            OR sys_new.item_code is DISTINCT FROM EXCLUDED.item_code
            OR sys_new.description is DISTINCT FROM EXCLUDED.description
            OR sys_new.quantity is DISTINCT FROM EXCLUDED.quantity
            OR sys_new.unit_price is DISTINCT FROM EXCLUDED.unit_price
            OR sys_new.adjusted_quantity is DISTINCT FROM EXCLUDED.adjusted_quantity
            OR sys_new.order_item_adjustment_reason_id is DISTINCT FROM EXCLUDED.order_item_adjustment_reason_id
        );
    """

    sql_update_increment_from = """
        UPDATE
            adm.table_parameters
        SET
            increment_from = increment_from_2
        WHERE
            table_name = 'sys_new.orderitems';
    """

    glinda_hook.run(sql_upsert_sys_new, autocommit=True)
    glinda_hook.run(sql_update_increment_from, autocommit=True)

with DAG(
    dag_id=p_dag_id,
    description=p_description,
    schedule_interval=v_schedule_interval,
    start_date=v_start_date,
    default_args=default_args,
    concurrency=16,
    max_active_runs=1,
    catchup=False,
) as dag:
    t_extract_to_tmp_new_orderitemadjustmentreasons = PythonOperator(
        task_id='extract_to_tmp_new_orderitemadjustmentreasons',
        python_callable=extract_to_tmp_new_orderitemadjustmentreasons,
    )

    t_upsert_to_sys_orderitemadjustmentreasons = PythonOperator(
        task_id='upsert_to_sys_orderitemadjustmentreasons',
        python_callable=upsert_to_sys_orderitemadjustmentreasons,
    )

    t_extract_to_tmp_new_orderitems = PythonOperator(
        task_id='extract_to_tmp_new_orderitems',
        python_callable=extract_to_tmp_new_orderitems,
    )

    t_upsert_to_sys_orderitems = PythonOperator(
        task_id='upsert_to_sys_orderitems',
        python_callable=upsert_to_sys_orderitems,
    )

t_extract_to_tmp_new_orderitemadjustmentreasons >> t_upsert_to_sys_orderitemadjustmentreasons
t_extract_to_tmp_new_orderitems >> t_upsert_to_sys_orderitems
