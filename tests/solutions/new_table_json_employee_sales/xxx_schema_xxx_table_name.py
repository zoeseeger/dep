from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pendulum import timezone

glinda_conn_id = "glinda_etl_airflow"

p_dag_id = "dwh_fct_loss_prevention_sale"
p_description = "Upsert into dwh.fct_loss_prevention_sale."
p_owner = "xxx"
p_timezone = timezone("Australia/Melbourne")
p_pool = "dwh"
env = Variable.get("environment")

if env == "prod":
    v_start_date = datetime(2023, 6, 2, 0, 0, tzinfo=p_timezone)
    v_schedule_interval = "X X X X xxx"
    v_email = ["dataeng@mecca.com.au", "xxx.xxx@mecca.com.au"]
    v_execution_timeout = timedelta(minutes=120)
    v_retry_delay = timedelta(minutes=1)
    v_retries = 0
elif env == "nonprod":
    v_start_date = datetime(2023, 6, 2, 0, 0, tzinfo=p_timezone)
    v_schedule_interval = None
    v_email = "xxx.xxx@mecca.com.au"
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


def upsert_fct_loss_prevention_sale(**kwargs):
    """Upsert from source -> dwh.fct_loss_prevention_sale."""

    glinda_hook = PostgresHook(glinda_conn_id)

    increment_from, increment_to = glinda_hook.get_first("""
        SELECT
            (increment_from::timestamptz-overlap::interval)::text
            , now()::timestamptz
        FROM
            adm.table_parameters
        WHERE
            table_name = 'dwh.fct_loss_prevention_sale';
    """)

    sql_upsert_dwh = f"""
        INSERT INTO dwh.fct_loss_prevention_sale AS dwh (
            date_key
            , store_key
            , item_key
            , time_key
            , customer_key
            , primary_store_key
            , cashier_primary_store_key
            , source_system
            , sale_line_sid
            , invoice_sid
            , receipt_number
            , customer_sid
            , cashier_id
            , employee_id
            , original_invoice_sid
            , original_sale_line_sid
            , billing_address_id
            , shipping_address_id
            , sale_channel
            , is_employee_flag
            , in_employment_period_flag
            , return_in_return_period
            , returned_greater_than_paid_flag
            , employee_ea_flag
        )
        SELECT
            fs.date_key
            , fs.store_key
            , fs.item_key
            , fs.time_key
            , fs.customer_key
            , ds1.store_key AS primary_store_key
            , ds3.store_key AS cashier_primary_store_key
            , fs.source_system
            , fs.sale_line_sid
            , fs.invoice_sid
            , fs.receipt_number
            , fs.customer_sid
            , fs.cashier_id
            , dcpd.customer_employee_id AS employee_id
            , fs.invoice_sid AS original_invoice_sid
            , fs.original_sale_line_sid
            , fsop.billing_address_id
            , fsop.shipping_address_id
            , ds2.finance_concept AS sale_channel
            , fs.loyalty_group_description AS is_employee_flag
            , de.in_employment_period_flag
            , fs.return_in_return_period
            , fs.returned_greater_than_paid_flag
            , fs.employee_ea_flag
        FROM
            dwh.fct_sale fs
        LEFT JOIN
            dwh.dim_store ds1 ON ds1.xxx = fs.xxx
        LEFT JOIN
            dwh.dim_store ds3 ON ds3.xxx = fs.xxx
        LEFT JOIN
            dwh_crm.dim_customer_personal_details dcpd ON dcpd.xxx = fs.xxx
        LEFT JOIN
            dwh.fct_sale_online_postcode fsop ON fsop.xxx = fs.xxx
        LEFT JOIN
            dwh.dim_store ds2 ON ds2.xxx = fs.xxx
        LEFT JOIN
            dwh.dim_employee de ON de.xxx = fs.xxx
        WHERE
            fs.record_updated >= '{increment_from}'::timestamptz
            AND fs.record_updated < '{increment_to}'::timestamptz
        ON CONFLICT
            (sale_line_sid, source_system)
        DO UPDATE SET
            date_key = EXCLUDED.date_key
            , store_key = EXCLUDED.store_key
            , item_key = EXCLUDED.item_key
            , time_key = EXCLUDED.time_key
            , customer_key = EXCLUDED.customer_key
            , primary_store_key = EXCLUDED.primary_store_key
            , cashier_primary_store_key = EXCLUDED.cashier_primary_store_key
            , invoice_sid = EXCLUDED.invoice_sid
            , receipt_number = EXCLUDED.receipt_number
            , customer_sid = EXCLUDED.customer_sid
            , cashier_id = EXCLUDED.cashier_id
            , employee_id = EXCLUDED.employee_id
            , original_invoice_sid = EXCLUDED.original_invoice_sid
            , original_sale_line_sid = EXCLUDED.original_sale_line_sid
            , billing_address_id = EXCLUDED.billing_address_id
            , shipping_address_id = EXCLUDED.shipping_address_id
            , sale_channel = EXCLUDED.sale_channel
            , is_employee_flag = EXCLUDED.is_employee_flag
            , in_employment_period_flag = EXCLUDED.in_employment_period_flag
            , return_in_return_period = EXCLUDED.return_in_return_period
            , returned_greater_than_paid_flag = EXCLUDED.returned_greater_than_paid_flag
            , employee_ea_flag = EXCLUDED.employee_ea_flag
            , record_updated = now()
        WHERE (
            dwh.date_key is DISTINCT FROM EXCLUDED.date_key
            OR dwh.store_key is DISTINCT FROM EXCLUDED.store_key
            OR dwh.item_key is DISTINCT FROM EXCLUDED.item_key
            OR dwh.time_key is DISTINCT FROM EXCLUDED.time_key
            OR dwh.customer_key is DISTINCT FROM EXCLUDED.customer_key
            OR dwh.primary_store_key is DISTINCT FROM EXCLUDED.primary_store_key
            OR dwh.cashier_primary_store_key is DISTINCT FROM EXCLUDED.cashier_primary_store_key
            OR dwh.invoice_sid is DISTINCT FROM EXCLUDED.invoice_sid
            OR dwh.receipt_number is DISTINCT FROM EXCLUDED.receipt_number
            OR dwh.customer_sid is DISTINCT FROM EXCLUDED.customer_sid
            OR dwh.cashier_id is DISTINCT FROM EXCLUDED.cashier_id
            OR dwh.employee_id is DISTINCT FROM EXCLUDED.employee_id
            OR dwh.original_invoice_sid is DISTINCT FROM EXCLUDED.original_invoice_sid
            OR dwh.original_sale_line_sid is DISTINCT FROM EXCLUDED.original_sale_line_sid
            OR dwh.billing_address_id is DISTINCT FROM EXCLUDED.billing_address_id
            OR dwh.shipping_address_id is DISTINCT FROM EXCLUDED.shipping_address_id
            OR dwh.sale_channel is DISTINCT FROM EXCLUDED.sale_channel
            OR dwh.is_employee_flag is DISTINCT FROM EXCLUDED.is_employee_flag
            OR dwh.in_employment_period_flag is DISTINCT FROM EXCLUDED.in_employment_period_flag
            OR dwh.return_in_return_period is DISTINCT FROM EXCLUDED.return_in_return_period
            OR dwh.returned_greater_than_paid_flag is DISTINCT FROM EXCLUDED.returned_greater_than_paid_flag
            OR dwh.employee_ea_flag is DISTINCT FROM EXCLUDED.employee_ea_flag
        );
    """

    sql_update_increment_from = f"""
        UPDATE 
            adm.table_parameters
        SET 
            increment_from = '{increment_to}'
        WHERE 
            table_name = 'dwh.fct_loss_prevention_sale';
    """

    glinda_hook.run(sql_upsert_dwh, autocommit=True)
    glinda_hook.run(sql_update_increment_from, autocommit=True)


def refresh_materialized_view_fct_loss_prevention_sale(**kwargs):
    """Refresh materialized view -> dwh.fct_loss_prevention_sale_v."""

    glinda_hook = PostgresHook(glinda_conn_id)

    sql_refresh_view = f"""
        REFRESH MATERIALIZED VIEW dwh.fct_loss_prevention_sale_v
    """


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
    t_upsert_fct_loss_prevention_sale = PythonOperator(
        task_id='upsert_fct_loss_prevention_sale',
        python_callable=upsert_fct_loss_prevention_sale,
    )

    t_refresh_materialized_view_fct_loss_prevention_sale = PythonOperator(
        task_id='refresh_materialized_view_fct_loss_prevention_sale',
        python_callable=refresh_materialized_view_fct_loss_prevention_sale,
    )

t_upsert_fct_loss_prevention_sale >> t_refresh_materialized_view_fct_loss_prevention_sale
