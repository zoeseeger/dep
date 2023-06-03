from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pendulum import timezone

glinda_conn_id = "glinda_etl_airflow"

p_dag_id = "dwh_agg_hist_item_level_projection"
p_description = "Upsert into dwh.agg_hist_item_level_projection."
p_owner = "Zoe Seeger"
p_timezone = timezone("Australia/Melbourne")
p_pool = "dwh"
env = Variable.get("environment")

if env == "prod":
    v_start_date = datetime(2023, 6, 1, 0, 0, tzinfo=p_timezone)
    v_schedule_interval = "X X X X xxx"
    v_email = ["dataeng@mecca.com.au", "zoe.seeger@mecca.com.au"]
    v_execution_timeout = timedelta(minutes=120)
    v_retry_delay = timedelta(minutes=1)
    v_retries = 0
elif env == "nonprod":
    v_start_date = datetime(2023, 6, 1, 0, 0, tzinfo=p_timezone)
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


def upsert_agg_hist_item_level_projection(**kwargs):
    """Upsert from source -> dwh.agg_hist_item_level_projection."""

    glinda_hook = PostgresHook(glinda_conn_id)

    increment_from, increment_to = glinda_hook.get_first("""
        SELECT
            (increment_from::timestamptz-overlap::interval)::text
            , now()::timestamptz
        FROM
            adm.table_parameters
        WHERE
            table_name = 'dwh.agg_hist_item_level_projection';
    """)

    sql_upsert_dwh = f"""
        INSERT INTO dwh.agg_hist_item_level_projection AS dwh (
            forecast_creation_week
            , item_code
            , week_start_date
            , closing_stock_on_hand
            , constrained_demand
            , unconstrained_demand
            , projected_lost_sales
            , end_of_period_safety_stock
            , end_of_period_model_stock
            , end_of_period_in_transit
        )
        SELECT
            dd.fiscal_year_month_week AS forecast_creation_week
            , jilp.item_code
            , jilp.week_start_date
            , jilp.closing_stock_on_hand
            , jilp.constrained_demand
            , jilp.unconstrained_demand
            , jilp.projected_lost_sales
            , jilp.end_of_period_safety_stock
            , jilp.end_of_period_model_stock
            , jilp.end_of_period_in_transit
        FROM
            dwh.dim_date dd
        LEFT JOIN
            sys_justenough.je_inventory_level_projection jilp ON jilp.xxx = dd.xxx
        WHERE
            dd.record_updated >= '{increment_from}'::timestamptz
            AND dd.record_updated < '{increment_to}'::timestamptz
        ON CONFLICT
            (forecast_creation_week, item_code, week_start_date)
        DO UPDATE SET
            closing_stock_on_hand = EXCLUDED.closing_stock_on_hand
            , constrained_demand = EXCLUDED.constrained_demand
            , unconstrained_demand = EXCLUDED.unconstrained_demand
            , projected_lost_sales = EXCLUDED.projected_lost_sales
            , end_of_period_safety_stock = EXCLUDED.end_of_period_safety_stock
            , end_of_period_model_stock = EXCLUDED.end_of_period_model_stock
            , end_of_period_in_transit = EXCLUDED.end_of_period_in_transit
        WHERE (
            dwh.closing_stock_on_hand is DISTINCT FROM EXCLUDED.closing_stock_on_hand
            OR dwh.constrained_demand is DISTINCT FROM EXCLUDED.constrained_demand
            OR dwh.unconstrained_demand is DISTINCT FROM EXCLUDED.unconstrained_demand
            OR dwh.projected_lost_sales is DISTINCT FROM EXCLUDED.projected_lost_sales
            OR dwh.end_of_period_safety_stock is DISTINCT FROM EXCLUDED.end_of_period_safety_stock
            OR dwh.end_of_period_model_stock is DISTINCT FROM EXCLUDED.end_of_period_model_stock
            OR dwh.end_of_period_in_transit is DISTINCT FROM EXCLUDED.end_of_period_in_transit
        );
    """

    sql_update_increment_from = f"""
        UPDATE 
            adm.table_parameters
        SET 
            increment_from = '{increment_to}'
        WHERE 
            table_name = 'dwh.agg_hist_item_level_projection';
    """

    glinda_hook.run(sql_upsert_dwh, autocommit=True)
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
    t_upsert_agg_hist_item_level_projection = PythonOperator(
        task_id='upsert_agg_hist_item_level_projection',
        python_callable=upsert_agg_hist_item_level_projection,
    )

t_upsert_agg_hist_item_level_projection
