from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum

@dag(
        dag_id="orchestrate_dag",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(2026, 10, 1, tz="EST"),
    )
def orchestrate():
    @task
    def ingest_cdc():
        ws = WorkspaceClient(
                host= "https://ss.cloud.databricks.com",
                token= "dapiXXXXXX"
                                )

        job_trigger = ws.jobs.run_now(job_id= 13157240575478)

        while True:
            job_status = ws.jobs.get_run(run_id=job_trigger.run_id)

            print(f"Job status: {job_status.state.life_cycle_state}, Result state: {job_status.state.result_state}")

            if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR  ]:
                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_status.state.result_state}")
            time.sleep(5)
        return "CDC data ingested"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t"
                                    )

    silver_technical_tests = BashOperator(
            task_id="silver_technical_tests",
            cwd = "/opt/airflow/walmart_project",
            bash_command="dbt test --select silver_t" 
                                    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_b"
                                     )

    silver_business_tests = BashOperator(
            task_id="silver_business_tests",
            cwd = "/opt/airflow/walmart_project",
            bash_command="dbt test --select silver_b" 
                                             )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephemeral"
                                        )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
                                     )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/fact"
                             )

    ingest_cdc() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_fact

orchestrate_dag = orchestrate()