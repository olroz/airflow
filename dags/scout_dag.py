
import json
import random

from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
                                                        KubernetesPodOperator)
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.hooks.aws_dynamodb import AwsDynamoDBHook
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor


with DAG(
    start_date=datetime(2022,10,23),
    catchup=True,
    schedule_interval="0 5 * * *",
    tags=['sandbox'],
    dag_id='scout_dag'
) as dag:

    KUBE_CONF_PATH = '/usr/local/airflow/dags/kube_config_mwaa-sandbox-PROD.yaml'
    BUCKET_NAME = 'mwaa-sandbox-flow-prod'
    #ACCOUNTID = '933560321714'
    ACCOUNTSVAR = Variable.get("scout-accounts")
    ACCOUNTS = ACCOUNTSVAR.split(',')


    def check_scan(ds, ti, **kwargs):
        scoutres = ti.xcom_pull('run_scout_check_'+acc)
        data1 = json.dumps(scoutres)
        data = json.loads(data1)
        for (k, v) in data.items():
            if v["flagged_items"] >0 and v["max_level"] == 'danger' :
                exists = True
        if exists:
            print('Exists')
            return True
        else:
            print('Doesnot exist')
            return False

    def save_to_dynamo(ds, ti, **kwargs):
        scoutres = ti.xcom_pull('run_scout_check_'+acc)
        dynamodb = AwsDynamoDBHook(aws_conn_id='aws_default',
            table_name='scoutsuite-db', table_keys=['account_id'], region_name='eu-west-1')
        dynamodb.write_batch_data(
            [{'account_id': acc, 'day': ds, 'scout-results': scoutres}]
        )

    # def get_accounts():
    #     #return "1111,2222"
    #     return str(random.randint(1111, 2222)) + "," + str(random.randint(1111, 2222))


    # ACCOUNTSVAR=get_accounts()
    # ACCOUNTS = ACCOUNTSVAR.split(',')

    sensor_one_key = S3KeySensor(
        task_id="sensor_one_key",
        bucket_name='mwaa-sandbox-flow-prod',
        bucket_key='key.txt',
    )

    for acc in ACCOUNTS:
        acc = acc.strip()
        run_scout_check=KubernetesPodOperator(
            task_id=f"run_scout_check_{acc}",
            cluster_context='aws',
            namespace="prod",
            name="air_scout_pod_1",
            image='933560321714.dkr.ecr.eu-west-1.amazonaws.com/scout-suite:5.12.0',
            service_account_name='sa-scout',
            cmds=["bash"],
            arguments=["-c", "echo ---scout_starting-" + acc + "; eval $(aws sts assume-role --role-arn arn:aws:iam::" + acc + ":role/ait-scout-scan-role --role-session-name scout-sts | jq -r '.Credentials | \"export AWS_ACCESS_KEY_ID=\(.AccessKeyId)\nexport AWS_SECRET_ACCESS_KEY=\(.SecretAccessKey)\nexport AWS_SESSION_TOKEN=\(.SessionToken)\n\"'); /root/scoutsuite/bin/scout aws --no-browser; tail scoutsuite-report/scoutsuite-results/scoutsuite_results_aws-*.js -n +2 > /tmp/scoutsuite_results_aws-" + acc + ".js; export AWS_ACCESS_KEY_ID=; export AWS_SECRET_ACCESS_KEY=; export AWS_SESSION_TOKEN=; aws s3 cp /tmp/scoutsuite_results_aws-*.js s3://" + BUCKET_NAME + "/artefacts/" + acc + "/{{ ds}}/; tail scoutsuite-report/scoutsuite-results/scoutsuite_results_aws-*.js -n +2 | jq '.last_run.summary' | tee /airflow/xcom/return.json"],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            config_file=KUBE_CONF_PATH,
            do_xcom_push=True,
            startup_timeout_seconds=60,
        )

        save_results = PythonOperator(
            task_id=f'save_to_dynamo_{acc}',
            python_callable=save_to_dynamo,
            provide_context=True,
        )
        
        task_check_scan = ShortCircuitOperator(
            task_id=f'check_scan_{acc}',
            python_callable=check_scan,
        )

        publish_message = SnsPublishOperator(
            task_id=f'publish_message_{acc}',
            target_arn='arn:aws:sns:eu-west-1:933560321714:mwaa-sandbox-PROD',
            message='Scout scan has found a danger issue in your env ' + acc,
        )


        run_scout_check >> save_results >> task_check_scan >> publish_message



