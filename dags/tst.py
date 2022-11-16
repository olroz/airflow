
import json
import random

from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
                                                        KubernetesPodOperator)
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


with DAG(
    start_date=datetime(2022,11,5),
    catchup=True,
    schedule_interval="0 5 * * *",
    tags=['sandbox'],
    dag_id='scout_dag2'
) as dag:

    BUCKET_NAME = 'mwaa-sandbox-flow-prod'
    ACCOUNTS = Variable.get("scout-accounts").split(',')
    #ACCOUNTS = ACCOUNTSVAR.split(',')


    def check_scan(ds, ti, acc, *op_args, **kwargs):
        scoutres = ti.xcom_pull('run_scout_check_'+acc)
        print(acc)
        data1 = json.dumps(scoutres)
        data = json.loads(data1)
        for (k, v) in data.items():
            if v["flagged_items"] >0 and v["max_level"] == 'danger' :
                exists = True
        if exists:
            print('danger')
            return True
        else:
            print('no danger')
            return False

    def save_to_dynamo(ds, ti, acc, *op_args, **kwargs):
        scoutres = ti.xcom_pull('run_scout_check_'+acc)
        print(acc)
        dynamodb = DynamoDBHook(aws_conn_id='aws_default',
            table_name='scoutsuite-db', table_keys=['account_id'], region_name='eu-west-1')
        dynamodb.write_batch_data(
            [{'account_id': acc, 'day': ds, 'scout-results': scoutres}]
        )


    for acc in ACCOUNTS:
        acc = acc.strip()
        run_scout_check=KubernetesPodOperator(
            task_id=f"run_scout_check_{acc}",
            cluster_context='aws',
            namespace="airflow",
            name="air_scout_pod_1",
            image='933560321714.dkr.ecr.eu-west-1.amazonaws.com/scout-suite:5.12.0',
            service_account_name='sa-scout2',
            cmds=["bash"],
            arguments=["-c", "echo ---scout_starting-" + acc + "; eval $(aws sts assume-role --role-arn arn:aws:iam::" + acc + ":role/ait-scout-scan-role --role-session-name scout-sts | jq -r '.Credentials | \"export AWS_ACCESS_KEY_ID=\(.AccessKeyId)\nexport AWS_SECRET_ACCESS_KEY=\(.SecretAccessKey)\nexport AWS_SESSION_TOKEN=\(.SessionToken)\n\"'); /root/scoutsuite/bin/scout aws --no-browser; tail scoutsuite-report/scoutsuite-results/scoutsuite_results_aws-*.js -n +2 > /tmp/scoutsuite_results_aws-" + acc + ".js; export AWS_ACCESS_KEY_ID=; export AWS_SECRET_ACCESS_KEY=; export AWS_SESSION_TOKEN=; aws s3 cp /tmp/scoutsuite_results_aws-*.js s3://" + BUCKET_NAME + "/artefacts/" + acc + "/{{ ds}}/; tail scoutsuite-report/scoutsuite-results/scoutsuite_results_aws-*.js -n +2 | jq '.last_run.summary' | tee /airflow/xcom/return.json"],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=True,
            do_xcom_push=True,
            startup_timeout_seconds=60,
        )

        save_results = PythonOperator(
            task_id=f'save_to_dynamo_{acc}',
            python_callable=save_to_dynamo,
            provide_context=True,
            op_kwargs={"acc":acc},
        )
        
        task_check_scan = ShortCircuitOperator(
            task_id=f'check_scan_{acc}',
            python_callable=check_scan,
            op_kwargs={"acc":acc},
        )

        publish_message = SnsPublishOperator(
            task_id=f'publish_message_{acc}',
            target_arn='arn:aws:sns:eu-west-1:933560321714:mwaa-sandbox-PROD',
            message='Scout scan has found a danger issue in your env ' + acc,
        )


        run_scout_check >> save_results >> task_check_scan >> publish_message

