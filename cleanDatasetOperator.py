import sys
import os
file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(file_dir+"/../elt/script")

import toCleanCurrentDatasetForTraining as cleaner

import logging
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import os



log = logging.getLogger(__name__)

class CleanDatasetOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CleanDatasetOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']
        df = task_instance.xcom_pull(key="df_engineered")
        df = cleaner.main(df)
        output_dir = file_dir+"/"        
        df.to_csv(output_dir+"20190227_training.csv")
        task_instance.xcom_push(key="df_cleaned", value=df)


class EngineerFeaturePlugun(AirflowPlugin):
    name = "to_enginer_the_feature"
    operators = [CleanDatasetOperator]