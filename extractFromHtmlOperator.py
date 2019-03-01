import sys
import os
file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(file_dir+"/../elt/script")

import toExtractCurrentRaceCardFromHtml as extract
import toEngineerFeaturesToCurrentRaceCard as engineer



import logging
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import os



log = logging.getLogger(__name__)

class ExtractFromHtmlOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ExtractFromHtmlOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("to extract current race card from html....!")
        df = extract.main('/Users/felixleung/airflow/datafile/html' )
        log.info("finished extract current race card......!")
      
        task_instance = context['task_instance']
        task_instance.xcom_push(key="df", value=df)


class ExtractFromHtmlPlugun(AirflowPlugin):
    name = "to_extract_from_html"
    operators = [ExtractFromHtmlOperator]