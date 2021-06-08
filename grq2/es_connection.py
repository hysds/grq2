from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from elasticsearch import RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from hysds.celery import app

MOZART_ES = None
GRQ_ES = None


def get_mozart_es(es_url, logger=None):
    global MOZART_ES
    if MOZART_ES is None:
        MOZART_ES = ElasticsearchUtility(es_url, logger)
    return MOZART_ES


def get_grq_es(logger=None):
    global GRQ_ES

    if GRQ_ES is None:
        aws_es = app.conf.get('GRQ_AWS_ES', False)
        es_host = app.conf['GRQ_ES_HOST']
        es_url = app.conf['GRQ_ES_URL']
        region = app.conf['AWS_REGION']

        if aws_es is True:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            GRQ_ES = ElasticsearchUtility(
                es_url=es_url,
                logger=logger,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True,
            )
        else:
            GRQ_ES = ElasticsearchUtility(es_url, logger)
    return GRQ_ES
