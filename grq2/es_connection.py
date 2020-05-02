from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from elasticsearch import RequestsHttpConnection
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

MOZART_ES = None
GRQ_ES = None


def get_mozart_es(es_url, logger):
    global MOZART_ES
    if MOZART_ES is None:
        MOZART_ES = ElasticsearchUtility(es_url, logger)
    return MOZART_ES


def get_grq_es(es_host='localhost', port=9200, logger=None, region='', aws_es_service=False):
    global GRQ_ES

    if GRQ_ES is None:
        es_url = 'http://%s:%d' % (es_host, port)

        if aws_es_service:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            GRQ_ES = ElasticsearchUtility(
                es_url=es_host,
                logger=logger,
                port=port,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
            )
        else:
            GRQ_ES = ElasticsearchUtility(es_url, logger)
    return GRQ_ES
