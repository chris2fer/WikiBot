import json
import hmac
import urllib
from base64 import b64encode
from hashlib import sha1
import requests
from requests.auth import HTTPBasicAuth
import conf
import logging


logger = logging.getLogger(__name__)

api_url = conf.api_url
def get_sig0(action,aws_sk,expires):

    query_string = action + expires

    h = hmac.new(aws_sk, query_string, sha1)
    signature = urllib.quote((b64encode(h.digest()).rstrip()))

    return signature


def truncate(f, n=2):
    '''Truncates/pads a float f to n decimal places without rounding'''
    print f
    if f == 100:
        return '100'
    elif f == 0:
        return '0'
    s = '%.12f' % f
    i, p, d = s.partition('.')
    return str('.'.join([i, (d+'0'*n)[:n]]))




def insertMetric(metric, value, table):

    metric = '%{' + metric + '}'
    tempTable = ''
    tempTable = table.replace(metric, value)
    return tempTable


def importTemplate(path):
    file = open(path, "r")
    tempTable = file.read()
    return tempTable


def get_token(page_id, page):
    initPayload = {'format': 'json', 'bot': 'true', 'action': 'query', 'prop': 'info', 'intoken': 'edit',
                   'titles': page}
    token_response = requests.post(api_url, verify=False, auth=HTTPBasicAuth(conf.wikiUser, conf.p), data=initPayload)
    global cookies

    cookies = token_response.cookies
    logger.info(token_response.text)
    response_json = json.loads(token_response.text)
    edit_token = response_json['query']['pages'][page_id]['edittoken']
    return edit_token


def write_wiki_page(table_code,token,page):
    editPayload = {'format': 'json', 'bot': 'true', 'action': 'edit', 'summary': 'Edit by API', 'token': token,
               'title': page, 'text': table_code}
    r = requests.post(api_url, verify=False, auth=HTTPBasicAuth(conf.wikiUser, conf.p), data=editPayload, cookies=cookies)
    return r


def add_wiki_table_row(region, table, controlPlane_m_wk, controlPlane_m_qtd, controlPlane_m_ytd,
                           controlPlane_avb_wk, controlPlane_avb_qtd,
                           controlPlane_avb_ytd, dataPlane_m_wk, dataPlane_m_qtd, dataPlane_m_ytd,
                           dataPlane_avb_wk, dataPlane_avb_qtd, dataPlane_avb_ytd,
                           rulesEngine_m_wk='', rulesEngine_m_qtd='', rulesEngine_m_ytd='',
                           rulesEngine_avb_wk='', rulesEngine_avb_qtd='', rulesEngine_avb_ytd=''):
    # type: (object, object, object, object, object, object, object, object, object, object, object, object, object, object, object, object) -> object


    table = insertMetric('region', value=region.upper(), table=table)
    table = insertMetric('controlPlane-im-wk', value=str(controlPlane_m_wk), table=table)
    table = insertMetric('controlPlane-avb-wk', value=truncate(controlPlane_avb_wk),
                                   table=table)
    table = insertMetric('controlPlane-avb-qtd', value=truncate(controlPlane_avb_qtd),
                                   table=table)
    table = insertMetric('controlPlane-avb-ytd', value=truncate(controlPlane_avb_ytd),
                                   table=table)
    table = insertMetric('controlPlane-im-ytd', value=str(controlPlane_m_ytd), table=table)
    table = insertMetric('controlPlane-im-qtd', value=str(controlPlane_m_qtd), table=table)
    table = insertMetric('dataPlane-im-wk', value=str(dataPlane_m_wk), table=table)
    table = insertMetric('dataPlane-avb-wk', value=truncate(dataPlane_avb_wk),
                                   table=table)
    table = insertMetric('dataPlane-avb-qtd', value=truncate(dataPlane_avb_qtd),
                                   table=table)
    table = insertMetric('dataPlane-avb-ytd', value=truncate(dataPlane_avb_ytd),
                                   table=table)
    table = insertMetric('dataPlane-im-ytd', value=str(dataPlane_m_ytd), table=table)
    table = insertMetric('dataPlane-im-qtd', value=str(dataPlane_m_qtd), table=table)
   # table = insertMetric('rulesEngine-im-wk', value=str(rulesEngine_m_wk), table=table)
   # table = insertMetric('rulesEngine-avb-wk', value=truncate(rulesEngine_avb_wk), table=table)
   # table = insertMetric('rulesEngine-avb-qtd', value=truncate(rulesEngine_avb_qtd), table=table)
   # table = insertMetric('rulesEngine-avb-ytd', value=truncate(rulesEngine_avb_ytd), table=table)
   # table = insertMetric('rulesEngine-im-ytd', value=str(rulesEngine_m_ytd), table=table)
   # table = insertMetric('rulesEngine-im-qtd', value=str(rulesEngine_m_qtd), table=table)


    return table

