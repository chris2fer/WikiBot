
p = ''
wikiUser = 'deariec'
sql_u = ''
startDaysAgo = 1
endDaysAgo = 0
api_url = 'https://w.amazon.com/api.php'
mws_url = 'http://monitor-api.amazon.com/'
db_host = 'iot-dashboard-metrics.cl10f7vdnpjk.us-east-1.rds.amazonaws.com'
db = 'metrics'
db_event_table = 'iot_impact_events'
db_m_tbl = 'iot_summary_metrics'
#db_m_tbl = 'test2'
writeToDatabase = True
updateWiki = True
updateTotals = True
wiki_page = 'Public/iot/graphs/summaryTable'
wiki_page_id = '2140187'
metric_period = 'FiveMinute'
availability_threshold = 95.0
fanout_threshold = 5
###########################################
regions = ('iad', 'pdx', 'dub', 'nrt', 'fra', 'sin', 'syd', 'icn')
AWSRegions = {'iad': 'us-east-1', 'pdx': 'us-west-2', 'dub': 'eu-west-1', 'nrt': 'ap-northeast-1', 'fra': 'eu-central-1', 'sin': 'ap-southeast-1', 'syd': 'ap-southeast-2', 'icn': 'ap-northeast-2'}

