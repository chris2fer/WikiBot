import datetime, mws.client, conf, logging
import rds.util
from calculators import *


# LOGGING SETUP
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('hello.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Specific Times takes the form of 2016-03-22T20:17:34Z
start = '2016-02-14'
today = datetime.datetime.today()
start = datetime.datetime.strptime(start,'%Y-%m-%d')
next_day = start
iot_regions = ('iad', 'pdx', 'dub', 'nrt')

def get_search_pattern(region):

    return{'iad': 'IotIdentityService metric=$Fault$ dataset=$Prod$ marketplace=$prod:us-east-1$ NOT methodname=($ALL$ OR $BSFPing$ OR $Unknown$) schemaname=Service',
    'pdx': 'IotIdentityService metric=$Fault$ dataset=$Prod$ marketplace=$prod:us-west-2$ NOT methodname=($ALL$ OR $BSFPing$ OR $Unknown$) schemaname=Service',
    'dub': 'IotIdentityService metric=$Fault$ dataset=$Prod$ marketplace=$prod:eu-west-1$ NOT methodname=($ALL$ OR $BSFPing$ OR $Unknown$) schemaname=Service',
    'nrt': 'IotIdentityService metric=$Fault$ dataset=$Prod$ marketplace=$prod:ap-northeast-1$ NOT methodname=($ALL$ OR $BSFPing$ OR $Unknown$) schemaname=Service'}[region]

f_expresion = '(1-SUM(S1)/SUM(S2))*100'


def get_aws_region(r):
    return {'iad': 'us-east-1', 'pdx': 'us-west-2', 'dub': 'eu-west-1', 'nrt': 'ap-northeast-1'}[r]


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


def calculate_events(region,conn):
    m_events = rds.util.get_impact_events(region=region,service='moonraker',conn=conn)
    if m_events:
        m_d0 = m_events[0]['date']
        m_d1 = datetime.date.today()
        m_diff = m_d1 - m_d0
        m_days = m_diff.days
    else:
        m_days = 0
        m_events = []

    t_events = rds.util.get_impact_events(region=region,service='thunderball',conn=conn)
    if t_events:
        t_d0 = t_events[0]['date']
        t_d1 = datetime.date.today()
        t_diff = t_d1 - t_d0
        t_days = t_diff.days
    else:
        t_days = 0
        t_events = []

    return {'moonraker_days_ago': m_days,'moonraker_last_five': m_events[:4],'thunderball_days_ago': t_days,'thunderball_last_five': t_events[:4]}




while (next_day < today):

    # Specific Times takes the form of 2016-03-22T20:17:34Z

    print "date: " + next_day.strftime('%Y-%m-%d')
    next_day = next_day + datetime.timedelta(days=1)

    start = next_day.strftime('%Y-%m-%dT%H:%M:%SZ')
    end = (next_day + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    run_date = (next_day + datetime.timedelta(days=1)).strftime('%Y-%m-%d')




    conn = rds.util.connect_to_db(h=conf.db_host,u=conf.sql_u,p=conf.sql_p,db=conf.db)


    for region in iot_regions:
        logger.info('Region : %s' % region)

        # P R O C E S S   M O O N R A K E R
        logger.info('Proccessing Moonraker')
        service = 'moonraker'
        aws_region = get_aws_region(r=region)

        # GET METRICS : GETS A DICT
        metrics = mws.client.get_service_availability(service="IotMoonrakerService",
                                                  region=aws_region,
                                                  st=start, end=end)

        dp_minutes = metrics['iminutes']
        dp_availability = metrics['availability']
        dp_n = (int(metrics['iminutes']) + int(metrics['aminutes']))
        impacts = metrics['impacts']

        if impacts:
            # LOOP THROUGH THE IMPACT EVENTS
            for d,v in impacts.items():
                d = datetime.datetime.strptime(d, '%Y/%m/%d %H:%M %Z').strftime('%Y-%m-%d')

                # WRITE THE IMPACT TO THE IMPACTS TABLE
                rds.util.write_impact(d=d,r=region,v=v,s=service,conn=conn)




        # P R O C E S S   T H U N D E R B A L L
        logger.info('Proccessing Thunderball')
        service = 'thunderball'
        metrics = mws.client.get_service_availability(search_pattern=get_search_pattern(region),
                                          st=start, end=end,region=region)
        cp_minutes = metrics['iminutes']
        cp_availability = metrics['availability']
        cp_n = (int(metrics['iminutes']) + int(metrics['aminutes']))

        impacts = metrics['impacts']
        if impacts:
            for d,v in impacts.items():
                d = datetime.datetime.strptime(d, '%Y/%m/%d %H:%M PDT').strftime('%Y-%m-%d')
                rds.util.write_impact(d=d,r=region,v=v,s=service,conn=conn)

        # WRITE metric values into the iot_summary_metrics table
        rds.util.record_metric(run_date=run_date,region=region,
                               cp_avail=cp_availability,cp_min=cp_minutes,
                               cp_n=cp_n,dp_avail=dp_availability,dp_min=dp_minutes,
                               dp_n=dp_n,conn=conn)

        # update yearly and quarterly totals
        logger.info("getting yearly total")


        last_impacts = calculate_events(region=region,conn=conn)
        q_metrics = measure_qtr_availability(region=region,conn=conn)
        y_metrics = measure_year_availability(region=region,conn=conn)

