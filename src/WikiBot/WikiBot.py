import os, logging, logging.config, datetime, sys, conf
import mws.controlPlane, mws.rulesEngine, mws.dataPlane, wiki.wiki, rds.util, time
from calculators import *

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# LOGGING SETUP
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('WikiBot')



today = datetime.date.today()
start = (today - datetime.timedelta(days=conf.startDaysAgo)).strftime(format='%Y-%m-%dT%H:%M:%SZ')
end = (today - datetime.timedelta(days=conf.endDaysAgo)).strftime(format='%Y-%m-%dT%H:%M:%SZ')

###########################################
iot_regions = conf.regions
write_to_db = conf.writeToDatabase
update_wiki = conf.updateWiki

# Specific Times takes the form of 2016-03-22T20:17:34Z
# Relative Times take the form of -P4D or -PT4H




def update_yearly_total(controlPlane_im,controlPlane_am,dataPlane_im,dataPlane_am,prev_week):

    #{'region': region, 'year': year, 'controlPlane_im': controlPlane_im, 'controlPlane_avm': controlPlane_avm, 'dataPlane_im': dataPlane_im, 'dataPlane_avm': dataPlane_avm}
    if prev_week:
        logger.info('Pevious Total found')
        controlPlane_im = controlPlane_im + int(prev_week['controlPlane_im'])
        controlPlane_am = controlPlane_am + int(prev_week['controlPlane_avm'])
        dataPlane_im = dataPlane_im + int(prev_week['dataPlane_im'])
        dataPlane_am = dataPlane_am + int(prev_week['dataPlane_avm'])
    else:
        logger.info('No Previous Week found.')
        controlPlane_im = controlPlane_im
        controlPlane_am = controlPlane_am
        dataPlane_im = dataPlane_im
        dataPlane_am = dataPlane_am

    return {'controlPlane_im': controlPlane_im, 'controlPlane_am': controlPlane_am, 'dataPlane_im': dataPlane_im, 'dataPlane_am': dataPlane_am}


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


if __name__ == '__main__':

    logger.info('Starting Wikibot script.')
    # Begin building the wiki content by starting with the top content
    reader = open("./files/table_head", "r")
    wikiTableContent = reader.read()
    reader = open("./files/table_row", "r")
    table_row = reader.read()
    reader = open("./files/table_foot", "r")
    table_foot = reader.read()
    table_foot = table_foot.replace('%{updated_date}',time.strftime('%Y-%m-%d %H:%M'))

    # Create a connection object to our MySQL instance
    conn = rds.util.connect_to_db(h=conf.db_host,u=conf.sql_u,p=conf.sql_p,db=conf.db)
    run_date = str((datetime.datetime.today() - datetime.timedelta(days=conf.endDaysAgo)).__format__('%Y-%m-%d'))

    for region in iot_regions:
        logger.info('Region : %s' % region)

        # P R O C E S S   M O O N R A K E R
        logger.info('Proccessing Moonraker')
        service = 'moonraker'
        aws_region = getAWSRegion(r=region)

        # GET METRICS : GETS A DICT
        metrics = mws.dataPlane.getDataPlaneMetrics(region=region)

        dataPlane_minutes = metrics['iminutes']
        dataPlane_availability = metrics['availability']
        dataPlane_n = (int(metrics['iminutes']) + int(metrics['aminutes']))
        impacts = metrics['impacts']

        # LOOP THROUGH THE IMPACT EVENTS
        if impacts:
            for d,v in impacts.items():
                d = datetime.datetime.strptime(d, '%Y/%m/%d %H:%M PDT').strftime('%Y-%m-%d')

                # WRITE THE IMPACT TO THE IMPACTS TABLE
                if conf.writeToDatabase:
                    rds.util.write_impact(d=d,r=region,v=v,s=service,conn=conn)




        # P R O C E S S   T H U N D E R B A L L

        logger.info('Proccessing Thunderball')
        service = 'thunderball'

        metrics = mws.controlPlane.getControlPlaneMetrics(region=region)
        controlPlane_minutes = metrics['iminutes']
        controlPlane_availability = metrics['availability']
        controlPlane_n = (int(metrics['iminutes']) + int(metrics['aminutes']))

        impacts = metrics['impacts']
        if impacts:
            for d,v in impacts.items():
                d = datetime.datetime.strptime(d, '%Y/%m/%d %H:%M PDT').strftime('%Y-%m-%d')

                if conf.writeToDatabase:
                    rds.util.write_impact(d=d,r=region,v=v,s=service,conn=conn)


#############  Process Rules Engine

        logger.info('Proccessing Rules Engine')
        service = 'rulesengine'

        metrics = mws.rulesEngine.getRulesEngineMetrics(region=region)
        rulesEngine_minutes = metrics['iminutes']
        rulesEngine_availability = metrics['availability']
        rulesEngine_n = (
        int(metrics['iminutes']) + int(metrics['aminutes']))

        impacts = metrics['impacts']
        if impacts:
            for d,v in impacts.items():
                d = datetime.datetime.strptime(d, '%Y/%m/%d %H:%M PDT').strftime('%Y-%m-%d')

                if conf.writeToDatabase:
                    rds.util.write_impact(d=d,r=region,v=v,s=service,conn=conn)



        # WRITE metric values into the iot_summary_metrics table
        logger.info('Recording metric in metrics table')
        logger.info('run_date :' + run_date)
        if conf.writeToDatabase:
            logger.info('writeToDatabase was set to True')
            rds.util.record_metric(run_date=run_date, region=region,
                                   cp_avail=controlPlane_availability, cp_min=controlPlane_minutes,
                                   cp_n=controlPlane_n, dp_avail=dataPlane_availability, dp_min=dataPlane_minutes,
                                   dp_n=dataPlane_n, re_avail=rulesEngine_availability, re_min=rulesEngine_minutes,re_n=rulesEngine_n, conn=conn)

        # update yearly and quarterly totals
        logger.info("getting yearly total")

        if conf.updateTotals:
            #last_impacts = calculate_events(region=region,conn=conn)
            w_metrics = measure_week_availability(region=region,conn=conn)
            q_metrics = measure_qtr_availability(region=region,conn=conn)
            y_metrics = measure_year_availability(region=region,conn=conn)

            logger.info('Contructing new wiki table row.')
            new_row = wiki.wiki.add_wiki_table_row(region=region,controlPlane_m_wk=w_metrics['cp_m'],
                                controlPlane_avb_wk=w_metrics['cp_av'],controlPlane_m_ytd=y_metrics['cp_m'],
                                controlPlane_m_qtd=q_metrics['cp_m'],
                                controlPlane_avb_ytd=y_metrics['cp_av'],
                                controlPlane_avb_qtd=q_metrics['cp_av'], dataPlane_m_wk=w_metrics['dp_m'],
                                dataPlane_m_qtd=q_metrics['dp_m'],
                                dataPlane_m_ytd=y_metrics['dp_m'],
                                dataPlane_avb_wk=w_metrics['dp_av'],
                                dataPlane_avb_qtd=q_metrics['dp_av'],
                                dataPlane_avb_ytd=y_metrics['dp_av'],
                            #    rulesEngine_m_qtd=q_metrics['re_m'],
                            #    rulesEngine_m_wk=w_metrics['re_m'],
                            #    rulesEngine_m_ytd=y_metrics['re_m'],
                            #    rulesEngine_avb_wk=w_metrics['re_av'],
                            #    rulesEngine_avb_qtd=q_metrics['re_av'],
                            #    rulesEngine_avb_ytd=y_metrics['re_av'],
                                table=table_row
                                )


            wikiTableContent = wikiTableContent + new_row


    logger.info('Completed looping through regions')
    # E N D    O F   R E G I O N   I T E R A T I O N

    if conf.updateWiki:
        logger.info('updateWiki was set to True')
        if new_row:  # Not sure why i don't test wikiTableContent instead
            cookies = None
            wiki_edit_token = wiki.wiki.get_token(conf.wiki_page_id, conf.wiki_page)
            wikiTableContent = wikiTableContent + table_foot
            wiki.wiki.write_wiki_page(table_code=wikiTableContent,token=wiki_edit_token,page=conf.wiki_page)

    # F I N I S H    U P
    if conn:
        rds.util.close_the_connection(conn)

# E N D   O F   F I L E
