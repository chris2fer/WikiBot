from __future__ import print_function
import mysql.connector
import conf
import logging


logger = logging.getLogger(__name__)


def connect_to_db(h, u, p, db):
    conn = mysql.connector.connect(user=u, password=p, host=h, database=db)
    return conn


def record_metric(run_date,region,cp_avail,cp_min,cp_n,dp_avail,dp_min,dp_n,re_avail,re_min,re_n,conn):

    """
    :param week:
    :type week: str
    """
    add_metric = ("INSERT INTO iot_summary_metrics "
                   "(run_date, region, cp_avail, cp_min, cp_n, dp_avail, dp_min, dp_n, re_avail, re_min, re_n) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    data_metric = (run_date,region,cp_avail,cp_min,cp_n,dp_avail,dp_min,dp_n,re_avail,re_min,re_n)

    try:
        conn.cursor().execute(add_metric, data_metric)
        conn.commit()
    except Exception, e:
        print (e)
        pass

def get_yearly_total(region,conn):

    cursor = conn.cursor()

    sql = "SELECT * FROM iot_yearly WHERE region = '%s'"  %  region

    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            region = row[0]
            year = row[1]
            controlPlane_im = row[2]
            controlPlane_avm = row[3]
            dataPlane_im = row[4]
            dataPlane_avm = row[5]
            rulesEngine_im = row[6]
            rulesEngine_avm = row[7]
            return {'region': region, 'year': year, 'controlPlane_im': controlPlane_im, 'controlPlane_avm': controlPlane_avm, 'dataPlane_im': dataPlane_im, 'dataPlane_avm': dataPlane_avm, 'rulesEngine_im': rulesEngine_im, 'rulesEngine_avm': rulesEngine_avm}

    except:
       print ("Error: unable to fecth data")


def get_quarterly_total(qrt,year,region,conn):

    cursor = conn.cursor()

    sql = "SELECT * FROM iot_quarterly WHERE region = '%s'AND quarter = '%d' AND year = '%d'"   % (region,qrt,year)

    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        for row in results:
            region = row[0]
            quarter = row[1]
            controlPlane_im = row[2]
            controlPlane_avm = row[3]
            dataPlane_im = row[4]
            dataPlane_avm = row[5]
            rulesEngine_im = row[6]
            rulesEngine_avm = row[7]
            return {'region': region, 'year': year, 'controlPlane_im': controlPlane_im, 'controlPlane_avm': controlPlane_avm, 'dataPlane_im': dataPlane_im, 'dataPlane_avm': dataPlane_avm, 'rulesEngine_im': rulesEngine_im, 'rulesEngine_avm':rulesEngine_avm}

    except:
       print ("Error: unable to fecth data")

def write_yearly_total(region,controlPlane_im,controlPlane_am,dataPlane_im,dataPlane_am,rulesEngine_im,rulesEngine_am,year,conn):
    cursor = conn.cursor()
    sql = "UPDATE iot_yearly SET controlPlane_impacted_m = %d, controlPlane_available_m = %d, dataPlane_impacted_m = %d, dataPlane_available_m = %d, rulesEngine_impacted_m = %d, rulesEngine_available_m = %d WHERE region = '%s' AND year = %d" % (controlPlane_im, controlPlane_am, dataPlane_im, dataPlane_am, rulesEngine_im, rulesEngine_am, region, year)
    print (sql)
    try:
        cursor.execute(sql)

    except:
        print ("Update yearly failed!!!!")
        conn.rollback()



def write_impact(d,r,v,s,conn):
    sql = "INSERT INTO %s (event_date, region, value, service) VALUES ('%s', '%s', '%s', '%s')" % (conf.db_event_table, d,r,v,s)
    print (sql)
    try:
        cursor = conn.cursor()
        s = cursor.execute(sql)
        print (sql)

    except:
        conn.rollback()

def write_quarterly_total(region,controlPlane_im,controlPlane_am,dataPlane_im,dataPlane_am,rulesEngine_im,rulesEngine_am,quarter,conn):
    cursor = conn.cursor()

    sql = "UPDATE iot_quarterly SET controlPlane_impacted_m = %d, controlPlane_available_m = %d, dataPlane_impacted_m = %d, dataPlane_available_m = %d, rulesEngine_impacted_m = %d, rulesEngine_available_m = %d WHERE region = '%s' AND quarter = %d" % (controlPlane_im, controlPlane_am, dataPlane_im, dataPlane_am, rulesEngine_im, rulesEngine_am, region, quarter)
    try:
        print (sql)
        cursor.execute(sql)
        conn.commit()

    except:
        conn.rollback()

def get_impact_events(region,service,conn):

    sql = 'SELECT *  FROM %s Where region = "%s" AND service = "%s" ORDER BY event_date DESC' % (conf.db_event_table, region, service)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    events =[]
    for row in results:
        date = row[1]
        region = row[2]
        service = row[3]
        value = row[4]

        events.append({'date': date, 'region': region, 'service': service, 'value': value})

    return events

def close_the_connection(conn):
    """
    :return: None
    """
    conn.commit()
    conn.cursor().close()
    conn.close()


def reset_tables():

    conn = connect_to_db(h=conf.db_host,u=conf.sql_u,p=conf.sql_p,db=conf.db)
    cursor = conn.cursor()

    for table in ('iot_yearly','iot_quarterly','iot_summary_metrics','iot_impact_events'):
        sql = 'DELETE FROM %s' % table
        cursor.execute(sql)

    conn.commit()
    conn.close()




