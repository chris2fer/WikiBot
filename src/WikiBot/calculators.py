import datetime,conf,logging

logger = logging.getLogger('WikiBot.calculators')

def get_current_quarter():
    m = int(datetime.date.today().strftime(format='%m'))
    if m <= 3:
        return 1
    elif m <= 6:
        return 2
    elif m <= 9:
        return 3
    else:
        return 4


def measure_qtr_availability(region,conn):
    c = conn.cursor()
    qtr = get_current_quarter()
    yr = int(datetime.date.today().strftime(format='%Y'))
    yr2 = yr + 1
    if qtr == 1:
        c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%d-01-01' AND '%d-04-01'" % (conf.db_m_tbl,region,yr,yr))
    elif qtr == 2:
        c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%s-04-01' AND '%s-07-01'" % (conf.db_m_tbl,region,yr,yr))
    elif qtr == 3:
        c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%d-07-01' AND '%d-10-01'" % (conf.db_m_tbl,region,yr,yr))
    else:
        c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%s-10-01' AND '%s-01-01'" % (conf.db_m_tbl,region,yr,yr2))

    da = c.fetchall()
    cp_av = 0
    cp_m = 0
    dp_av = 0
    dp_m = 0
    re_av = 0
    re_m = 0
    if da:
        for row in da:
            cp_av += float(row[2])
            cp_m += int(row[3])
            dp_av += float(row[5])
            dp_m += int(row[6])
            re_av += 100.0
            re_m = 0

        return {'cp_av': (cp_av / len(da)),
                'cp_m': cp_m,
                'dp_av': (dp_av / len(da)),
                'dp_m': dp_m,
                're_av': (re_av / len(da)),
                're_m': re_m
                }
    else:
        return {'cp_av': 100,
                'cp_m': 0,
                'dp_av': 100,
                'dp_m': 0,
                're_av': 100,
                're_m': 0
                }


def measure_year_availability(region,conn):
    c = conn.cursor()
    yr = int(datetime.date.today().strftime(format='%Y'))

    c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%d-01-01' AND '%d-12-31'" % (conf.db_m_tbl,region,yr,yr))

    da = c.fetchall()
    cp_av = 0
    cp_m = 0
    dp_av = 0
    dp_m = 0
    re_av = 0
    re_m = 0
    if da:
        for row in da:
            cp_av += float(row[2])
            cp_m += int(row[3])
            dp_av += float(row[5])
            dp_m += int(row[6])
            re_av += 100.0 # += float(row[8])
            re_m = 0 # +=int(row[9])
        return {'cp_av': (cp_av / len(da)),
                'cp_m': cp_m,
                'dp_av': (dp_av / len(da)),
                'dp_m': dp_m,
                're_av': (re_av / len(da)),
                're_m': re_m,
                }
    else:
        return {'cp_av': 100,
                'cp_m': 0,
                'dp_av': 100,
                'dp_m': 0,
                're_av': 100,
                're_m': 0
                }


def measure_week_availability(region,conn):
    c = conn.cursor()
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=7)).strftime(format='%Y-%m-%d')
    end = today.strftime(format='%Y-%m-%d')

    c.execute("SELECT * FROM %s WHERE region = '%s' AND run_date BETWEEN '%s' AND '%s'" % (conf.db_m_tbl,region,start,end))

    data = c.fetchall()
    cp_av = 0
    cp_m = 0
    dp_av = 0
    dp_m = 0
    re_av = 0
    re_m = 0
    if data:

        for row in data:
            cp_av += float(row[2])
            cp_m += int(row[3])
            dp_av += float(row[5])
            dp_m += int(row[6])
            re_av += 100.0 #+= float(row[8])
            re_m = 0 #+= int(row[9])
        return {'cp_av': (cp_av / len(data)),
                'cp_m': cp_m,
                'dp_av': (dp_av / len(data)),
                'dp_m': dp_m,
                're_av': (re_av / len(data)),
                're_m': re_m
                }
    else:
        return {'cp_av': 100,
                'cp_m': 0,
                'dp_av': 100,
                'dp_m': 0,
                're_av': 100,
                're_m': 0
                }


def getAWSRegion(r):
    logger.info("Resolving AWS region for {0}".format(r))
    # Grab Dict from the config file and evaluate based on internal region
    return conf.AWSRegions[r]


