import time, conf, client, requests, csv, calculators, logging

#logger = logging.getLogger('WikiBot.mws.rulesEngine')
logger = logging.getLogger('___name___')


def rulesEngineRequestString(region):


    logger.info('Creating MWS Request string for {0}'.format(region))
    # Resolves an internal region code into AWS region (iad becomes us-east-1)
    AWSRegion = calculators.getAWSRegion(region)

    hostURL = conf.mws_url
    version = '?Version=2007-07-07'
    action = '&Action=GetGraph'
    exp = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                        time.gmtime(time.time() + 900))
    sig = '&Signature=' + client.get_signature(action='GetGraph',
                                               aws_sk=conf.sk,
                                               expires=exp)
    expires = '&Expires=' + exp
    schemaname1 = '&SchemaName1=Service'
    dataset1 = '&DataSet1=Prod'
    marketplace1 = '&Marketplace1=prod%3A{0}'.format(AWSRegion)
    hostgroup1 = '&HostGroup1=ALL'
    host1 = '&Host1=ALL'
    servicename1 = '&ServiceName1=IotGoldenEyeRuntimeService'
    methodname1 = '&MethodName1=ProcessMessage'
    client1 = '&Client1=ALL'
    metricclass1 = '&MetricClass1=NONE'
    instance1 = '&Instance1=NONE'
    metric1 = '&Metric1=ProcessMessageSuccessAndRetry'
    period1 = '&Period1=FiveMinute'
    stat1 = '&Stat1=avg'
    label1 = '&Label1=ProcessMessageSuccessAndRetry%20avg'
    schemaname2 = '&SchemaName2=Service'
    metric2 = '&Metric2=ProcessMessageError'
    stat2 = '&Stat2=sum'
    starttime1 = '&StartTime1=-P{0}D'.format(str(conf.startDaysAgo))
    endtime1 = '&EndTime1=-P{0}D'.format(str(conf.endDaysAgo))
    functionexpression1 = '&FunctionExpression1=M1*100'
    output = '&OutputFormat=CSV_TRANSPOSE'


    req_base = hostURL + version + action + '&AWSAccessKeyId=' + conf.ak + sig + expires

    fullString = req_base + schemaname1 + dataset1 + marketplace1 + hostgroup1 \
                + host1 + servicename1 + methodname1 + client1 + metricclass1 +instance1 \
                + metric1 + period1 + stat1 + label1 + schemaname2 + metric2 + stat2 \
                + starttime1 + endtime1 + functionexpression1 + output

    logger.info('Request String : ' + fullString)

    return fullString



def getRulesEngineMetrics(region):

    requestString = rulesEngineRequestString(region)

    api_response = requests.get(requestString)

    split_data_lines = str(api_response.text).splitlines()

    commaDelimitedData = csv.reader(split_data_lines[6:])

    # Initialize value of minutes where service is below availability threshold
    rulesEngineDegradedMinutes = 0

    # Initialize value of minutes where service is available
    rulesEngineHealthyMinutes = 0

    # Initialize value for availability.
    rulesEngineAvailability = 0.0

    # Initialize iteration count
    iterations = 0

    for row in commaDelimitedData:
        iterations  += 1
        if row[1]:
            print row[1]
            if float(row[1]) < conf.availability_threshold:

                rulesEngineDegradedMinutes += 5

            else:

                rulesEngineHealthyMinutes += 5
                logger.info("Degraded Minutes found for " + row[0])

    percentageAvailability = (float(rulesEngineHealthyMinutes) / (
                rulesEngineHealthyMinutes + rulesEngineDegradedMinutes)) * 100

    logger.info('Availability : ' + str(rulesEngineAvailability/iterations))
    logger.info('Impactful Minutes : ' + str(rulesEngineDegradedMinutes))
    logger.info('Available Minutes " ' + str(rulesEngineHealthyMinutes))

    return {'availability': str(percentageAvailability),
            'iminutes': str(rulesEngineDegradedMinutes),
            'aminutes': str(rulesEngineHealthyMinutes), 'impacts': {}}

