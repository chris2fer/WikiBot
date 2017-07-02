import time, conf, client, requests, csv, calculators, logging

logger = logging.getLogger('WikiBot.mws.controlPlane')


def createControlPlaneRequestString(region):

    logger.info('Creating MWS Request string for {0}'.format(region))
    # Resolves an internal region code into AWS region (iad becomes us-east-1)
    AWSRegion = calculators.getAWSRegion(region)

    hostURL = conf.mws_url
    version = '?Version=2007-07-07'
    action = '&Action=GetGraph'
    exp = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                            time.gmtime(time.time() + 900))
    sig = '&Signature=' + client.get_signature(action='GetGraph', aws_sk=conf.sk,
                                        expires=exp)
    expires = '&Expires=' + exp
    schemaname1 = '&SchemaName1=Search'
    pattern1 = '&Pattern1=metric%3D%24Fault%24%20dataset%3D%24Prod%24%20marketplace%3D%24prod%3A{0}%24%20NOT%20methodname%3D%28%24ALL%24%20OR%20%24BSFPing%24%20OR%20%24Unknown%24%29%20schemaname%3DService%20servicename%3D%24IotIdentityService%24'.format(AWSRegion)
    period1 = '&Period1=FiveMinute'
    stat1 = '&Stat1=sum'
    schemaname2 = '&SchemaName2=Search'
    stat2 = '&Stat2=n'
    schemaname3 = '&SchemaName3=Search'
    pattern3 = '&Pattern3=metricclass%3D%24NONE%24%20servicename%3D%24IotGoldenEyeService_beta%24%20dataset%3D%24Prod%24%20marketplace%3D%24prod%3A{0}%24%20hostgroup%3D%24ALL%24%20host%3D%24ALL%24%20metric%3D%24Fault%24%20schemaname%3DService%20NOT%20methodname%3D%24ALL%24'.format(AWSRegion)
    stat3 = '&Stat3=sum'
    schemaname4 = '&SchemaName4=Search'
    pattern4 = '&Pattern4=metricclass%3D%24NONE%24%20servicename%3D%24IotGoldenEyeService_beta%24%20dataset%3D%24Prod%24%20marketplace%3D%24prod%3A{0}%24%20hostgroup%3D%24ALL%24%20host%3D%24ALL%24%20metric%3D%24Time%24%20schemaname%3DService%20NOT%20methodname%3D%24ALL%24'.format(AWSRegion)
    stat4 = '&Stat4=n'
    schemaname5 = '&SchemaName5=Search'
    pattern5 = '&Pattern5=metric%3D%24Fault%24%20dataset%3D%24Prod%24%20marketplace%3D%24prod%3A{0}%24%20NOT%20methodname%3D%28%24ALL%24%20OR%20%24BSFPing%24%20OR%20%24Unknown%24%29%20schemaname%3DService%20servicename%3D%24IotRegistryService%24'.format(AWSRegion)
    stat5 = '&Stat5=sum'
    schemaname6 = '&SchemaName6=Search'
    stat6 = '&Stat6=n'
    starttime1 = '&StartTime1=-P{0}D'.format(str(conf.startDaysAgo))
    endtime1 = '&EndTime1=-P{0}D'.format(str(conf.endDaysAgo))
    functionexpression1 = '&FunctionExpression1=%281-SUM%28S1%29%2FSUM%28S2%29%29*100'
    functionlabel1 = '&FunctionLabel1=Identity%20API%20Availability'
    functionexpression2 = '&FunctionExpression2=%281-SUM%28S3%29%2FSUM%28S4%29%29*100'
    functionlabel2 = '&FunctionLabel2=Rules%20Engine%20API%20Availability'
    functionexpression3 = '&FunctionExpression3=%281-SUM%28S5%29%2FSUM%28S6%29%29*100'
    functionlabel3 = '&FunctionLabel3=Registry%20API%20Availability'
    output = '&OutputFormat=CSV_TRANSPOSE'

    req_base = hostURL + version + action + '&AWSAccessKeyId=' + conf.ak + sig + expires

    fullString = req_base + schemaname1 + pattern1 + period1 + stat1 + schemaname2 \
                + stat2 + schemaname3 + pattern3 + stat3 + schemaname4 + pattern4 \
                + stat4 + schemaname5 + pattern5 + stat5 + schemaname6 + stat6 + starttime1 \
                + endtime1 + functionexpression1 + functionlabel1 + functionexpression2 \
                + functionlabel2 + functionexpression3 + functionlabel3 + output
    logger.info('Request String : ' + fullString)
    return fullString


def getControlPlaneMetrics(region):

    requestString = createControlPlaneRequestString(region)

    api_response = requests.get(requestString)

    split_data_lines = str(api_response.text).splitlines()

    commaDelimitedData = csv.reader(split_data_lines[6:])
    ##  DATA is in form of   DateTime, metric1, metric2, metric3.... etc


    # Initialize value of minutes where service is below availability threshold
    controlPlaneDegradedMinutes = 0

    # Initialize value of minutes where service is available
    controlPlaneHealthyMinutes = 0

    # Initialize value for availability.
    controlPlaneAvailability = 0.0

    # Initialize iteration count
    iterations = 0
    for row in commaDelimitedData:
        if row[1] and row[2] and row[3]:
            if float(row[1]) < conf.availability_threshold or float(row[2]) \
                    < conf.availability_threshold or float(
                row[3]) < conf.availability_threshold:

                print sorted([row[1], row[2], row[3]])
                controlPlaneDegradedMinutes += 5
                # Record (metricPeriod) minutes of failure

            else:

                # Record (metricPeriod) minutes of availability
                controlPlaneHealthyMinutes += 5

            availabilities = [float(row[1]), float(row[2]), float(row[3])]
            lowestAvailability = sorted(availabilities)[0]

            controlPlaneAvailability += float(lowestAvailability)
            iterations += 1

        # ToDo: Else log failed timestamp because we dont have a complete row of data.

    logger.info('Availability : ' + str(controlPlaneAvailability/iterations))
    logger.info('Impactful Minutes : ' + str(controlPlaneDegradedMinutes))
    logger.info('Available Minutes " ' + str(controlPlaneHealthyMinutes))
    percentageAvailability = (float(controlPlaneHealthyMinutes) / (controlPlaneHealthyMinutes + controlPlaneDegradedMinutes)) * 100
    return {'availability':str(percentageAvailability),'iminutes': str(controlPlaneDegradedMinutes),
                    'aminutes': str(controlPlaneHealthyMinutes),'impacts': {}}