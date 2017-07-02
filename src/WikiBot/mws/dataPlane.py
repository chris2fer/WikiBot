import time, conf, client, requests, csv, calculators




def createDataPlaneRequestString(region):

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

    schemaname1 = '&SchemaName1=Service'
    dataset1 = '&DataSet1=Prod'
    marketplace1 = '&Marketplace1=prod%3A{0}'.format(AWSRegion)
    hostgroup1 = '&HostGroup1=ALL'
    host1 = '&Host1=ALL'
    servicename1 = '&ServiceName1=IotMoonrakerService'
    methodname1 = '&MethodName1=ALL'
    client1 = '&Client1=ALL'
    metricclass1 = '&MetricClass1=NONE'
    instance1 = '&Instance1=NONE'
    metric1 = '&Metric1=MessageAvailability'
    period1 = '&Period1=FiveMinute'
    stat1 = '&Stat1=avg'
    schemaname2 = '&SchemaName2=Service'
    servicename2 = '&ServiceName2=IotDealer'
    methodname2 = '&MethodName2=Fanout.PUBLISH'
    instance2 = '&Instance2=ALL'
    metric2 = '&Metric2=Drop.AGE'
    stat2 = '&Stat2=n'
    schemaname3 = '&SchemaName3=Service'
    metric3 = '&Metric3=Result.SENT'
    schemaname4 = '&SchemaName4=Service'
    methodname4 = '&MethodName4=fastpath.publish'
    metric4 = '&Metric4=success'
    stat4 = '&Stat4=sum'
    schemaname5 = '&SchemaName5=Service'
    stat5 = '&Stat5=n'
    starttime1 = '&StartTime1=-P{0}D'.format(str(conf.startDaysAgo))
    endtime1 = '&EndTime1=-P{0}D'.format(str(conf.endDaysAgo))
    functionexpression1 = '&FunctionExpression1=M1*100'
    functionexpression2 = '&FunctionExpression2=%28M3%2F%28M2%2BM3%29%29*100'
    functionexpression3 = '&FunctionExpression3=%28M4%2FM5%29*100'
    output = '&OutputFormat=CSV_TRANSPOSE'

    req_base = hostURL + version + action + '&AWSAccessKeyId=' + conf.ak + sig + expires

    fullString = req_base + schemaname1 + dataset1 + marketplace1 + hostgroup1 + host1 \
                + servicename1 + methodname1 + client1 + metricclass1 + instance1 + metric1 \
                + period1 + stat1 + schemaname2 + servicename2 + methodname2 + instance2 \
                + metric2 + stat2 + schemaname3 + metric3 + schemaname4 + methodname4 + metric4 \
                + stat4 + schemaname5 + stat5 + starttime1 + endtime1 + functionexpression1 \
                + functionexpression2 + functionexpression3 + output

    return fullString


def getDataPlaneMetrics(region):

    requestString = createDataPlaneRequestString(region)

    api_response = requests.get(requestString)

    split_data_lines = str(api_response.text).splitlines()

    commaDelimitedData = csv.reader(split_data_lines[6:])

    # Initialize value of minutes where service is below availability threshold
    dataPlaneDegradedMinutes = 0

    # Initialize value of minutes where service is available
    dataPlaneHealthyMinutes = 0

    # Initialize value for availability.
    dataPlaneAvailability = 0.0

    # Initialize iteration count
    iterations = 0

    for row in commaDelimitedData:
        if row[1] and row[2] and row[3]:
            if float(row[1]) < conf.availability_threshold or float(row[2])\
                    < conf.availability_threshold or float(row[3]) < conf.availability_threshold:

                print sorted([row[1],row[2],row[3]])
                dataPlaneDegradedMinutes += 5
                # Record (metricPeriod) minutes of failure

            else:

                # Record (metricPeriod) minutes of availability
                dataPlaneHealthyMinutes += 5

            availabilities = [float(row[1]),float(row[2]),float(row[3])]
            lowestAvailability = sorted(availabilities)[0]

            dataPlaneAvailability += float(lowestAvailability)
            iterations += 1

        # ToDo: Else log failed timestamp because we dont have a complete row of data.


    print 'Available Minutes : ' + str(dataPlaneHealthyMinutes)
    print 'Degraded Minutes  : ' + str(dataPlaneDegradedMinutes)

    availability_metric = float(dataPlaneHealthyMinutes / (dataPlaneHealthyMinutes + float(dataPlaneDegradedMinutes)))*100

    print str(availability_metric)
    print "availability is: " + str(dataPlaneAvailability)
    print "iterations is: " + str(iterations)
    print "calculated availability is: " + str(dataPlaneAvailability/iterations)
    print "Availability Threshold is: " + str(conf.availability_threshold)

    percentageAvailability = (float(dataPlaneHealthyMinutes) / (dataPlaneHealthyMinutes + dataPlaneDegradedMinutes)) * 100
    return {'availability':str(percentageAvailability),'iminutes': str(dataPlaneDegradedMinutes),
                    'aminutes': str(dataPlaneHealthyMinutes),'impacts': {}}
