import re
import os
import xml.etree.ElementTree as ET

ITEM_LIST_result = ['ParentSHA1', 'FileSHA1', 'FileMD5', 'TrueFileType',
                    'FileSize', 'OrigFileName', 'GRIDIsKnownGood', 'AnalyzeTime',
                    'VirusName', 'AnalyzeStartTime', 'ParentChildRelationship'
                    ]

ITEM_LIST_charac = ['FileSHA1', 'violatedPolicyName', 'Event',
                    'Details', 'image_type'
                    ]

DETAIL_VALUE = ['AUTH', 'CIFS', 'DHCP', 'DNS Response', 'FTP', 
                'HTTP', 'ICMP', 'IM', 'IRC', 'LDAP',
                'P2P', 'POP3', 'Remote Access', 'SMTP', 'SNMP',
                'SQL', 'TCP', 'TFTP', 'UDP', 'WEBMAIL',
                'STREAMING', 'VOIP', 'TUNNELING', 'IMAP4', 'DNS Request',
                'MAIL'
                ]

ITEM_CAV = ['__log_time', 'severity', 'threattype', 'detectionname', 'description',
            'srcip', 'dstip', 'filename', 'protocolgroup', 'dstport',
            'domainname', 'hostname', 'ruleid', 'truefiletype', 'filesize',
            'recipient', 'detectedby', 'filenameinarc', 'interestedip', 'peerip',
            'hasdtasres', 'sha1', 'sender', 'subject', 'url',
            'sha1inarc', 'attachmentfilename', 'attachmentfilesize', 'attachmentfiletype', 'attachmentsha1',
            'ece_severity', 'attack_phase', 'Remarks', 'malwaretype', 'filenameinarc',
            'common_threat_family', 'messageid', 'apt_group', 'apt_campaign', '""', '""'
            ]

ITEM_TMUFE = ['__log_time', 'severity', 'threattype', '""', 'description',
              'srcip', 'dstip', '""', 'protocolgroup', 'dstport', 
              'domainname', 'hostname', '""', '""', '""',
              'recipient', 'detectedby', '""', 'interestedip', 'peerip',
              '""', '""', 'sender', 'subject', 'url',
              '""', '""', '""', '""', '""',
              'ece_severity', 'attack_phase', 'Remarks', '""', '""',
              '""', 'messageid', '""', '""', 'score',
              'category'
              ]

DETAIL_KEY = list(range(1, 25)) + [68, 25]
DETAIL = dict(zip(DETAIL_KEY, DETAIL_VALUE))

def parserXML(xml_file, item_list, script=False):
    if script:
        parser = ET.XMLParser(encoding='utf-8')
        root = ET.fromstring(xml_file, parser=parser)
    else:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    content = []
    violation_content = []
    image_mapping = {}

    risk_level = root.find('OVERALL_RISK_LEVEL').text

    for i in root.findall('IMAGE_TYPE'):
        for j in i.findall('TYPE'):
            image_mapping[j.attrib['id']] = j.text

    for report in root.findall('FILE_ANALYZE_REPORT'):
        image_type = image_mapping[report.attrib['image_type']]
        report_list = [risk_level] + \
                      [report.find(i).text if report.find(i) != None 
                       else None for i in item_list]
        fileSHA1 = report.find('FileSHA1').text
        content.append(tuple(report_list))
        for violation in report.findall('AnalysisViolation'):
            violatedPolicyName = violation.find('violatedPolicyName').text
            
            for evt in violation.findall('AnalysisViolatedEvent'):
                event = evt.find('Event').text
                details = evt.find('Details').text
                violation_list = [fileSHA1, violatedPolicyName, event, details, image_type]
                violation_content.append(tuple(violation_list))
                # pp.pprint(violation_content)
                
    return content, violation_content

def readCustomizedSchema(path):
    files = [i for i in os.listdir(path) if i.startswith('table_')]
    schema = []
    for file in files:
        with open(os.path.join(path, file)) as f:
            schema.append(f.readlines())
    return dict(zip(files, schema))

def vaTablesConvertor(db_object, schemas_dict):
    query = 'SELECT report FROM tb_sandbox_result'
    raw = db_object.fetching(query)
    
    data = [i[0].replace('\\n', '') for i in raw]
    parse_all = [parserXML(i, ITEM_LIST_result, True) for i in data]
    parse_result = [i[0] for i in parse_all]
    parse_charac = [i[1] for i in parse_all]
    va_results_data = [j for i in parse_result for j in i]
    va_charac_data = [j for i in parse_charac for j in i]

    print('[Process] Converting - va_sample_results...')
    schema = schemas_dict['table_va_sample_results.txt']
    db_object.restore(schema, va_results_data)

    print('[Process] Converting - va_sample_charac...')
    schema = schemas_dict['table_va_sample_charac.txt']
    db_object.restore(schema, va_charac_data)


def protocolRequestLogs(db_object, schemas_dict):
    query = 'SELECT * FROM tb_protocol_request_logs'
    raw = db_object.fetching(query)
    col1 = [i[0].split(',') for i in raw]
    col1 = list(set([DETAIL[int(j)] for i in col1 for j in i if j != '0']))
    length = len(col1)
    protocol_anal = ','.join(col1)
    col2 = [i[1] for i in raw]
    traffic = int(sum(col2)/1024**3)
    col3 = [i[2].split(',') for i in raw]
    DNS = [i[0] for i in col3]
    HTTP = [i[1] for i in col3]
    Email = [i[2] for i in col3]
    sum_DNS = int(sum([int(re.search(r'\d+', i).group()) for i in DNS])/1024**2)
    sum_HTTP = int(sum([int(re.search(r'\d+', i).group()) for i in HTTP])/1024**2)
    sum_Email = int(sum([int(re.search(r'\d+', i).group()) for i in Email])/1024**2)
    data = [tuple([length, traffic, sum_HTTP, sum_Email, sum_DNS, protocol_anal])]
    print('[Process] Converting - data_statistics...')
    schema = schemas_dict['table_data_statistics.txt']
    db_object.restore(schema, data)

def logTableConvertor(db_object, schemas_dict):

    print('[Process] Converting - log from tb_cav_total_logs...')
    query = ['SELECT']
    query.append(', '.join(ITEM_CAV))
    query.append('FROM tb_cav_total_logs')
    query = ' '.join(query)
    raw = db_object.fetching(query)
    query = 'SELECT CASE WHEN interestedip == srcip THEN srchost ELSE dsthost END FROM tb_cav_total_logs'
    interestedhostColumn = db_object.fetching(query)
    data = list(zip(raw, interestedhostColumn))
    data_cav = [tuple([i for j in before for i in j]) for before in data]

    print('[Process] Converting - log from tb_tmufe_total_logs...')
    query = ['SELECT']
    query.append(', '.join(ITEM_TMUFE))
    query.append('FROM tb_tmufe_total_logs')
    query = ' '.join(query)
    raw = db_object.fetching(query)
    query = 'SELECT CASE WHEN interestedip == srcip THEN srchost ELSE dsthost END FROM tb_tmufe_total_logs'
    interestedhostColumn = db_object.fetching(query)
    data = list(zip(raw, interestedhostColumn))
    data_tmufe = [tuple([i for j in before for i in j]) for before in data]

    schema = schemas_dict['table_log.txt']
    data = data_cav + data_tmufe
    db_object.restore(schema, data)


