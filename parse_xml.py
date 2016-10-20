import os
import pprint as pp
import xml.etree.ElementTree as ET


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


if __name__ == '__main__':
    file_name = '94FD67BED746BC2E530BA1048557E9C27C2A6990.xml'
    item_list = [
        'ParentSHA1',
        'FileSHA1',
        'FileMD5',
        'TrueFileType',
        'FileSize',
        'OrigFileName',
        'GRIDIsKnownGood',
        'AnalyzeTime',
        'VirusName',
        'AnalyzeStartTime',
        'ParentChildRelationship'
        ]

    item_list_charac = [
        'FileSHA1',
        'violatedPolicyName',
        'Event',
        'Details',
        'image_type'
        ]

    script = '''<?xml version="1.0" encoding="UTF-8"?>\n
    <REPORTS version="1.1">\n
    <IMAGE_TYPE>\n
    \t<TYPE id="638AAC3DB25A69BF">XPSP3</TYPE>\n
    \t<TYPE id="A438112341E09F9A">W764bit</TYPE>\n
    </IMAGE_TYPE>\n
    <FILE_ANALYZE_REPORT image_type="638AAC3DB25A69BF">\n
    \t<FileSHA1>4DB010E467B1E09A8C9C7956EEA533AA6A88EE4A</FileSHA1>\n
    \t<TrueFileType>GNU ZIP</TrueFileType>\n
    \t<FileSize>693</FileSize>\n
    \t<OrigFileName>user.min.js</OrigFileName>\n
    \t<OverallROZRating>-1</OverallROZRating>\n
    \t<FileMD5>680B369C830CD6986D3BB3A0A3FEF32D</FileMD5>\n
    \t<MalwareSourceIP/>\n
    \t<MalwareSourceHost/>\n
    \t<ROZRating>-1</ROZRating>\n
    \t<GRIDIsKnownGood>-1</GRIDIsKnownGood>\n
    \t<AnalyzeTime>2016-07-29 08:35:08</AnalyzeTime>\n
    \t<VirusDetected>0</VirusDetected>\n
    \t<EngineVersion>9.862.1057</EngineVersion>\n
    \t<PatternVersion>12.679.92</PatternVersion>\n
    \t<VirusName/>\n
    \t<PcapReady>0</PcapReady>\n
    \t<ParentChildRelationship/>\n
    \t<CensusPrevalence>-1</CensusPrevalence>\n
    \t<SandcastleClientVersion>6.0.1204</SandcastleClientVersion>\n
    \t<AnalyzeStartTime>2016-07-29 08:35:08</AnalyzeStartTime>\n
    \t<SampleFilePassword/>\n
    \t<DuplicateSHA1>0</DuplicateSHA1>\n
    </FILE_ANALYZE_REPORT>\n
    <FILE_ANALYZE_REPORT image_type="638AAC3DB25A69BF">\n
    \t<ParentSHA1>4DB010E467B1E09A8C9C7956EEA533AA6A88EE4A</ParentSHA1>\n
    \t<FileSHA1>DE60FEBC3069F8897E828191B264FBA3F95F87A6</FileSHA1>\n
    \t<TrueFileType>ASCII text</TrueFileType>\n
    \t<FileSize>1352</FileSize>\n
    \t<OrigFileName>NONAMEFL</OrigFileName>\n
    \t<OverallROZRating>-1</OverallROZRating>\n
    \t<FileMD5>4C5AD875932CBF0B0A0278358B280863</FileMD5>\n
    \t<MalwareSourceIP/>\n
    \t<MalwareSourceHost/>\n
    \t<ROZRating>-1</ROZRating>\n
    \t<GRIDIsKnownGood>-1</GRIDIsKnownGood>\n
    \t<AnalyzeTime>2016-07-29 08:35:08</AnalyzeTime>\n
    \t<VirusDetected>0</VirusDetected>\n
    \t<EngineVersion>9.862.1057</EngineVersion>\n
    \t<PatternVersion>12.679.92</PatternVersion>\n
    \t<VirusName/>\n
    \t<PcapReady>0</PcapReady>\n
    \t<ParentChildRelationship>Extracted from archive</ParentChildRelationship>\n
    \t<CensusPrevalence>-1</CensusPrevalence>\n
    \t<SandcastleClientVersion>6.0.1204</SandcastleClientVersion>\n
    \t<AnalyzeStartTime>2016-07-29 08:35:08</AnalyzeStartTime>\n
    \t<SampleFilePassword/>\n
    \t<DuplicateSHA1>0</DuplicateSHA1>\n
    </FILE_ANALYZE_REPORT>\n
    \n
    \n
    <OVERALL_RISK_LEVEL>-1</OVERALL_RISK_LEVEL>\n</REPORTS>\n
    '''

    # Testing Case
    # a, b = parserXML(script, item_list, True)
    a, b = parserXML(file_name, item_list, False)
    # print(len(a))
    # print(a)
    # print(b)
    # print(a[0])
