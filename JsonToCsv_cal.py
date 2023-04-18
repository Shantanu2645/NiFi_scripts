import json
import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from java.io import BufferedReader, InputStreamReader
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processor.io import OutputStreamCallback
import csv
from java.io import StringWriter



class PyInputStreamCallback(InputStreamCallback):
    _text = None
 
    def __init__(self):
        pass
 
    def getText(self) : 
        return self._text
 
    def process(self, inputStream):
        self._text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self, data):
        self.data = data

    def process(self, outputStream):
        outputStream.write(self.data.encode('utf-8'))

flowFile = session.get()



def calculateJson(data):

    data = eval(data.replace("'",""))

    for district in data:
        district['Pilgrim'] = district.pop('Registration')

        if 'next_obj_id' in district:
            del district['next_obj_id']
        
        pilgrims = int(str(district['Pilgrim']).replace(",",""))
        bio_pilgrims = int(str(district['Bio-Metric']).replace(",",""))
        bio_percent = float(bio_pilgrims) / pilgrims * 100
        passport_pilgrims = int(str(district['Passport Received']).replace(",",""))
        passport_percent = float(passport_pilgrims) / pilgrims * 100

        district['Pilgrim'] = pilgrims
        district['Bio-Metric'] = bio_pilgrims
        district['Passport Received'] = passport_pilgrims
        district['Bio-Metric %'] = str(round(bio_percent, 0))+"%"
        district['Passport Received %'] = str(round(passport_percent, 0))+"%"

    total_pligrims = 0
    total_bio_matrics = 0
    total_passport_received = 0

    for district in data:
        total_pligrims += int(str(district['Pilgrim']).replace(",", ""))
        total_bio_matrics += int(str(district['Bio-Metric']).replace(",", ""))
        total_passport_received += int(str(district['Passport Received']).replace(",", ""))

    total_bio_percentage = round(float(total_bio_matrics) / total_pligrims * 100, 0)
    total_passport_percentage = round(float(total_passport_received) / total_pligrims * 100, 0)

    total_value = {
        'District': 'TOTAL', 
        'Bio-Metric': total_bio_matrics, 
        'Passport Received': total_passport_received, 
        'Pilgrim': total_pligrims, 
        'Bio-Metric %': str(total_bio_percentage)+"%", 
        'Passport Received %': str(total_passport_percentage)+"%"
    }

    data.append(total_value)
    output = json.dumps(data, sort_keys= False)

    key_order = ["District", "Pilgrim", "Bio-Metric", "Passport Received", "Bio-Metric %", "Passport Received %"]

    for i in range(len(data)):
        data[i] = {k: data[i][k] for k in key_order}

    csv_str = StringWriter()
    writer = csv.DictWriter(csv_str, fieldnames=key_order)
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    # Get the CSV data as a string from the StringWriter object
    csv_data = csv_str.toString()

    return csv_data


if flowFile is not None :
    reader = PyInputStreamCallback()
    session.read(flowFile, reader)
    hmis_data = reader.getText()
    output = calculateJson(hmis_data)

    write_cb = PyOutputStreamCallback(output)

    flowFile = session.write(flowFile, write_cb)
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
