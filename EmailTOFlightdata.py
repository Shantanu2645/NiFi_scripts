import re
import json
import uuid
import email
import mimetypes
from email.parser import Parser
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from java.io import BufferedReader, InputStreamReader
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processor.io import OutputStreamCallback



class PyInputStreamCallback(InputStreamCallback):
    _text = None
 
    def __init__(self):
        pass
 
    def getText(self) : 
        return self._text
 
    def process(self, inputStream):
        self._text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
 
flowFile = session.get()


class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self, data):
        self.data = data

    def process(self, outputStream):
        outputStream.write(bytearray(self.data.encode('utf-8')))


def process_data(email):
    pregex = r'(P\/[A-Z]{2,3}\/[A-Z0-9]{9,11}\/[A-Z]{2,3}\/\d{1,2}[A-Z]{3}\d{2,4}\/[MF]\/\d{1,2}[A-Z]{3}\d{2,4}\/[A-Z]+/[A-z]+)'

    uid = str(uuid.uuid4())
    data = re.sub(r'=C2=A5','',email,flags=re.MULTILINE)
    data = re.sub(r'=20','',data,flags=re.MULTILINE)
    data = re.sub(r'=','',data,flags=re.MULTILINE)
    data = re.sub(r'\n','',data,flags=re.MULTILINE)

    matches = re.findall(pregex, data)

    pax_master = {
        "uid":uid,
        "airlines_code": "" ,
        "source": 0,
        "receieved_at":1111,
        "flight_code":"",
        "flight_date":  "",
        "docs": email
        }  

    parsed_data = []
    for i in matches:

        pax_details = {
                "id": 0,
                "pax_master_id": uid,
                "pax_name": i.split('/')[7]+"/"+i.split('/')[8],
                "passport_no": i.split('/')[2],
                "ticket_no": "", 
                "is_archived": 0,
                "docs": ""
            }
        
        parsed_data.append(pax_details)

    full_data = {
        "pax_master" : pax_master,
        "pax_details" : parsed_data

    }

    return(json.dumps(full_data))
        


if flowFile is not None :
    reader = PyInputStreamCallback()
    session.read(flowFile, reader)
    msg = email.message_from_string(reader.getText())
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))

            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=False)  # decode
                output = process_data(body)

                break
    else:
        body = msg.get_payload(decode=True)

    # flowFile = session.putAttribute(flowFile, 'msgbody', output.decode('utf-8', 'ignore')) #
    write_cb = PyOutputStreamCallback(output)
    flowFile = session.write(flowFile, write_cb)
    
       
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)# your code goes here
