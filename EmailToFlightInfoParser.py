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
    pregex = r'^P\/([A-Z0-9]{1,3})\/([A-Z0-9]+)\/([A-Z]{2,3})\/(\d{2,4}[A-Z]{3}\d{2,4})\/([MF])\/(\d{2,4}[A-Z]{3}\d{2,4})\/([A-Z]+)'

    uid = str(uuid.uuid4())
    data = re.sub(r'^\s+', '', email, flags=re.MULTILINE)
    data = re.sub(r'=C2=A5','',data,flags=re.MULTILINE)
    data = re.sub(r'=20','',data,flags=re.MULTILINE)
    data = data.split('\n')
    data = [re.sub(r'\s+', ' ', line).strip() for line in data]
    header = data[:2]


    pax_master= {}
    pax_master = {
        "uid":uid,
        "airlines_code": header[1].split(" ")[0] ,
        "source":0,
        "receieved_at":1111,
        "flight_code":str(header[1].split(" ")[0])+str(header[1].split(" ")[1]),
        "flight_date":  header[1].split(" ")[2],
        "docs": ' '.join(header)
        }  
    
    email_gist = '\n'.join(data[2:])

    cleaned_email_gist = email_gist.split('LFTD')
    # return email_gist

    parsed_data = []

    for i in cleaned_email_gist:
        val = i.split('\n')        
        for items in val:
            # print(items)
            if re.match(pregex,items):
                p_info = items

                try:

                    pax_details = {
                        "id": 0,
                        "pax_master_id": uid,
                        "pax_name": p_info.split('/')[7]+"/"+p_info.split('/')[8],
                        "passport_no": p_info.split('/')[2],
                        "ticket_no": i.split("\n")[-1].split(" ")[-6], 
                        "is_archived": 0,
                        "docs": re.sub("\n"," ", i)+"LFTD"
                    }

                except IndexError:

                    pax_details = {
                        "id": 0,
                        "pax_master_id": uid,
                        "pax_name": p_info.split('/')[7]+"/"+p_info.split('/')[8],
                        "passport_no": p_info.split('/')[2],
                        "ticket_no": "Ticket Number Not Found", 
                        "is_archived": 0,
                        "docs": re.sub("\n"," ", i)
                    }




                parsed_data.append(pax_details)
            else:
                pass


    main_data ={
        "pax_master": pax_master,
        "pax_details": parsed_data
    }  

    return json.dumps(main_data)


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

