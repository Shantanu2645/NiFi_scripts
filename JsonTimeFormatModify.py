import re
import json

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
        self._text = IOUtils.toString(inputStream)
 


class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self, data):
        self.data = data

    def process(self, outputStream):
        outputStream.write(bytearray(self.data))


def date_replacer(data):
    pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z" #"2023-03-02T10:37:30.000000Z to 2023-03-02 10:37:30.000000"

    for key in data:
        if isinstance(data[key], str):
            # Search for date-time strings in the value
            matches = re.findall(pattern, data[key])
            for match in matches:
                new_match = match.replace('T', ' ').replace('Z', '')
                data[key] = data[key].replace(match, new_match)

    return json.dumps(data)
#this test
flowFile = session.get()

if flowFile is not None :
    reader = PyInputStreamCallback()
    session.read(flowFile, reader)
    data = reader.getText()
    
    write_cb = PyOutputStreamCallback(date_replacer(data))
    flowFile = session.write(flowFile, write_cb)   
       
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)#
