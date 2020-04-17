from enum import Enum

import socket
import json
import xml.etree.ElementTree as ET
import pickle
import sys
class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class Queue:
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        self.topic = topic
        HOST = 'localhost'                 # Symbolic name meaning all available interfaces
        PORT = 8000               # Arbitrary non-privileged port	
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((HOST, PORT))

    def push(self, value):
        self.s.send(value)

    def pull(self):
        num = self.s.recv(8)
        msg = self.s.recv(int(num.decode('utf-8')))
        return msg

class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        reg_msg = f"{type}|{topic}|JSON"
        self.s.sendall((format(sys.getsizeof(reg_msg), '08d') + reg_msg).encode('utf-8'))
    
    def push(self,value):
        jmsg = json.dumps({"op":"publish","topic":self.topic,"message":value})
        encodedmsg = (format(sys.getsizeof(jmsg), '08d') + jmsg).encode("utf-8")
        super().push(encodedmsg)
    
    def pull(self):
        jmsg = json.dumps({"op":"accepting"})
        encodedmsg = (format(sys.getsizeof(jmsg), '08d') + jmsg).encode("utf-8")
        super().push(encodedmsg)

        msg = super().pull()
        msg = json.loads(msg.decode('utf-8'))
        return (msg["topic"], msg["data"])

class XMLQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        reg_msg = f"{type}|{topic}|XML"
        self.s.sendall((format(sys.getsizeof(reg_msg), '08d') + reg_msg).encode('utf-8'))
    
    def push(self,value):
        XMLmsg = f'<?xml version="1.0" encoding="UTF-8"?><data><op>publish</op><topic>{self.topic}</topic><message>{value}</message></data>'
        encodedmsg = (format(sys.getsizeof(XMLmsg), '08d') + XMLmsg).encode("utf-8")
        super().push(encodedmsg)
    
    def pull(self):
        XMLmsg = f'<?xml version="1.0" encoding="UTF-8"?><data><op>accepting</op></data>'
        encodedmsg = (format(sys.getsizeof(XMLmsg), '08d') + XMLmsg).encode("utf-8")
        super().push(encodedmsg)

        msg = super().pull()
        parser = ET.XMLParser(encoding="utf-8")
        xmltree = ET.fromstring(msg.decode('utf-8'), parser=parser)
        data = {}
        for child in xmltree:
            data[child.tag] = child.text
        return (data["topic"], data["data"])

class PickleQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        reg_msg = f"{type}|{topic}|Pickle"
        self.s.sendall((format(sys.getsizeof(reg_msg), '08d') + reg_msg).encode('utf-8'))

    def push(self,value):
        msg = {"op":"publish","topic":self.topic,"message":value}
        enc = format(sys.getsizeof(msg), '08d').encode('utf-8')
        picklemsg = pickle.dumps(msg)
        super().push(enc + picklemsg)

    def pull(self):
        msg = {"op":"accepting"}
        enc = format(sys.getsizeof(msg), '08d').encode('utf-8')
        picklemsg = pickle.dumps(msg)
        super().push(enc + picklemsg)

        msg = super().pull()
        msg = pickle.loads(msg)
        return (msg["topic"], msg["data"])
