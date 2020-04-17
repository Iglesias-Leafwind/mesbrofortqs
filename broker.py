import socket
import json
import selectors
import sys
import fcntl
import os
import pickle
import xml.etree.ElementTree as ET
from middleware import MiddlewareType

HOST = 'localhost'                 # Symbolic name meaning all available interfaces
PORT = 8000               # Arbitrary non-privileged port
sel = selectors.DefaultSelector()
Consumer = []			#Consumer List
Producer = []			#Producer List
LastMessage = {}		#Last message of a topic
TopicSubscribed = {}	#Who is sobscribed to a topic
TopicQueue = {}

# set sys.stdin non-blocking
orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

def accept(sock, mask):
    conn, addr = sock.accept()
    print('Accepted: ', conn, ' FROM ', addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, fm)

def read(conn,mask):
    global Consumer
    global Producer
    global TopicSubscribed
    global LastMessage

    #diz que tipo a conn é se é consumer ou producer
    for info in (Consumer + Producer):
        if(info["conn"] == conn):
            userInfo = info
            if(len(info) == 2):
                tipo = f"{MiddlewareType.CONSUMER}"	#é um consumer
            else:
                tipo = f"{MiddlewareType.PRODUCER}"	#é um producer
            break
    
    data = conn.recv(8)
    #verifica se o client desconectou a partir da data == b''
    if(data == b''):
        if(tipo == f"{MiddlewareType.CONSUMER}"):
            Consumer.remove(info)
            #verifica se este consumer esta subscrito a qualquer topico e elemina-o
            for topic in TopicSubscribed:
                if(conn in TopicSubscribed[topic]):
                    TopicSubscribed[topic].remove(conn)
        else:
            Producer.remove(info)
        conn.close()
        sel.unregister(conn)
        return
    
    data = conn.recv(int(data.decode('utf-8')))
    #sabendo que tipo de informação esta encryptada vai traduzir para um dicionario
    if(userInfo['encrypted'] == "JSON"):
           data = data.decode('utf-8')
           data = json.loads(data)

    elif(userInfo['encrypted'] == "XML"):
            data = data.decode('utf-8')
            parser = ET.XMLParser(encoding="utf-8")
            xmltree = ET.fromstring(data, parser=parser)
            data = {}
            for child in xmltree:
                data[child.tag] = child.text
    
    elif(userInfo['encrypted'] == "Pickle"):
            data = pickle.loads(data)
    #aqui vai escolher a opção que o cliente quer fazer a partir da mensagem ja enviada com a opção guardada em data["op"]
    if(data["op"] == "subscribe"):	#expecting data to be {"op":"subscribe","topic":"topic_name"}
        #verifica se o topic ja existe ou n
        if(data["topic"] in TopicSubscribed):
            #verifica se o cliente ja esta subscribed
            if (not (conn in TopicSubscribed[data["topic"]])):
                TopicSubscribed[data["topic"]].append({"conn":conn,"accepting":False})  
        else:
            TopicSubscribed[data["topic"]] = [{"conn":conn,"accepting":False}]
        #verifica se existe lastmessage desse topico e envia ao cliente
        if(data["topic"] in LastMessage):
            conn.send(converter(LastMessage[data["topic"]],data["encrypted"]))

    elif(data["op"] == "unsubscribe"):	#expecting data to be {"op":"unsubscribe","topic":"topic_name"}
        #verifica se ja existe o topico e depois verifica se o cliente esta subscribed a esse topico
        if(data["topic"] in TopicSubscribed and conn in TopicSubscribed[data["topic"]]):
            TopicSubscribed[data["topic"]].remove(conn)

    elif(data["op"] == "list"):			#expecting data to be {"op":"list"}
        print(TopicSubscribed.keys())
        for info in Consumer:
            if(info["conn"] == conn):
                conn.send(converter({"topics":TopicSubscribed.keys()},info["encrypted"]))

    elif(data["op"] == "publish"):		#expecting data to be {"op":"publish","topic":"topic_name","message":"the_great_massage"}
        #verifica se o topico nao existe
        if(not (data["topic"] in TopicSubscribed)):
            TopicSubscribed[data["topic"]] = []

        #adiciona a queue de mensagens a enviar
        if(data["topic"] in TopicQueue):
            TopicQueue[data["topic"]].append(data["message"])
        else:
            TopicQueue[data["topic"]] = [data["message"]]

    elif(data["op"] == "accepting"):
        for topic in TopicSubscribed:
            for user in TopicSubscribed[topic]:
                if(user["conn"] == conn):
                    user["accepting"] = True
                    break
    else:
        print("HOPtion "+data["op"]+" not recognized.")
    #print(Consumer)
    #print(Producer)
    #print(TopicSubscribed)
    #print(LastMessage)

def sendtoconsumers():
    global Consumer
    global TopicQueue
    global TopicSubscribed
    global LastMessage
    for topic in TopicQueue:
        if(TopicQueue[topic] != [] and not(False in [disc["accepting"] for disc in TopicSubscribed[topic]])):
            #vai buscar todos os clientes desse topico
            message = TopicQueue[topic][0]
            TopicQueue[topic].remove(message)
            LastMessage[topic] = message
            for connection in TopicSubscribed[topic]:
                #procura nos consumers cada cliente
                for info in Consumer:
                    #se encontrar o cliente registado nesse topico e como consumer
                    if(info["conn"] == connection["conn"]):
                        #codifica no metodo de encriptação que este cliente usa e envia a mensagem para ele
                        connection["conn"].send(converter({"topic":topic,"data":message},info["encrypted"]))
                        break

#this is the 1st message
def fm(conn, mask):
    global Consumer
    global Producer
    global TopicSubscribed
    global TopicQueue

    data = conn.recv(8)
    data = conn.recv(int(data.decode('utf-8')))
    data = data.decode('utf-8')
    data = data.split("|")
    
    if(data[0] == f"{MiddlewareType.CONSUMER}"):		#Consumer = 1
        Consumer = Consumer + [{"conn":conn,"encrypted":data[2]}]
        if(data[1] in TopicSubscribed):
            TopicSubscribed[data[1]].append({"conn":conn,"accepting":False})
        else:
            TopicSubscribed[data[1]] = [{"conn":conn,"accepting":False}]
        if(data[1] in LastMessage and TopicQueue[data[1]] == []):
            conn.send(converter({"topic":data[1],"data":LastMessage[data[1]]},data[2]))
    else:					#Producer = 2
        Producer = Producer + [{"conn":conn,"topic":data[1],"encrypted":data[2]}]
    
    sel.unregister(conn)
    sel.register(conn, selectors.EVENT_READ, read)

def close(stdin, mask):
    global Consumer
    global Producer
    data = stdin.read()
    
    if(Consumer == [] and Producer == []):
        sock.close()
        sel.close()
        sys.exit()
    else:
        print('There are still clients connected!')

def converter(msg, to):
    codedsize = (format(sys.getsizeof(msg), '08d')).encode('utf-8')
    if(to == "JSON"):
            return codedsize + (json.dumps(msg)).encode('utf-8')
    elif(to == "XML"):
    	return codedsize + (f'<?xml version="1.0" encoding="UTF-8"?><data><topic>{msg["topic"]}</topic><data>{msg["data"]}</data></data>').encode('utf-8')
    elif(to == "Pickle"):
        return codedsize + pickle.dumps(msg)

sock = socket.socket()
sock.bind((HOST, PORT))
sock.listen(100)
sock.setblocking(False)
sel.register(sys.stdin, selectors.EVENT_READ, close)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    sendtoconsumers()
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj,mask)
