#coding=utf-8
#!/usr/bin/python
from kafka import KafkaConsumer 
from kafka import KafkaProducer    
from socket import *
from shutil import copyfile
import ssl
import subprocess
 #setting server environment 
HOST = '192.168.31.135'
PORT = 8083
BUFSIZE = 12365535
ADDR = (HOST, PORT)
# setting socket server and SSL parameter
context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
context.load_cert_chain(certfile="/home/cz/work/python/mycertfile.pem", keyfile="/home/cz/work/python/mykeyfile.pem")
   
tcpSerSock = socket(AF_INET, SOCK_STREAM)
tcpSerSock.bind(ADDR)
tcpSerSock.listen(5)

 # call consumer API from backend and record sys.out into file 
def consumer():
		p = subprocess.Popen(['python', '/home/cz/work/python/consumer_test.py'])

# run producer API and send massage
def kafka(KAFKA_BROKERS,topic,massage):
		producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,retries=3,acks='all',max_block_ms=100000)
		for i in range(10):
				producer.send(topic, key=str.encode('key_{}'.format(i)),value=massage.encode())
		producer.flush()
			
# load file and return list
def logFileRead(logFile):
        logFileDealList = []
        with open(logFile, 'r') as logFileContent:
            for line in logFileContent.readlines():
                logFileDealList.append(line)
        return logFileDealList
 # main method if not imported then run directly   
if __name__ == "__main__":
        fileDir = '/home/cz/work/kafka_2.11-1.0.0/logs/server.log'
    	consumer_file = '/home/cz/work/python/xx.txt'
		while True:
				print 'Enter 12.1 python socket server'
				tcpCliSock, addr = tcpSerSock.accept()
				print 'Connected from : ', addr
				ssl_conn = context.wrap_socket(tcpCliSock, server_side=True)
				data = ssl_conn.recv(BUFSIZE)                                                              # listen command from client side and run the method relevantly 
				print data

				if data == '-e':
						text = "shutting down server!"
						ssl_conn.sendall(text)
						ssl_conn.close()
						break
				if data == '-d':  
						logFileContentList = logFileRead(fileDir)
						text = " ".join(str(x) for x in logFileContentList)
						print("readfile finished")
						ssl_conn.sendall(text)
		
				if data == '-c':
						consumer()
						text = "consumer run in backend"
						ssl_conn.sendall(text)
				if data == '-m':
						logFileContentList = logFileRead(consumer_file)
						text = " ".join(str(x) for x in logFileContentList)
						print("readfile finished")
						ssl_conn.sendall(text)

				if data == '-p':
						parameter = ssl_conn.recv(BUFSIZE)
						KAFKA_BROKERS,topic,massage= parameter.split()
						kafka(KAFKA_BROKERS,topic,massage)
						text = "massage produced in kafka"
						ssl_conn.sendall(text)
		tcpSerSock.close()

