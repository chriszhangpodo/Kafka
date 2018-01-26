#!/usr/bin/python
import ssl    
from kafka import KafkaProducer    
from socket import *
# presetting client environment     
HOST = '192.168.31.135'
PORT = 8083
BUFSIZE = 12365535
ADDR = (HOST, PORT)
log_fileDir = '/home/cz1/work/python1/server.log'
data_fileDir = '/home/cz1/work/python1/2.txt'  
welcomeStr = 'Welcome to 12.1 python socket server'
tcpCliSock = socket(AF_INET, SOCK_STREAM)
#read file and write in local  
def fileWrite(record, fileName):
         with open(fileName, 'w') as logFile:
            for recordItem in record:
                logFile.write(recordItem)
# directly run kafka producer API without SSL not safe
def kafka(KAFKA_BROKERS,topic,massage):
		producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,retries=3,acks='all',max_block_ms=100000)
		for i in range(10):
				producer.send(topic, key=str.encode('key_{}'.format(i)),value=massage.encode())
		producer.flush()
		print('message send!')
	
# help module
def help():
		print("don't know")
	
# read all latest consumer API sys.out
def consume_kafka(topic):
		print("send topic name %s" % topic)
		consumeDataTmp = tcpCliSock.recv(BUFSIZE)
		print(consumeDataTmp)
	

# main method to build the SSL and sending massage to server
def main():
		ssl_conn = ssl.wrap_socket(tcpCliSock, ca_certs="/home/cz1/work/python/mycertfile.pem", cert_reqs=ssl.CERT_REQUIRED)
		ssl_conn.connect(ADDR)
        print 'Will connect 12.1 python socket server'
		client_command = raw_input(">>")
        data = client_command
		if len(data) > 3:
			print("command valid, press -h for help")
			ssl_conn.close()
		if data == '-e':                                                                         # shutting down server
			print("close server")
			ssl_conn.send(data)
			retmsg = ssl_conn.recv(BUFSIZE)
			print(retmsg)
			ssl_conn.close()
        if data == '-h':                                                                       # run help and exit
			help()
			ssl_conn.close()
		if data == '-k':                                                                      # run kafka producer API without SSL
			new_command = raw_input("broker_addr topic massage (press enter to end):>>")
			KAFKA_BROKERS, topic, massage= new_command.split()
			kafka(KAFKA_BROKERS,topic,massage)
			ssl_conn.close()
		if data == '-c':                                                                       # run consumer API on server side, do this before send massage
			ssl_conn.send(data)
			print("run comsumer in kafka server")
			retDataTmp = ssl_conn.recv(BUFSIZE)
			print(retDataTmp)
			ssl_conn.close()
		if data == '-m':                                                                     # get topic output file and print out
			print("get topic data file")
			new_command1 = raw_input("topic  (no_space)>>")
			ssl_conn.send(data)
			consume_kafka(new_command1)
			ssl_conn.close()
		if data == '-d':                                                                      # get Kafka server log
			print("get kafka server log")
			ssl_conn.sendall(data)
        	retDataTmp = ssl_conn.recv(BUFSIZE)
    		print(retDataTmp)
			ssl_conn.close()
		if data == '-p':                                                                      # send broker info, topic and massage with space to server to run producer API on server side
			ssl_conn.send(data)
			new_command_2 = raw_input("type:broker_addr topic massge (press enter to finish) >>")
			KAFKA_BROKERS, topic, massage= new_command_2.split()
			producer_msg = " ".join([KAFKA_BROKERS,topic,massage])
			ssl_conn.send(producer_msg)
			retmsg = ssl_conn.recv(BUFSIZE)
			if retmsg =='massage produced in kafka':
				print('consumer run & massage produced successfully')
				ssl_conn.close()
			if not retmsg:
				print('producer didn not run')
			ssl_conn.close()
    
if __name__ == '__main__':
			main()
