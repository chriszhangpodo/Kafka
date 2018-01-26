from SocketServer import TCPServer, ThreadingMixIn, StreamRequestHandler
import ssl
from time import  ctime 
from kafka import KafkaProducer  

# STEP to build socket server:
# 1 create handle class for handling the received request
# 2 create a server class (TCP,UDP...)
# 3 use server class to run the serve_forever() to process the request
# 4 if multiple thread needed, use ThreadingMaxIn

class Handler(StreamRequestHandler):                             # step 1 create handle class for handling the received request
    def handle(self):
			print 'connect from:', self.client_address 
			while True:                                             # request processing, most of the data send from client side will be processed here
					text = self.request.recv(1024)
					self.request.sendall('[%s] %s' % (ctime(),text))     # return the time-stamp and massage to client to indicate massage had been successfuly sent
					topic,massage= text.split()
					producer.send(topic,value=massage.encode())          # kafka producer send data to kafka broker
					producer.flush()
					print(text)
					
					
class TCP_SSL_SERVER(TCPServer):                # step 2 as SSL needed so rewrite the TCP server class with SSL 
    def __init__(self, server_address, RequestHandlerClass, certfile, keyfile, ssl_version=ssl.PROTOCOL_TLSv1, bind_and_activate=True):
			TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
			self.certfile = certfile                # give cert file variables to self and using in request rewrite
			self.keyfile = keyfile
			self.ssl_version = ssl_version

    def get_request(self):                       # Rewrite the request method by using ssl.wrap_socket
			socket, addr = self.socket.accept()
			connection = ssl.wrap_socket(socket, server_side=True, certfile = self.certfile, keyfile = self.keyfile, ssl_version = self.ssl_version)
			return connection, addr

 
class SSL_ThreadingTCPServer(ThreadingMixIn, TCP_SSL_SERVER): pass         # step 4 if multiple thread needed, use ThreadingMaxIn
 
producer = KafkaProducer(bootstrap_servers="192.168.31.135:9092",retries=3,acks='all',max_block_ms=100000)      # create Kafka producer

print 'waiting for connection...'

SSL_ThreadingTCPServer(('192.168.31.135',9000),Handler,"/home/cz/work/python/mycertfile.pem","/home/cz/work/python/mykeyfile.pem").serve_forever()
# step 3 run multiple thread class with a ThreadingMixIn with server class