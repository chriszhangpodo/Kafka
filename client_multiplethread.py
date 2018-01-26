#!/usr/bin/python
import os
import socket, ssl

# pre-define this client, could also write a def client(): 
command = raw_input('press -h for help and exit or enter to continue >>')
# help command and exit
if command == '-h':
		print('usage: -h[help and exit] -k[kafka_host:port] -p[port to connect] -t[topic] -i[interface] -c[another client ip] -f[certfilename] -m[connected event topic] -info [logs to console] -debug[detials on log]')
		os._exit(0)
	
if command <> '-h':
		print("enter for skip and submit")
		HOST = raw_input('-k[kafka_host:192.168.31.135]>')       # could add a regx check 
		if HOST == "":
				HOST = '192.168.31.135'    
		PORT = raw_input('-p[port to connect:9000]>')	    # could put a check for numeric only
		if PORT == "":
				PORT = 9000 
		topic = raw_input('-t[topic]>')                          # could limit the str size
		if topic == "":
				topic = 'test'
		interface = raw_input('-i[interface]>')
		another_client_ip = raw_input('-c[another client ip]>')  # could add ipaddress format check
		certfilename = raw_input('-f[certfilename]>')
		if certfilename == "":                                                       # setting default cert file 
				certfilename = '/home/cz1/work/python/mycertfile.pem'
		event_topic = raw_input('-m[connected event topic]>')
		logs_info = raw_input('-info [logs to console]>')
		debugs_info = raw_input('-debug[detials on log]>')

ADDR = (HOST, PORT)                                         # define the connection address

while True: 
			s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)  # create a socket
			ssl_sock = ssl.wrap_socket(s,
                           ca_certs=certfilename,
                           cert_reqs=ssl.CERT_REQUIRED,
                           ssl_version=ssl.PROTOCOL_TLSv1)    # encrypt socket with SSL
			ssl_sock.connect(ADDR)             # connect to host
			print("server connected")
			data = raw_input('massage (enter to finish)>>') 
			whole_massage = ' '.join([topic,data])
			ssl_sock.send(whole_massage)                          # make sure sending topic & message
			retrunmsg = ssl_sock.recv(1024) 
			print retrunmsg.strip() 
	
