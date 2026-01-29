import socket
import sys
import logging
from datetime import datetime
import xml.etree.ElementTree as ET

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)					

def calculateElapsed(xmlData):
	log.debug("Calculating elapsed time from message")
	root = ET.fromstring(xmlData)
	body = root.find('body').text
	data = body.split('|')
	
	index = data[0]
	startTime = float(data[1])
	
	end = datetime.now()
	elapsedtime = end.timestamp() - startTime
	
	log.info("Message %s elapsed: %.4f seconds", index, elapsedtime)
	
def main(messageCount, partnerId, port):

	client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('localhost', port)

	client_socket.connect(server_address)
	
	message = 'PARTNER_ID=' + partnerId + '\r\n'

	log.info("Starting TCP client: partnerId=%s, messageCount=%d", partnerId, messageCount)
	log.info("Connecting to server at %s:%d", server_address[0], server_address[1])
	
	try:
		
		client_socket.sendall(message.encode('utf-8'))
		log.info("%s sent", message.strip())
	
		#client_socket.settimeout(5)
		data = client_socket.recv(1024)
		receivedData = data.decode()
		log.info("Received: %s", receivedData.strip())
		
		counter = messageCount
		first = True
		
		start = datetime.now()
		end = datetime.now()
		
		while messageCount == 0 or counter > 0:
			#Added message length in the data sent to read data dynamically 
			length = client_socket.recv(4, socket.MSG_PEEK)
			#data + 4 for length header + 2 for terminal \r\n 
			data = client_socket.recv(int(length) + 6)
			
			if first:
				start = datetime.now()
				first = False
			
			if data:
				receivedData = data.decode()[4:]
				log.info("Received: %s", receivedData)
				calculateElapsed(receivedData)
				
			counter -= 1
			end = datetime.now()
	
	except KeyboardInterrupt:
		log.info("Interrupt")

	finally:
		client_socket.close()
		log.info("Start: %s", start)
		log.info("End: %s", end)
		log.info("Messages per second: %s", messageCount / (end.timestamp() - start.timestamp()))
		
if __name__=='__main__':
	
	try:
		partnerId = sys.argv[1]
	except IndexError:
		sys.exit("PartnerId is mandatory")
	
	try:
		port = sys.argv[2]
	except IndexError:
		port = 9293
	
	try:
		messageCount = sys.argv[3]
	except IndexError:
		messageCount = 0
		
	log.info("Starting client: messageCount=%s, partnerId=%s", messageCount, partnerId)
	
	main(int(messageCount), partnerId, int(port))