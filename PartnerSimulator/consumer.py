import socket
import sys
from datetime import datetime
import xml.etree.ElementTree as ET

def calculateElapsed(xmlData):
	
	root = ET.fromstring(xmlData)
	body = root.find('body').text
	data = body.split('|')
	
	index = data[0]
	startTime = float(data[1])
	
	end = datetime.now()
	elapsedtime = end.timestamp() - startTime
	
	print(f"Elapsed: {elapsedtime: .4f} seconds")
	
def main(messageCount):

	client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('localhost', 9293)

	client_socket.connect(server_address)

	print('connected to', server_address )

	try:
		
		counter = messageCount
		first = True
		
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
				print('Received:', receivedData)
				calculateElapsed(receivedData)
				
			counter -= 1
		
		end = datetime.now()	
		print('Start: ', start)	
		print('End: ', end)
		print('Messages per second: ', messageCount/(end.timestamp() - start.timestamp()))

	finally:
		client_socket.close()
		
if __name__=='__main__':
	
	try:
		messageCount = sys.argv[1]
	except IndexError:
		messageCount = 0
	
	main(int(messageCount))