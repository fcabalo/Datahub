import socket
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
	
def main():

	client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('localhost', 9293)

	client_socket.connect(server_address)

	print('connected to', server_address )

	try:
		message = 'Hello, Server!\r\n'
		
		print(message)
		#client_socket.sendall(message.encode())
		
		while True:
			data = client_socket.recv(1024)
			receivedData = data.decode()
			
			print('Received:', receivedData)
			calculateElapsed(receivedData)
			

	finally:
		client_socket.close()
		
if __name__=='__main__':
	main()