import socket

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
		print('Received:', data.decode())

finally:
	client_socket.close()