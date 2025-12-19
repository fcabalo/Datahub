import requests
import xml.etree.ElementTree as ET
import random
import string

def send_messages(message):
	
	partners = [1, 2]
	formats = ['A','B','C']
	rand_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
	headers = {'Content-Type': 'application/xml'}
	xml_data = """<Message>
	<partnerId>""" + str(random.choice(partners)) + """</partnerId>
	<formatType>""" + random.choice(formats) + """</formatType>
	<body>""" + message + rand_string + """</body>
	</Message>"""
	url = 'http://localhost:8080/datahub/'
	
	try:
		print('Sending:' , xml_data)
		
		response = requests.post(url=url, data=xml_data, headers=headers)
		
		if response.status_code == 200:
			posts = response.text
			return posts
		else:
			print('Error: ', response.status_code)
			return None
	except requests.exceptions.RequestException as e:
		print('Error:', e)
		return None
		
def main():
	
	for x in range(1):
		message = 'message: ' + str(x)
		posts = send_messages(message)
		print(posts)
	
if __name__=='__main__':
	main()