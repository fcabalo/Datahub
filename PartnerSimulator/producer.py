import sys
import xml.etree.ElementTree as ET
import os
import random
import string
import requests

def generate_message(template, index):
	partners = [1, 2]
	formats = ['A','B','C']
	rand_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
	
	root = ET.fromstring(template)
	
	replacements = {
		"partnerId": str(random.choice(partners)),
		"formatType": str(random.choice(formats)),
		"body": str(index) + rand_string
	}
	
	for tag, newValue in replacements.items():
		for elem in root.iter(tag):
			elem.text = str(newValue)
			
	return ET.tostring(root,encoding='unicode')
	
def send_message(xmlMessage):
	headers = {'Content-Type': 'application/xml'}
	url = 'http://localhost:8080/datahub/'
	
	try:
		#print('Sending:' , xmlMessage)
		
		response = requests.post(url=url, data=xmlMessage, headers=headers)
		
		if response.status_code == 200:
			posts = response.text
			return posts
		else:
			print('Error: ', response.status_code)
			return None
	except requests.exceptions.RequestException as e:
		print('Error:', e)
		return None
		
def open_template(input_file):
	try:
		with open(input_file, 'r') as file:
			return file.read()
	except FileNotFoundError:
		print("Error: The file was not found.")
	except PermissionError:
		print("Error: Permission denied while accessing the file.")
	except Exception as e:
		print(f"An unexpected error occurred: {e}")
		
def main(messageCount, xmlSource):
	
	#print('Count: ', messageCount)
	#print('Template Source:', xmlSource)
	
	rawTemplate = open_template(xmlSource)
	
	#print(rawTemplate)
	
	#print(message)
	
	for x in range(int(messageCount)):
		message = generate_message(rawTemplate, x)
		posts = send_message(message)
		print('Sent:', posts)
	
	
if __name__=='__main__':
	
	try:
		messageCount = sys.argv[1]
	except IndexError:
		messageCount = 1
		
	try:
		messageTemplate = sys.argv[2]
	except IndexError:
		messageTemplate = 'templates/default.xml'	
	
	main(messageCount, messageTemplate)

