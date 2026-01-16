import sys
import xml.etree.ElementTree as ET
import os
import random
import string
import requests
import logging			  
from datetime import datetime

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

log = logging.getLogger(__name__)					
def generate_message(template, index, msgType, recipient):
	partnerInterfaces = {3,6}
	partners = {1,2,3}
	types = {'A','B','C'}
	
	source = str(random.choice(list(partnerInterfaces)))
	messageType = ''
	
	if msgType == '-':
		messageType = str(random.choice(list(types)))
	else:
		messageType = msgType
	
	if recipient == '-':
		destination = None
	elif recipient == '0':
		destination = str(random.choice(list(partners)))
	else:
		destination = recipient
		
	timestamp = datetime.now().timestamp()
	
	log.debug("Generating message: index=%d, messageType=%s, destination=%d", index, messageType, destination)																		
	root = ET.fromstring(template)
	
	header = root.find('header')
	
	headerRep = {
		"source": source,
		"destination": destination,
		"messageType": messageType
	}
	
	replacements = {
		"body": str(index) + '|' + str(timestamp)
	}
	
	for tag, newValue in headerRep.items():
		for elem in header.iter(tag):
			if newValue is None:
				header.remove(elem)
			else:
				elem.text = str(newValue)
			
	for tag, newValue in replacements.items():
		for elem in root.iter(tag):
			elem.text = str(newValue)
			
	log.debug("Generated message: source=%s, messageType=%s", headerRep["source"], headerRep["messageType"])																										 
	return ET.tostring(root,encoding='unicode')
	
def send_message(xmlMessage):
	headers = {'Content-Type': 'application/xml'}
	url = 'http://localhost:8080/datahub/'
	
	log.debug("Sending message to %s", url)									
	try:
		#print('Sending:' , xmlMessage)
		
		response = requests.post(url=url, data=xmlMessage, headers=headers)
		
		if response.status_code == 200:
			posts = response.text
			log.info("Message sent successfully")
			log.debug("Response: %s", posts)							
			return posts
		else:
			log.error("Failed to send message: status_code=%d", response.status_code)
			return None
	except requests.exceptions.RequestException as e:
		log.error("Request failed: %s", e)
		return None
		
def open_template(input_file):
	try:
		with open(input_file, 'r') as file:
			content = file.read()
			log.debug("Template loaded successfully, length=%d", len(content))																			 
			return content
	except FileNotFoundError:
		log.error("Template file not found: %s", input_file)
	except PermissionError:
		log.error("Permission denied accessing template: %s", input_file)
	except Exception as e:
		log.error("Unexpected error loading template: %s", e)
		
def main(messageCount, messageType, destination, xmlSource):
	log.info("Starting message generation: count=%s, messageType=%s, destination=%s, template=%s",
			 messageCount, messageType, destination, xmlSource)								   
	
	#print('Count: ', messageCount)
	#print('Template Source:', xmlSource)
	
	rawTemplate = open_template(xmlSource)
	
	#print(rawTemplate)
	
	
	for x in range(int(messageCount)):
		log.info("Processing message %d of %s", x + 1, messageCount)
		message = generate_message(rawTemplate, x, messageType, destination)
		log.debug("Message content: %s", message)
		posts = send_message(message)
		log.debug('Sent: {}', posts)
	
	
if __name__=='__main__':
	
	try:
		messageCount = sys.argv[1]
	except IndexError:
		messageCount = 1
		
	try:
		messageType = sys.argv[2]
	except IndexError:
		messageType = '-'
		
	try:
		destination = sys.argv[3]
	except IndexError:
		destination = '-'
		
	messageTemplate = 'templates/default.xml'	
	log.info("Application started with args: count=%s, messageType=%s, destination=%s, template=%s",
			 messageCount, messageType, destination, messageTemplate)
	
	main(messageCount, messageType, destination, messageTemplate)

