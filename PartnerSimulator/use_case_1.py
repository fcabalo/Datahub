import sys
import xml.etree.ElementTree as ET
import requests
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

def load_template(template_path):
    """Load XML template from file."""
    try:
        with open(template_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        log.error("Template file not found: %s", template_path)
        sys.exit(1)
    except Exception as e:
        log.error("Error loading template: %s", e)
        sys.exit(1)

def generate_message(template, source, messageType, destination, index):
    """
    Generate XML message using template with specified routing parameters.

    Args:
        template: XML template string
        source: Partner interface ID (3 or 6)
        messageType: Message type (A, B, or C)
        destination: Target partner ID or None
        index: Message sequence number
    """
    timestamp = datetime.now().timestamp()

    root = ET.fromstring(template)
    header = root.find('header')

    # Update header values
    for elem in header.iter('source'):
        elem.text = str(source)

    for elem in header.iter('messageType'):
        elem.text = messageType

    # Handle destination - add or remove based on whether it's provided
    dest_elem = header.find('destination')
    if destination:
        if dest_elem is not None:
            dest_elem.text = str(destination)
        else:
            # Add destination element if it doesn't exist
            dest_elem = ET.Element('destination')
            dest_elem.text = str(destination)
            header.insert(1, dest_elem)
    else:
        if dest_elem is not None:
            header.remove(dest_elem)

    # Update body with index and timestamp for latency tracking
    for elem in root.iter('body'):
        elem.text = f"{index}|{timestamp}"

    return ET.tostring(root, encoding='unicode')

def send_message(xml_message):
    """Send message to datahub ingestion endpoint."""
    headers = {'Content-Type': 'application/xml'}
    url = 'http://localhost:8080/datahub/'

    try:
        response = requests.post(url=url, data=xml_message, headers=headers)

        if response.status_code == 200:
            return True
        else:
            log.error("Failed to send message: status_code=%d", response.status_code)
            return False
    except requests.exceptions.RequestException as e:
        log.error("Request failed: %s", e)
        return False

def main(message_count, template_path):
    """
    Use Case 1: Few messages to many partners (broadcast scenario).

    Sends messages with different routing characteristics to test fan-out behavior.
    Messages are routed to multiple partners based on messageType and region.
    """
    log.info("Starting broadcast test: message_count=%d", message_count)

    template = load_template(template_path)

    # Define test message patterns that trigger different routing rules
    # Each pattern specifies: (source, messageType, destination, description)
    patterns = [
        (3, "A", "2", "Type A with dest=2, source=3 (Hesse) -> Partner 2 (PI4) + Partner 3 (PI7)"),
        (3, "B", "2", "Type B with dest=2, source=3 (Hesse) -> Partner 2 (PI5) + Partner 3 (PI7)"),
        (3, "C", "2", "Type C with dest=2, source=3 (Hesse) -> Partner 2 (PI5) + Partner 3 (PI7)"),
        (3, "A", None, "Type A no dest, source=3 (Hesse) -> Partner 3 (PI7)"),
        (6, "A", "2", "Type A with dest=2, source=6 (Berlin) -> Partner 2 (PI4)"),
    ]

    sent_count = 0
    failed_count = 0

    # Cycle through patterns to generate the requested number of messages
    for i in range(message_count):
        pattern = patterns[i % len(patterns)]
        source, msg_type, destination, description = pattern

        message = generate_message(template, source, msg_type, destination, i)

        log.info("Sending message %d/%d: %s", i + 1, message_count, description)

        if send_message(message):
            sent_count += 1
        else:
            failed_count += 1

    log.info("Broadcast test completed: sent=%d, failed=%d", sent_count, failed_count)
    log.info("Success rate: %.2f%%", (sent_count / message_count * 100) if message_count > 0 else 0)

if __name__ == '__main__':
    try:
        message_count = int(sys.argv[1])
    except (IndexError, ValueError):
        log.error("Usage: python use_case_1.py <message_count>")
        log.error("Example: python use_case_1.py 10")
        sys.exit(1)

    if message_count <= 0:
        log.error("Message count must be greater than 0")
        sys.exit(1)

    template_path = 'templates/default.xml'
    main(message_count, template_path)
