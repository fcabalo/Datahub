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

def generate_message(template, index, source, messageType, destination):
    """
    Generate XML message using template for load testing.

    Args:
        template: XML template string
        index: Message sequence number for tracking
        source: Partner interface ID (3 or 6)
        messageType: Message type (A, B, or C)
        destination: Target partner ID
    """
    timestamp = datetime.now().timestamp()

    root = ET.fromstring(template)
    header = root.find('header')

    # Update header values
    for elem in header.iter('source'):
        elem.text = str(source)

    for elem in header.iter('destination'):
        elem.text = str(destination)

    for elem in header.iter('messageType'):
        elem.text = messageType

    # Update body with index and timestamp for latency tracking
    for elem in root.iter('body'):
        elem.text = f"{index}|{timestamp}"

    return ET.tostring(root, encoding='unicode')

def send_message(xml_message):
    """Send message to datahub ingestion endpoint."""
    headers = {'Content-Type': 'application/xml'}
    url = 'http://localhost:8080/datahub'

    try:
        response = requests.post(url=url, data=xml_message, headers=headers)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def main(message_count, source, messageType, destination, batch_size, template_path):
    """
    Use Case 2: Many messages to one partner (load test).

    Sends high volume of messages to test throughput and performance.
    Tracks success rate and provides progress updates.

    Usage:
        python3 use_case_2.py <message_count> [source] [messageType] [destination] [batch_size]

    Arguments:
        message_count : Number of messages to send (required)
        source        : Source interface - 3 (Hesse region) or 6 (Berlin region) (default: 6)
        messageType   : Message type - A, B, or C (default: A)
        destination   : Target partner - 1, 2, 3, or 4 (default: 2)
        batch_size    : Progress reporting interval (default: 100)

    Examples:
        python3 use_case_2.py 1000
            Sends 1000 Type A messages from source=6 to Partner 2 (no fan-out)

        python3 use_case_2.py 1000 6 A 2 100
            Sends 1000 Type A messages from source=6 (Berlin) to Partner 2
            Reports progress every 100 messages
            Routes to: PI4Outgoing only (no fan-out to Partner 3)

        python3 use_case_2.py 1000 3 A 2 100
            Sends 1000 Type A messages from source=3 (Hesse) to Partner 2
            Routes to: PI4Outgoing + PI7Outgoing (fan-out to Partner 2 and Partner 3)

        python3 use_case_2.py 5000 6 B 2 500
            Sends 5000 Type B messages from source=6 to Partner 2
            Reports progress every 500 messages
            Routes to: PI5Outgoing only

    Source Parameter Impact:
        source=6 -> region=Berlin  -> Avoids RC3 (Partner 3) -> No fan-out
        source=3 -> region=Hesse   -> Triggers RC3 (Partner 3) -> Fan-out to multiple partners
    """
    log.info("Starting load test")
    log.info("Configuration: messages=%d, source=%d, type=%s, dest=%s, batch=%d",
             message_count, source, messageType, destination, batch_size)

    template = load_template(template_path)

    start_time = datetime.now()
    sent_count = 0
    failed_count = 0

    for i in range(message_count):
        message = generate_message(template, i, source, messageType, destination)

        if send_message(message):
            sent_count += 1
        else:
            failed_count += 1

        # Progress reporting at batch intervals
        if (i + 1) % batch_size == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = sent_count / elapsed if elapsed > 0 else 0
            log.info("Progress: %d/%d messages (sent=%d, failed=%d) - Rate: %.2f msg/sec",
                    i + 1, message_count, sent_count, failed_count, rate)

    end_time = datetime.now()
    total_elapsed = (end_time - start_time).total_seconds()

    log.info("Load test completed")
    log.info("Total messages: %d", message_count)
    log.info("Successfully sent: %d", sent_count)
    log.info("Failed: %d", failed_count)
    log.info("Success rate: %.2f%%", (sent_count / message_count * 100) if message_count > 0 else 0)
    log.info("Total time: %.2f seconds", total_elapsed)
    log.info("Average throughput: %.2f messages/second",
             sent_count / total_elapsed if total_elapsed > 0 else 0)

if __name__ == '__main__':
    # Parse command line arguments
    try:
        message_count = int(sys.argv[1])
    except (IndexError, ValueError):
        log.error("Usage: python use_case_2.py <message_count> [source] [messageType] [destination] [batch_size]")
        log.error("Example: python use_case_2.py 1000 6 A 2 100")
        log.error("")
        log.error("Arguments:")
        log.error("  message_count : Number of messages to send (required)")
        log.error("  source        : Source interface (3 or 6, default: 6)")
        log.error("  messageType   : Message type (A/B/C, default: A)")
        log.error("  destination   : Target partner (1/2/3/4, default: 2)")
        log.error("  batch_size    : Progress reporting interval (default: 100)")
        sys.exit(1)

    if message_count <= 0:
        log.error("Message count must be greater than 0")
        sys.exit(1)

    # Optional parameters with defaults
    source = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    messageType = sys.argv[3] if len(sys.argv) > 3 else "A"
    destination = sys.argv[4] if len(sys.argv) > 4 else "2"
    batch_size = int(sys.argv[5]) if len(sys.argv) > 5 else 100

    # Validate parameters
    if source not in [3, 6]:
        log.error("Source must be 3 or 6")
        sys.exit(1)

    if messageType not in ['A', 'B', 'C']:
        log.error("MessageType must be A, B, or C")
        sys.exit(1)

    if destination not in ['1', '2', '3', '4']:
        log.error("Destination must be 1, 2, 3, or 4")
        sys.exit(1)

    if batch_size <= 0:
        log.error("Batch size must be greater than 0")
        sys.exit(1)

    template_path = 'templates/default.xml'
    main(message_count, source, messageType, destination, batch_size, template_path)
