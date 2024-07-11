import logging
import logging.handlers

### Note: cf. ./Notes/syslog_systemd-journald

# Configure logging to syslog
handler = logging.handlers.SysLogHandler(address='/dev/log')  # Adjust the address as per your syslog configuration
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Create a logger and set the logging level
logger = logging.getLogger()
logger.addHandler(handler)
