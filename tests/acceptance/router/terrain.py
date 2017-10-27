import logging
 
logging.basicConfig(
		stream=open('/var/log/scalarizr.lettuce.log', 'w'),
		format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
