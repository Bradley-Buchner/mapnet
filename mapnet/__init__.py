import logging

logging.basicConfig(format=('%(levelname)s: [%(asctime)s] %(name)s'
                            ' - %(message)s'),
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# Suppress INFO-level logging from some dependencies
# logging.getLogger('requests').setLevel(logging.ERROR)
# logging.getLogger('urllib3').setLevel(logging.ERROR)
# logging.getLogger('rdflib').setLevel(logging.ERROR)
# logging.getLogger('boto3').setLevel(logging.CRITICAL)
# logging.getLogger('botocore').setLevel(logging.CRITICAL)

logger = logging.getLogger('mapnet')
