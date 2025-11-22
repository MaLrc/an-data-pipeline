from dotenv import load_dotenv
import os
load_dotenv()

print(os.environ.get('LOGURU_LEVEL'))
print(os.getenv('ENV'))