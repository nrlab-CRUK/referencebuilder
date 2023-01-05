import json
import os
import sys
import urllib3

user = 'richard.bowers@cruk.cam.ac.uk'
passw = '!3ug3Cx!mjmc#PTh&Ph^'

url = sys.argv[1]
outFile = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(os.path.normpath(url))

http = urllib3.PoolManager()
headers = urllib3.make_headers(basic_auth = f"{user}:{passw}")

response = http.request('GET', url, headers = headers)
infomap = json.loads(response.data.decode('utf-8'))

try:
    fetchUrl = infomap['url']
    
    response = http.request('GET', fetchUrl)
    if response.status != 200:
        print(f"Failed to download from {fetchUrl}")
        exit(1)

    with open(outFile, 'wb') as fh:
        fh.write(response.data)
        
    sys.exit(0)
except KeyError:
    print(infomap['error'])
    
sys.exit(1)
