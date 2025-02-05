import pandas as pd
import re
import requests
import gzip
from io import BytesIO
import ipaddress

urls = [
    "http://www.secrepo.com/self.logs/access.log.2025-01-30.gz",
    "http://www.secrepo.com/self.logs/access.log.2025-01-31.gz",
    "http://www.secrepo.com/self.logs/access.log.2025-02-01.gz",
    "http://www.secrepo.com/self.logs/access.log.2025-02-02.gz"
]

# Regular expression with named capture groups
regex_pattern = r'''(?P<SourceIp>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - (?P<RemoteUser>\S+) \[(?P<TimeLocal>.*?)\] "(?P<Request>.*?)" (?P<Status>\d+) (?P<BodyBytesSent>\d+) "(?P<HttpReferer>.*?)" "(?P<HttpUserAgent>.*?)"'''
compiled_regex = re.compile(regex_pattern)

# Function to extract groups
def extract_groups(text):
    match = compiled_regex.match(text)
    if match:
        return match.groupdict()
    else:
        return {name: None for name in compiled_regex.groupindex}


try:
    content = ""
    for url in urls:
      response = requests.get(url, stream=True)
      response.raise_for_status()  # Raise an exception for bad status codes

      # Decompress the gzip file
      gzip_file = gzip.GzipFile(fileobj=BytesIO(response.content))
      one_content = gzip_file.read().decode('utf-8') # Decode to string
      content += one_content
    
    # Split the content into lines and extract data
    lines = content.splitlines()
    extracted_data = [extract_groups(line) for line in lines]

    # Create the DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Replace '-' with None
    df.replace('-', None, inplace=True)
    df['TimeLocal'] = pd.to_datetime(df['TimeLocal'], format='%d/%b/%Y:%H:%M:%S %z', errors='coerce')
    df.drop('RemoteUser', axis=1, inplace=True)
    df['Status'] = pd.to_numeric(df['Status'], errors='coerce')
    df['BodyBytesSent'] = pd.to_numeric(df['BodyBytesSent'], errors='coerce')
    df['SourceIp'] = df['SourceIp'].apply(ipaddress.ip_address)

    print(df.head())

except requests.exceptions.RequestException as e:
    print(f"Error downloading file: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
