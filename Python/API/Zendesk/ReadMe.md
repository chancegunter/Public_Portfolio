## Public_Portfolio/Python/API/Zendesk_Api_Imagebank.py
#### Description
This file is part of the Quality Assurance and the Zendesk helpdesk. This script is responsible for fetching ticket information in regard to consumer orders. The ticket is then scrapped to download images that can be used in 

#### Usage
To use this file, you need to:
Ensure you have the necessary dependencies installed (see below).
Run the script in a Python environment.
Have Zendesk Api Credentials, up to date Chrome Driver and Local Transactional Database

#### Dependencies
This file depends on the following libraries:
Pandas
Numpy
Requests
MySql.Connector
Selenium
Keyring
PIL
io

#### Known Issues
The script assumes a local path for file storage which may need to be modified based on your environment.
The script does not handle API session timeouts which could result in errors when fetching data.
The logic for file handling could be optimized.

#### Future Improvements
Implement error handling for API calls and file operations.
Add configuration options for the local path and other parameters.
Optimize the file handling logic to avoid errors in image download.
Include logging to track the script’s execution and errors.
