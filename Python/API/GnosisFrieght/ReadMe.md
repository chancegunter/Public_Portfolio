## Public_Portfolio/Python/API/Gnosis_Api.py
#### Description
This file is part of the Gnosis Frieght Tracking and it is responsible for fetching container information to ensure proper lead times from vendors to consumers.

#### Usage
To use this file, you need to:
Ensure you have the necessary dependencies installed (see below).
Run the script in a Python environment.
Have Gnosis Api Credentials and Local Transactional Database

#### Dependencies
This file depends on the following libraries:
Pandas
Numpy
Requests
MySql.Connector

#### Known Issues
The script assumes a local path for file storage which may need to be modified based on your environment.
The script does not handle API session timeouts which could result in errors when fetching data.
The logic for file handling is could be optimized.

#### Future Improvements
Implement error handling for API calls and file operations.
Add configuration options for the local path and other parameters.
Optimize the file handling logic to avoid errors in column matching.
Include logging to track the script’s execution and errors.
