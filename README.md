## 
#### Description
This file is part of the Crypto Trading Project and it is responsible for fetching cryptocurrency prices, storing them, and executing trading strategies like mean reversion and simple moving average to determine profitable trading opportunities.
#### Usage
To use this file, you need to:
- Ensure you have the necessary dependencies installed (see below).
- Run the script in a Python environment.
- The script will fetch cryptocurrency prices from the Binance TestNet server, execute the trading strategies, and store the results in a JSON file.

#### Dependencies
This file depends on the following libraries:
- numpy
- json
- binance

#### Known Issues
The script does not handle API rate limits which could result in errors when fetching data.

#### Future Improvements
- Implement error handling for API calls and file operations.
- Include logging to track the script’s execution and errors.
