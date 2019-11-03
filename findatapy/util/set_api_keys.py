'''

We can set API keys by running this script, these will be read in by dataconstants.py

Alternatively, we can set them manually in dataconstants.py or create a datacred.py file

'''

import os
import keyring

service_names = ['Quandl', 'AlphaVantage', 'Twitter App Key', 'Twitter App Secret', 'Twitter OAUTH token',
                 'Twitter OAUTH token Secret', 'FRED']

for s in service_names:
    key = input("Please enter the %s API key: " % s)

    keyring.set_password(s, os.getlogin(), key)