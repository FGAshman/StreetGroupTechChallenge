# StreetGroup-TechChallenge
Tech Challenge created for Street Group Application completed 24/02/2023 - 27/02/2023

Input csv using the 2022 Land Registry data from https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

Completed using Python 3.6.6 and Apache Beam 2.38.0

Solution uses apache beam to load in the data, groups by the full address (PAON + SAON + street + locality + city + district + county + postcode), and then generate a newline delimited JSON file containing full address, transaction IDs, number of transactions, most recent transfer date, max/min price the property was sold for, if the address was a new build and the property type. Any other fields missed from the transaction data can be looked up using the transaction ID. Address ID is created using the in-built string hashing creating a random 16 digit ID for each address.

Usage:

```python ./path/to/directory/interview_tech_challenge_beam.py --input_file --output_path --postcode_filter```

input_file and output_path are required arguments and need to be specified, postcode_filter is optional with default value "" if no filter is wanted/needed.

Example usage to generate the outputs given:

```python ./path/to/directory/interview_tech_challenge_beam.py --input_file pp-2022.csv --output_path ./output/beam_output.json --postcode_filter S10```

Have also included a solution that only uses the Pandas library to output a similar newline delimited JSON file which can be used by:

```python ./path/to/directory/interview_tech_challenge_pandas.py --input_file pp-2022.csv --output_path ./output/pandas_output.json --postcode_filter S10 --hash_address True```
