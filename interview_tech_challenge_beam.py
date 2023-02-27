import argparse
import json
import apache_beam as beam
from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

class Transaction(beam.DoFn):
    def process(self, element):
        '''Returns a dictionary like object of each of the elements from the input csv'''
        transactionID, price, transferDate, postcode, propertyType, newBuild, duration, PAON, SAON, street, locality, city, district, county, PPDCategory, recordStatus = element.split(",")
        return [{"transactionID": transactionID.strip("{}"),
                 "price": price,
                 "transferDate": transferDate,
                 "postcode": postcode,
                 "propertyType": propertyType,
                 "newBuild": newBuild,
                 "duration": duration,
                 "PAON": PAON,
                 "SAON": SAON,
                 "street": street,
                 "locality": locality,
                 "city": city,
                 "district": district,
                 "county": county,
                 "PPDCategory": PPDCategory,
                 "recordStatus": recordStatus,
                 "fullAddress": f"{PAON} {SAON} {street} {locality} {city} {district} {county} {postcode}"}]
    
class OutputTransform(beam.DoFn):    
    @staticmethod
    def tuple_element_to_list(t: tuple, position: int) -> list:
        return [item[position] for item in t]

    def process(self, element):
        full_address, transactions_data = element[0], element[1]
        transaction_ids = self.tuple_element_to_list(transactions_data, 0)
        dates = self.tuple_element_to_list(transactions_data, 1)
        prices = self.tuple_element_to_list(transactions_data, 2)
        new_build = self.tuple_element_to_list(transactions_data, 3)
        property_type = self.tuple_element_to_list(transactions_data, 4)

        return [{"fullAddress": full_address,
                 "addressID": str(hash(full_address))[1:17],
                 "transactionID": transaction_ids,
                 "numberOfTransactions": len(transaction_ids),
                 "mostRecentTransferDate": max(dates),
                 "maxPrice": max(prices),
                 "minPrice": min(prices),
                 "newBuild": new_build[-1],
                 "propertyType": property_type[-1]}]


def run(argv=None) -> None:
    # Add arguments with argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_file", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--postcode_filter", type=str, required=False, default="")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Separate step to load the data to use the raw data in multiple operations
        data_from_source = (p 
                            | "Read Data" >> ReadFromText(known_args.input_file)
                            | "Removing punctuation from elements" >> beam.Regex.replace_all(r'"([^"]*)"',lambda x:x.group(1).replace(',',''))
                            | "Clean the record" >> beam.ParDo(Transaction())
                            | "Filter Out results" >> beam.Filter(lambda x: str(x["postcode"]).startswith(known_args.postcode_filter))
                            )    
        
        address_transactions = (data_from_source
                             | "Map each address to all data" >> beam.Map(lambda element: (element["fullAddress"], (element["transactionID"], element["transferDate"], 
                                                                                                                    element["price"], element["newBuild"], element["propertyType"])))
                             | "Group the data" >> beam.GroupByKey()   
                             | "Transform into more useable format" >> beam.ParDo(OutputTransform())                                                   
                             )  
        
        output = (address_transactions
                  | "Format Output" >> beam.Map(json.dumps)
                  | "Write output to File" >> WriteToText(known_args.output_path, file_name_suffix=".json") 
                  )

    return None

if __name__ == "__main__":
    run()