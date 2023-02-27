import pandas as pd
import argparse
import os

def hash_addresses(df: pd.DataFrame,
                   save_df: bool=True,
                   df_name: str="./output/address_hash_map.csv") -> pd.DataFrame:
    assert "fullAddress" in df.columns, "fullAddress is not in dataframe columns"

    df["addressID"] = abs(df["fullAddress"].map(hash))
    address_map = df[["fullAddress", "addressID"]]
    address_map = address_map.drop_duplicates()

    if save_df and not os.path.exists(df_name):
        address_map.to_csv(df_name)
    
    return df


def run(argv=None):
    # Add arguments with argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_file", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--postcode_filter", type=str, required=False, default="")
    parser.add_argument("--hash_address", type=str, required=False, default=False)

    args = parser.parse_args()


    # Headers aren't given in original csv - adding names from https://www.gov.uk/guidance/about-the-price-paid-data#download-option for clarity
    df = pd.read_csv(args.input_file, names=["transactionID", "price", "transferDate", "postcode", "propertyType", "newBuild", "duration", "PAON", "SAON", "street", "locality",
    "city", "district", "county", "PPDCategory", "recordStatus"])

    # Removing {} from transactionID column - purely for neatness not needed
    df["transactionID"] = df["transactionID"].str.strip("{}")

    # Replacing NaNs with blank strings to make it easier to read
    df = df.fillna("")

    df["fullAddress"] = df["PAON"].astype(str) + " " + df["SAON"].astype(str) + " " + df["street"].astype(str) + " " 
    + df["locality"].astype(str) + " " + df["city"].astype(str) + " " + df["city"].astype(str) + " " + df["district"].astype(str) + " " 
    + df["county"].astype(str)

    if args.postcode_filter is not None:
        # Getting a subset of the data based on postcode
        df = df[df["postcode"].str.startswith(args.postcode_filter)]

    if args.hash_address:
        df = hash_addresses(df)

    transaction_df = df.groupby("addressID")["transactionID"].unique()

    transaction_df = pd.DataFrame({"count": transaction_df}).reset_index()
    transaction_df.columns = ["addressID", "transactions"]

    address_df = df.groupby(by="addressID").agg({"transactionID": "count", "price": ["max", "min", "mean"], "transferDate": "max",
    "newBuild" : "max", "propertyType" : "max", "duration" : "max"})

    address_df.columns = address_df.columns.to_flat_index()

    address_df.columns = ["transactions", "maxPrice", "minPrice", "meanPrice", "mostRecentTransfer", "newBuild", "propertyType", "duration"]

    address_df.reset_index(inplace=True)

    mapping = dict(transaction_df[["addressID", "transactions"]].values)
    address_df["transactionIDs"] = address_df.addressID.map(mapping)

    # Output to newline delimited json file
    address_df.to_json(args.output_path, orient="records", lines=True)


    return None


if __name__ == "__main__":
    run()