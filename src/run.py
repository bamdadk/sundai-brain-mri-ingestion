import os
import logging
from datetime import datetime
from etl import bids_creator
import openai
import pandas as pd


# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


class RunETL():

    def __init__(self, input_zip_dir: str, output_dir: str, id: str, session: str):
    
        self.id = id
        self.session = session
        self.output_dir = output_dir
        self.input_zip_dir = input_zip_dir

        self.output_id_directory = os.path.join(self.output_dir, id)
        os.makedirs(self.output_id_directory, exist_ok=True)

        self.output_ses_directory = os.path.join(self.output_id_directory, session)
        os.makedirs(self.output_ses_directory, exist_ok=True)

    def run_bids_creator(self):
        os.makedirs(f"{self.upload_ses_directory}/bids", exist_ok=True) #dummy change
        self.bids_ses = f"{self.upload_ses_directory}/bids"

        bids = bids_creator.DicomToBIDS(self.input_zip_dir, self.output_ses_directory, self.id, self.session)
        self.result_bids = bids.dcm_to_bids()
        return self.result_bids

    def run_bids_validator(self):
        bids_val = bids_creator.DicomToBIDS(self.result_bids, self.output_ses_directory, self.id, self.session)
        self.result_bids_val = bids_val.bids_validator(self.result_bids)
        return self.result_bids_val

    def run_all(self):
        self.run_bids_creator()
        self.run_bids_validator()
        return True
        


def main():
    input_zip = "input-infti-file"
    output_zip = "output-dir"
    id = "patient-id"
    session = "session-id"
    r = RunETL(input_zip, output_zip, id, session)
    res = r.run_all()

if __name__ == "__main__":
    main()