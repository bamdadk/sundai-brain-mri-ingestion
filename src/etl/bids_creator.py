import os
import json
import shutil
import pydicom
import logging
import subprocess
import pandas as pd
from datetime import datetime
from collections import OrderedDict
from config import BIDS_DICT_DIR
from config import BASE_DIR
import openai

# Setup logging and timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d,%H:%M:%S', format="%(asctime)s | %(levelname)s | %(message)s")

class DicomToBIDS:
   
    def __init__(self, input_dir: str, output_dir: str, upload_dir: str, id:str, session:str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.upload_dir = upload_dir
        self.id = id
        self.session = session
        self.bids_dict = pd.read_csv(BIDS_DICT_DIR)

    def take_extension(self, filepath: str) -> str:
        """Returns file extension, defaulting to '.dcm' if missing."""
        filedir, ext = os.path.splitext(filepath)
        if ext == ".gz":
            return filedir[:-4], ".nii.gz"
        else:
            return filedir, ext

    def get_bids_name_hardcoded(self, series_description: str):
        """
        Looks up the BIDS-style name corresponding to a DICOM SeriesDescription.
        If not found, returns the original description.
        """
        match = self.bids_dict[self.bids_dict['description'] == series_description]
        if not match.empty:
            row = match.iloc[0]
            bids_name = row.get('bids_name')
            data_type = row.get('data_type')
            task = row.get('task-<label>')

            if pd.notna(task):
                return bids_name, data_type, task
            return bids_name, data_type, None

        return series_description
    
    def get_bids_name(self, series_description: str):
        openai.api_key = "FILL_ME_IN" #os.getenv("OPENAI_API_KEY")
        # Read the CSV file
        csv_path = BIDS_DICT_DIR# Adjust the path as necessary
        df = pd.read_csv(csv_path)
        # Convert DataFrame to a string representation
        csv_content = df.to_string()
        prompt = f"""Given the DICOM SeriesDescription: {series_description}', 
        determine the BIDS-style name, data type, and task (if applicable). You should format the output as 3 comma separated values.
        Use the following CSV data for reference (you may not get an exact match, 
        instead use the following to understand the patterns):{csv_content}
        
        Return the result in the format: bids_name, data_type, task."""
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100
        )
        result = response.choices[0].message.content.strip().split(',')
        print(f""">>>> OpenAI call with input: {series_description} 
              resulted in: {result}""")
        
        if len(result) >= 3:
            return result[0].strip(), result[1].strip(), result[2].strip()
        else:
            return series_description, None, None


    def add_taskname_to_json(self, json_path: str, task: str):
        try:
            # Load the existing JSON file
            with open(json_path, 'r') as f:
                data = json.load(f)

            # Create a new OrderedDict with "TaskName" first
            new_data = OrderedDict()
            new_data["TaskName"] = task
            for key, value in data.items():
                if key != "TaskName":  # Avoid duplicate if it already existed
                    new_data[key] = value

            # Save the updated JSON file
            with open(json_path, 'w') as f:
                json.dump(new_data, f, indent=4)

            print(f'TaskName "{task}" added at the top of {json_path}')

        except FileNotFoundError:
            print(f"File not found: {json_path}")
        except json.JSONDecodeError:
            print(f"Invalid JSON in file: {json_path}")
        except Exception as e:
            print(f"Error: {e}")


    def ds_descriptor(self, ds_desc_path):

        # Define the contents of the dataset_description.json
        dataset_description = {
            "Name": "-",
            "BIDSVersion": "1.8.0",
            "Authors": ["-"],
        }

        # Define the output file path
        desc_path = ds_desc_path

        # Write the dictionary to a JSON file
        with open(desc_path, 'w') as f:
            json.dump(dataset_description, f, indent=4)

        #return logging("Dataset description created.")
        # print(f"Created {output_path}")

    def dcm_to_nifti(self):
        """This function creates the Nifti from the input dcm fiels."""
        logging.info("========== Nifti Conversion Started ==========")

        # Nifiti directory creator
        self.output_nifti = f"extra_staging_nifti_{timestamp}"
        self.full_output_nifti = os.path.join(self.output_dir, self.output_nifti)
        os.makedirs(self.full_output_nifti, exist_ok=True)
        for dcm_folder in os.listdir(self.input_dir):
            dcm_folder_dir = os.path.join(self.input_dir, dcm_folder)
            # Use dcm2niix system binary instead of Dcm2Nii
            command = [
                "dcm2niix",
                "-o", self.full_output_nifti,
                dcm_folder_dir
            ]
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode != 0:
                logging.error(f"dcm2niix failed for {dcm_folder_dir}: {result.stderr}")
            else:
                logging.info(f"dcm2niix succeeded for {dcm_folder_dir}: {result.stdout}")
        logging.info(f"DICOM2NIFTI CONVERSION completed. Output stored at: {self.full_output_nifti}")
        logging.info("==================================================")
        return self.full_output_nifti
    
    def fmap_type_extraction(self, nifti_file_dir):
        with open(f"{nifti_file_dir}.json", "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            data = [data]  # Wrap single entry in list

        for row in data:
            image_type = row.get("ImageType", [])
            eco_number = row.get("EchoNumber", None)

            if isinstance(image_type, list) and image_type:
                last_value = image_type[-1].lower()

                if last_value == "phase":
                    return "phasediff"
                elif eco_number == 1:
                    return "magnitude1"
                elif eco_number == 2:
                    return "magnitude2"

    def fmap_bids(self, nifti_dir):
        file_dir, ext = self.take_extension(nifti_dir)
        suffix = self.fmap_type_extraction(file_dir)
        fmap_nifti_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{suffix}{ext}"
        fmap_json_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{suffix}.json"
        if os.path.exists(fmap_nifti_name):
            logging.error(f"Naming error with 'fmap' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(fmap_json_name):
            logging.error(f"Naming error with 'fmap' for ID: {self.id}_{self.session}.")
            return False
        else:
            os.rename(nifti_dir, fmap_nifti_name)
            os.rename(f"{file_dir}.json", fmap_json_name)
            return True

    def dwi_bids(self, nifti_dir, bids_name):
        file_dir, ext = self.take_extension(nifti_dir)
        dwi_nifti_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}{ext}"
        dwi_json_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}.json"
        dwi_bval_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}.bval"
        dwi_bvec_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}.bvec"
        if os.path.exists(dwi_nifti_name):
            logging.error(f"Naming error with 'dwi' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(dwi_json_name):
            logging.error(f"Naming error with 'dwi' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(dwi_bval_name):
            logging.error(f"Naming error with 'dwi' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(dwi_bvec_name):
            logging.error(f"Naming error with 'dwi' for ID: {self.id}_{self.session}.")
            return False
        else:
            os.rename(nifti_dir, dwi_nifti_name)
            os.rename(f"{file_dir}.json", dwi_json_name)
            os.rename(f"{file_dir}.bval", dwi_bval_name)
            os.rename(f"{file_dir}.bvec", dwi_bvec_name)
            return True

    def func_bids(self, nifti_dir, bids_name, task):
        file_dir, ext = self.take_extension(nifti_dir)
        self.add_taskname_to_json(f"{file_dir}.json", task)
        func_nifti_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}{ext}"
        func_json_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}.json"
        if os.path.exists(func_nifti_name):
            logging.error(f"Naming error with 'func' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(func_json_name):
            logging.error(f"Naming error with 'func' for ID: {self.id}_{self.session}.")
            return False
        else:
            os.rename(nifti_dir, func_nifti_name)
            os.rename(f"{file_dir}.json", func_json_name)
            return True

    
    def anat_bids(self, nifti_dir, bids_name):
        file_dir, ext = self.take_extension(nifti_dir)
        anat_nifti_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}{ext}"
        anat_json_name = f"{self.data_type_dir}/sub-{self.id}_ses-{self.session}_{bids_name}.json"
        if os.path.exists(anat_nifti_name):
            logging.error(f"Naming error with 'func' for ID: {self.id}_{self.session}.")
            return False
        elif os.path.exists(anat_json_name):
            logging.error(f"Naming error with 'func' for ID: {self.id}_{self.session}.")
            return False
        else:
            os.rename(nifti_dir, anat_nifti_name)
            os.rename(f"{file_dir}.json", anat_json_name)
            return True

    def dcm_to_bids(self):
        '''
        This method convert the dicom files to bids.
        '''
        logging.info("========== BIDS Conversion Started ==========")
        flag = False
        self.dir_stage_6 = os.path.join(self.output_dir, f"stage6_bids_{timestamp}/bids/sub-{self.id}/ses-{self.session}")
        # stage 6 creator
        os.makedirs(self.dir_stage_6, exist_ok=True)
        # derivatives directory creator
        os.makedirs(f"{self.output_dir}/stage6_bids_{timestamp}/bids/derivatives", exist_ok=True)
        # dataset_description json file creator
        self.ds_descriptor(f"{self.output_dir}/stage6_bids_{timestamp}/bids/dataset_description.json")

        for dcm_folder in os.listdir(self.input_dir):
            dcm_folder_dir = os.path.join(self.input_dir, dcm_folder)

            # bids_name, data_type, task = self.get_bids_name_hardcoded(dcm_folder)
            bids_name, data_type, task = self.get_bids_name(dcm_folder)

            if bids_name != dcm_folder:
                self.data_type_dir = f"{self.dir_stage_6}/{data_type}"
                os.makedirs(self.data_type_dir, exist_ok=True)
                # Use dcm2niix system binary instead of Dcm2Nii
                command = [
                    "dcm2niix",
                    "-o", self.data_type_dir,
                    dcm_folder_dir
                ]
                result = subprocess.run(command, capture_output=True, text=True)
                if result.returncode != 0:
                    logging.error(f"dcm2niix failed for {dcm_folder_dir}: {result.stderr}")
                    converter_log = []
                else:
                    logging.info(f"dcm2niix succeeded for {dcm_folder_dir}: {result.stdout}")
                    # List all NIfTI files generated in the output directory
                    converter_log = [os.path.join(self.data_type_dir, f) for f in os.listdir(self.data_type_dir) if f.endswith('.nii') or f.endswith('.nii.gz')]
                
                if data_type == 'fmap':
                    for dir in converter_log:
                        rename = self.fmap_bids(f"{dir}")
                        if rename == False:
                            flag = True
                            logging.error("check the following path: \n{}")

                elif data_type == 'anat':
                    for dir in converter_log:
                        rename = self.anat_bids(dir, bids_name)
                        if rename == False:
                            flag = True
                            logging.error("check the following path: \n{}")

                elif data_type == 'dwi':
                    for dir in converter_log:
                        rename = self.dwi_bids(dir, bids_name)
                        if rename == False:
                            flag = True
                            logging.error("check the following path: \n{}")


                elif data_type == 'func':
                    for dir in converter_log:
                        rename = self.func_bids(dir, bids_name, task)
                        if rename == False:
                            flag = True
                            logging.error("check the following path: \n{}")

        if flag == True:
            shutil.copytree(f"{self.output_dir}/stage6_bids_{timestamp}/bids", f"{BASE_DIR}/prearchive/{self.id}/{self.session}/bids")

        logging.info(f"DICOM2NIFTI CONVERSION completed. Output stored at: {self.dir_stage_6}")
        logging.info("==================================================")
        return os.path.join(self.output_dir, f"stage6_bids_{timestamp}/bids")


    def bids_validator(self, bids_path):
        command = [
            'docker', 'run', '--rm', '-v',
            f'{bids_path}:/bids:ro',
            'bids/validator', '/bids'
        ]
        result = subprocess.run(command, capture_output=True, text=True)

        self.dir_stage_8 = os.path.join(self.output_dir, f"stage8_bids-validator_{timestamp}/bids")
        os.makedirs(self.dir_stage_8)

        if result.returncode == 0:
            shutil.copytree(bids_path, self.upload_dir, dirs_exist_ok=True )
            logging.info("✅ BIDS dataset is valid!")
            shutil.copytree(f"{bids_path}", f"{self.dir_stage_8}", dirs_exist_ok=True)
            return self.dir_stage_8
        else:
            logging.error(f"❌ BIDS dataset has issues: {result.stderr}")
            shutil.copytree(f"{self.output_dir}/stage8_bids-validator_{timestamp}/bids", f"{BASE_DIR}/prearchive/{self.id}/{self.session}/bids", dirs_exist_ok=True)
            return False