import os
import re
import cv2
import time
import json
import shutil
import requests
import numpy as np
import pandas as pd
from metaflow import FlowSpec, step
from gcp_functions import upload_files, delete_files, download_files


class SingleTreeFlow(FlowSpec):
    @step
    def start(self):
        print("initiating tasks...")

        directory1 = "./data/tmp_raw"
        directory2 = './data/tmp_preprocessed_img/'
        directory3 = './data/tmp_processed_img/'
        directory4 = './data/tmp_processed_csv/'
        
        if not os.path.exists(directory1):
            os.makedirs(directory1)
        
        if not os.path.exists(directory2):
            os.makedirs(directory2)

        if not os.path.exists(directory3):
            os.makedirs(directory3)

        if not os.path.exists(directory4):
            os.makedirs(directory4)

        self.next(self.pull_img_from_gcs_to_tmp_raw)

    #@step
    #def pull_img_from_gdrive_to_gcs():

    @step
    def pull_img_from_gcs_to_tmp_raw(self):
        directory = "./data/tmp_raw"
        
        download_files("invoice-text-extraction", directory, "raw_img")
        self.next(self.img_preprocess_)

    @step
    def img_preprocess_(self):
        num = 0
        input_path = './data/tmp_raw/'
        output_path = './data/tmp_preprocessed_img/'

        if not os.path.exists(input_path):
            os.makedirs(input_path)

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        def image_processor(image):
            img = cv2.imread(image)
            blurred = cv2.blur(img, (3,3))
            canny = cv2.Canny(blurred, 50, 200)

            # find the non-zero min-max coords of canny
            pts = np.argwhere(canny>0)
            y1, x1 = pts.min(axis=0)
            y2, x2 = pts.max(axis=0)

            # crop the region
            cropped = img[y1:y2, x1:x2]
            
            # Apply dilation and erosion to remove some noise
            kernel = np.ones((1, 1), np.uint8)
            img = cv2.dilate(cropped, kernel, iterations=1)
            img = cv2.erode(img, kernel, iterations=1)
            return img

        for filename in os.listdir(input_path):
            if any([filename.endswith(x) for x in [".png", "jpg"]]):
                try:
                    img = image_processor(os.path.join(input_path, filename))
                    cv2.imwrite(os.path.join(output_path, 'img{}.jpg'.format(num)), img)
                    cv2.waitKey(2)
                    print("thumbnail created")
                    os.remove(os.path.join(input_path, filename))
                    print("removed raw image")
                    num += 1
                except Exception as error:
                    print(f"skipping {filename}", " ", error)
                    continue
        self.next(self.etl)
    
    @step
    def etl(self):
        #
        num = 0
        input_path = './data/tmp_preprocessed_img/'
        target = './data/tmp_processed_img/'

        file_id = str(int(time.time()))

        def extract_text(image):
            receiptOcrEndpoint = 'https://ocr.asprise.com/api/v1/receipt' # Receipt OCR API endpoint
            imageFile = image # Modify it to use your own file
            r = requests.post(receiptOcrEndpoint, data = { \
                                                    'api_key': 'TEST',        # Use 'TEST' for testing purpose \
                                                    'recognizer': 'auto',       # can be 'US', 'CA', 'JP', 'SG' or 'auto' \
                                                    'ref_no': 'ocr_python_123', # optional caller provided ref code \
                                                    }, \
            files = {"file": open(imageFile, "rb")}) # bill1.jpg
            return r

        def get_address_phone(ocr_text):
            test=""
            rgx_phone = re.compile(r"(?:\+\d{2})?\d{3,4}\D?\d{3}\D?\d{3}")
            phone_no = [x.replace(" ","") for x in ocr_text if re.findall(rgx_phone, x)]
            states = ["Abia", "Adamawa", "Akwa Ibom","Anambra","Bauchi","Bayelsa","Benue","Borno","Cross River","Delta","Ebonyi","Edo",
                    "Ekiti","Enugu","Gombe","Imo","Jigawa","Kaduna","Kano","Katsina","Kebbi","Kogi","Kwara","Lagos", "Mainland", "Island", "Lekki",
                    "Nasarawa","Niger","Ogun","Ondo","Osun","Oyo","Plateau","Rivers","Sokoto","Taraba","Yobe","Zamfara"]
            #ocr_text
            for i in states:
                for x in ocr_text:
                    if x.find(i) > 0:
                        test = x
                        break

            return test.lstrip(), phone_no[0]

        for filename in os.listdir(input_path):
            if any([filename.endswith(x) for x in [".png","jpg"]]):
                print(os.path.join(input_path, filename))
                res = json.loads(extract_text(os.path.join(input_path, filename)).text)
                df = pd.json_normalize(res["receipts"][0]["items"])
                #ocr_text = res["receipts"][0]["ocr_text"].split("\n")#" ".join(res["receipts"][0]["ocr_text"].split("\n")[0:10])
                address, phone_no = get_address_phone(res["receipts"][0]["ocr_text"].split("\n"))
                df["date"], df["merchant_name"], df["merchant_address"], df["address"], df["merchant_phone"] = \
                    res["receipts"][0]["date"], res["receipts"][0]["merchant_name"], res["receipts"][0]["merchant_address"], address, phone_no
                df = df[["date","merchant_name","description","amount","qty", "unitPrice","merchant_address", "address","merchant_phone"]]
                df = df[df["amount"] > 0.0]
                df.to_csv(f"./data/tmp_processed_csv/file_{file_id}_{num}.csv", index=False) #
                shutil.move(os.path.join(input_path, filename), target)
                num += 1
                time.sleep(1)
        self.next(self.file_upload)

    @step
    def file_upload(self):
        upload_files("invoice-text-extraction", "./data/tmp_processed_img", "processed_img/")

        upload_files("invoice-text-extraction", "./data/tmp_processed_csv", "processed_csv/")
        
        delete_files("invoice-text-extraction", "raw_img")
        # gcs folders: raw_img, processed_img, processed_csv
        self.next(self.end)

    @step
    def end(self):
        print("finished processing...")

if __name__ == '__main__':
    SingleTreeFlow()