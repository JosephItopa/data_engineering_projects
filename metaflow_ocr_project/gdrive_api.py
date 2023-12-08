import io
import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# folder_name = 'invoice'

# setup google drive
credentials = service_account.Credentials.from_service_account_file(
                              'deep-contact-credentials.json',\
                              scopes=['https://www.googleapis.com/auth/drive']
                            )
# call drive api client
service = build("drive", "v3", credentials=credentials)


def download_gdrive_files(local_dir, folder_name, folder_id):
    """download files from gdrive"""
    folder_metadata = {'name': folder_name,
                   "parents": [folder_id],
                   'mimeType': 'application/vnd.google-apps.folder'
                  }
    fileDownPath=local_dir # "./store/"
    results = service.files().list(q=f"'{folder_metadata['parents'][0]}' in parents", fields="files(id), files(name)").execute() # new_folder['id']
    items = results.get('files', [])
    for item in items:
        file_id = (item.get('id'))
        file_name = (item.get('name'))
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print ("Download %d%%." % int(status.progress() * 100) + file_name)
            filepath = fileDownPath + os.path.basename(file_name)
            with io.open(filepath, 'wb') as f:
                fh.seek(0)
                f.write(fh.read())

def upload_files(input_path, folder_name, folder_id):
    #upload file inside the folder
    folder_metadata = {'name': folder_name,
                   "parents": [folder_id],
                   'mimeType': 'application/vnd.google-apps.folder'
                  }
    for filename in os.listdir(input_path):
        #if any([filename.endswith(x) for x in [".png","jpg"]]):
        file_name = os.path.join(input_path, filename) # 
        file_metadata = {'name': file_name, 'parents': folder_metadata["parents"]} # [new_folder['id']]
        media = MediaFileUpload(file_name)
        file = service.files().create(body=file_metadata, media_body=media).execute()
        print(file_name+" "+"is uploaded")

def create_folders(folder_name, parent_folder_id):
    service = build("drive", "v3", credentials=credentials)
    folder_metadata = {'name': folder_name,\
                      "parents": [parent_folder_id],
                      'mimeType': 'application/vnd.google-apps.folder'
                     }
    # create folder 
    new_folder = service.files().create(body=folder_metadata).execute()
    print(folder_name+" "+"is created")

# https://stackoverflow.com/questions/40725769/move-a-file-with-google-drive-api
# https://developers.google.com/drive/api/guides/folder#move-files


def _trash_files(old_folder,old_folder_id):
  try:
    # pylint: disable=maybe-no-member
    # Retrieve the existing parents to remove
    """download files from gdrive"""
    folder_metadata = {'name': old_folder,\
                       "parents": [old_folder_id],\
                       'mimeType': 'application/vnd.google-apps.folder'
                  }
    results = service.files().list(q=f"'{folder_metadata['parents'][0]}' in parents", fields="files(id), files(name)").execute() # new_folder['id']
    items = results.get('files', [])

    for item in items:
        file_id = (item.get('id'))
        body_value = {'trashed': True}
        response = service.files().update(fileId=file_id, body=body_value).execute()
        #response = service.files().delete(fileId=file_id).execute()
        print(response.body)

  except HttpError as error:
    print(f"An error occurred: {error}")


# folder_name = pd.Timestamp.now().strftime("%Y-%m-%d %H%M%S")
# create_folders(folder_name, "11QhnyE5WphEHMrubYYpPC31Gtayppv2o")
#current_folder_id = "1jwZI8KthGWJCLdiTqq6EG2fkEXDFkys0" # "_processed_invoices"
#_trash_files("_invoices","11QhnyE5WphEHMrubYYpPC31Gtayppv2o")

#folder_name = pd.Timestamp.now().strftime("%Y-%m-%d %H%M%S")
#create_folders(folder_name, folder_id)
