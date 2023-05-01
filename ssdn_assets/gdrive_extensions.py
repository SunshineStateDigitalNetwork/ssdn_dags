import os
from pathlib import Path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.error import HttpError


def gdrive_file_auth(fp):
    """

    :param fp: Path to credential file
    :return:
    """
    creds = None
    if os.path.exists(fp):
        creds = Credentials.from_service_account_file(fp)
    return creds


def gdrive_token_auth(*kwags):
    """

    :param kwags:
    :return:
    """
    creds = None

    return creds


def upload_to_folder(folder_id, creds, partner_dict):
    """

    :param folder_id:
    :param creds:
    :param partner_dict:
    :return:
    """
    partner_log_urls = dict()

    try:
        service = build('drive', 'v3', credentials=creds)

        for partner, partner_log_fp in partner_dict.items():
            partner_log_fp = Path(partner_log_fp)
            file_metadata = {
                'name': f'{partner_log_fp.name}',
                'parents': [folder_id],
                'mimeType': 'application/vnd.google-apps.spreadsheet',
            }

            new_spreadsheet = MediaFileUpload(partner_log_fp, chunksize=-1,
                                              mimetype='text/csv', resumable=True)
            file_upload = service.files().create(body=file_metadata,
                                                 media_body=new_spreadsheet,
                                                 fields='id').execute()
            partner_log_urls[partner] = f"https://docs.google.com/spreadsheets/d/{file_upload.get('id')}/edit?usp=sharing"

    except HttpError as error:
        print(f'An error occurred: {error}')
        return None

    return partner_log_urls

