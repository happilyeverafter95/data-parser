import os
import pandas as pd


class DataPipeline:
    def __init__(self):
        self.dir = 'maildir'
        self.data = self.extract_all_emails()

    def clean_email(self, text: str) -> str:
        return text

    def extract_all_emails(self) -> pd.DataFrame:
        emails = []
        employee = []
        email_types = []
        for subdir, dirs, files in os.walk(self.dir):
            if files != ['.DS_Store']:
                for file in files:
                    with open(os.path.join(subdir, file), 'rb') as f:
                        text = f.read()
                    employee.append(file.split('/')[0])
                    email_types.append(file.split('/')[1])
                    emails.append(self.clean_email(str(text, 'utf-8')))
        return pd.DataFrame({'text': emails,
                             'employee': employee,
                             'type': email_types})
