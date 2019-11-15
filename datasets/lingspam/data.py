import os
import pandas as pd


class DataPipeline:
    def __init__(self):
        self.dir = 'lingspam_public/bare'
        self.data = self.extract_all_messages()

    def parse_message(self, msg) -> dict:
        subject = msg.split('\n\n')[0].replace('Subject: ', '')
        body = msg.split('\n\n')[1]
        return {'subject': subject,
                'body': body}

    def extract_all_messages(self) -> pd.DataFrame:
        subject_titles = []
        messages = []
        labels = []
        for subdir, dirs, files in os.walk(self.dir):
            if files != ['.DS_Store']:
                for file in files:
                    with open(os.path.join(subdir, file), 'r') as f:
                        text = f.read()
                    parsed_message = self.parse_message(text)
                    subject_titles.append(parsed_message['subject'])
                    messages.append(parsed_message['body'])
                    labels.append(int('spmsg' in file))
        return pd.DataFrame({'subject': subject_titles,
                             'message': messages,
                             'label': labels})

    def extract_messages(self):
        self.data.to_csv('messages.csv', index=False)
