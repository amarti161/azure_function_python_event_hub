from typing import List
import logging
import azure.functions as func
import json
import os
from sqlalchemy import create_engine, text
import pandas as pd
import urllib
from urllib.parse import quote_plus
import pyodbc
# # azure function
# from dotenv import load_dotenv
from O365 import Account, FileSystemTokenBackend


class DatabaseManager:
    def __init__(self, db_user_name, db_server_name, db_database, db_database_psw):
        self.db_user_name = db_user_name
        self.db_server_name = db_server_name
        self.db_database = db_database
        self.db_database_psw = db_database_psw
        self.engine = self._create_engine()

    def _create_engine(self):
        authentication = 'ActiveDirectoryInteractive'
        driver = '{ODBC Driver 18 for SQL Server}'
        # db_odbc_str = (f'Driver={driver};Server=tcp:serverbdtesthub.database.windows.net,1433;'
        #                f'Database=bdtest;Uid=martinaj;Pwd={self.db_database_psw};'
        #                f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        #
        # db_odbc_str = f'DRIVER={driver};SERVER={db_server_name};PORT=1433;UID={db_user_name};' \
        #               f'DATABASE={db_database};' \
        #               f'AUTHENTICATION={authentication}'

        db_odbc_str = f'Driver={driver};Server={self.db_server_name};PORT=1433;Uid={self.db_user_name};' \
                      f'Database={self.db_database};' \
                      f'Authentication={authentication}'
        db_connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(db_odbc_str)
        return create_engine(db_connect_str)

    def get_data_by_inject_id(self, inject_id):
        # query = text(f'SELECT * FROM[dbo].[Estudiantes] where ID = {inject_id}')
        query = text(f'SELECT DISTINCT i.Id, i.EventTitle, t.Name AS [Team], '
                     f'r.Name AS [Role], m.Email, m.FirstName, m.LastName '
                     f'FROM Inject i '
                     f'JOIN InjectTeam it ON it.InjectID = i.Id '
                     f'JOIN TeamRole tr ON tr.TeamId = it.TeamId '
                     f'JOIN Team t ON t.Id = it.TeamId JOIN Role r ON r.Id = tr.RoleId '
                     f'JOIN Member m ON m.Id = tr.MemberId WHERE i.Id = {inject_id}')

        with self.engine.connect() as conn:
            try:
                dataframe = pd.read_sql(query, conn)
                return dataframe
            except Exception as e:
                logging.error(f"Error retrieving data from the database: {e}")
                return pd.DataFrame()


class EmailManager:
    def __init__(self, mail_client_id, mail_client_secret):
        self.credentials = (mail_client_id, mail_client_secret)
        self.token_backend = FileSystemTokenBackend(token_path='.', token_filename='o365_token.txt')
        self.account = Account(self.credentials, token_backend=self.token_backend)

    def authenticate_if_needed(self):
        if not self.account.is_authenticated:
            self.account.authenticate(scopes=['basic', 'message_all'])

    def send_email(self, email, subject, content):
        try:
            self.authenticate_if_needed()
            mailbox = self.account.mailbox()
            message = mailbox.new_message()
            message.to.add(email)
            message.subject = subject
            message.body = content
            message.send()

        except Exception as e:
            logging.error(f"Error al enviar el correo: {e}")


class EventHandler:
    def __init__(self, db_manager: DatabaseManager, email_manager: EmailManager):
        self.db_manager = db_manager
        self.email_manager = email_manager

    def handle_event(self, event):
        try:
            message = event.get_body().decode('utf-8')
            message_obj = json.loads(message)

            if not message_obj:
                return

            inject_id = message_obj['InjectSession']['InjectId']

            if inject_id is None:
                return

            data_frame_result_set = self.db_manager.get_data_by_inject_id(inject_id)

            if "Email" not in data_frame_result_set.columns:
                return

            for _, row in data_frame_result_set.iterrows():
                emails = row["Email"]
                parsed_json_mails = json.loads(emails)
                for email in parsed_json_mails['mail']:
                    logging.info('Python results: %s', email)
                    # self.email_manager.send_email(email, "subject", "body")

        except Exception as e:
            logging.info('Error: %s', e)


def main(events: List[func.EventHubEvent]):
    db_user_name = os.environ.get("DB_USERNAME")
    db_server_name = os.environ.get("DB_SERVERNAME")
    db_database = os.environ.get("DB_DATABASE")
    db_database_psw = os.environ.get("DB_PSW")
    mail_client_id = os.environ.get("MAIL_CLIENT_ID")
    mail_client_secret = os.environ.get("MAIL_CLIENT_SECRET")

    db_manager = DatabaseManager(db_user_name, db_server_name, db_database, db_database_psw)
    email_manager = EmailManager(mail_client_id, mail_client_secret)
    event_handler = EventHandler(db_manager, email_manager)

    for event in events:
        event_handler.handle_event(event)
