# from typing import List
# import logging
# import azure.functions as func
# import json
#
# # azure function
# import os
# from dotenv import load_dotenv
# from sqlalchemy import create_engine, text
# import numpy as np
# import pandas as pd
# import urllib
# from urllib.parse import quote_plus
# import pyodbc
#
# # mail
# # from O365 import Account, FileSystemTokenBackend
#
# db_user_name = os.environ.get("DB_USERNAME")
# db_server_name = os.environ.get("DB_SERVERNAME")
# db_database = os.environ.get("DB_DATABASE")
# db_database_psw = os.environ.get("DB_PSW")
#
#
# def main(events: List[func.EventHubEvent]):
#     for event in events:
#         try:
#             # logging.info('Python EventHub trigger processed an event: %s', event.get_body().decode('utf-8'))
#             # Decode the message - deserialize
#             message = event.get_body().decode('utf-8')
#             message_obj = json.loads(message)
#
#             if not message_obj:
#                 continue
#
#             inject_id = message_obj['InjectSession']['InjectId']
#
#             if inject_id is None:
#                 continue
#
#             data_frame_result_set = get_data_by_inject_id(inject_id, db_user_name, db_server_name,
#                                                           db_database, db_database_psw)
#
#             if "email" not in data_frame_result_set.columns:
#                 continue
#
#             for _, row in data_frame_result_set.iterrows():
#                 emails = row["email"]
#                 parsed_json_mails = json.loads(emails)
#                 for email in parsed_json_mails['mail']:
#                     logging.info('Python results: %s', email)
#                     # send_email(email, "Asunto del correo", "Cuerpo del correo")
#
#         except Exception as e:
#             logging.info('Error: %s', e)
#
#
# def get_data_by_inject_id(inject_id, db_user_name, db_server_name, db_database, db_database_psw):
#     # dev_username= 'tilmon.mccullum@fedgems.net'
#     # dev_servername= 'n-sql-2d-ms-mgt-001.database.windows.net'
#     # dev_database= 'n-sqldb-2d-ms-main-01'
#
#     # dev_user_name= 'martinaj'
#     # dev_server_name= 'n-sql-2d-ms-mgt-001.database.windows.net'
#     # dev_database= 'n-sqldb-2d-ms-main-01'
#
#     authentication = 'ActiveDirectoryInteractive'
#     driver = '{ODBC Driver 18 for SQL Server}'
#
#     db_odbc_str = f'Driver={driver};Server=tcp:serverbdtesthub.database.windows.net,1433;' \
#                   f'Database=bdtest;Uid=martinaj;Pwd={db_database_psw};' \
#                   f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
#
#     # db_odbc_str = f'DRIVER={driver};SERVER={db_server_name};PORT=1433;UID={db_user_name};' \
#     #               f'DATABASE={db_database};' \
#     #               f'AUTHENTICATION={authentication}'
#
#     db_connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(db_odbc_str)
#     engine = create_engine(db_connect_str)
#
#     query = text(f'SELECT * FROM[dbo].[Estudiantes] where ID = {inject_id}')
#
#     # query = text(f'SELECT DISTINCT i.Id, i.EventTitle, t.Name AS [Team], '
#     #              f'r.Name AS [Role], m.Email, m.FirstName, m.LastName '
#     #              f'FROM Inject i '
#     #              f'JOIN InjectTeam it ON it.InjectID = i.Id '
#     #              f'JOIN TeamRole tr ON tr.TeamId = it.TeamId '
#     #              f'JOIN Team t ON t.Id = it.TeamId JOIN Role r ON r.Id = tr.RoleId '
#     #              f'JOIN Member m ON m.Id = tr.MemberId WHERE i.Id = {inject_id}')
#
#     with engine.connect() as conn:
#         try:
#             dataframe = pd.read_sql(query, conn)
#             return dataframe
#         except Exception as e:
#             logging.error(f"Error retrieving data from the database: {e}")
#             return pd.DataFrame()



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
        db_odbc_str = (f'Driver={driver};Server=tcp:serverbdtesthub.database.windows.net,1433;'
                       f'Database=bdtest;Uid=martinaj;Pwd={self.db_database_psw};'
                       f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        db_connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(db_odbc_str)
        return create_engine(db_connect_str)

    def get_data_by_inject_id(self, inject_id):
        query = text(f'SELECT * FROM[dbo].[Estudiantes] where ID = {inject_id}')
        with self.engine.connect() as conn:
            try:
                dataframe = pd.read_sql(query, conn)
                return dataframe
            except Exception as e:
                logging.error(f"Error retrieving data from the database: {e}")
                return pd.DataFrame()


class EmailManager:
    def __init__(self):
        self.credentials = ('client_id', 'client_secret')  # Se pueden obtener desde variables de entorno
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

            if "email" not in data_frame_result_set.columns:
                return

            for _, row in data_frame_result_set.iterrows():
                emails = row["email"]
                parsed_json_mails = json.loads(emails)
                for email in parsed_json_mails['mail']:
                    logging.info('Python results: %s', email)
                    self.email_manager.send_email(email, "subject", "body")

        except Exception as e:
            logging.info('Error: %s', e)


def main(events: List[func.EventHubEvent]):
    db_user_name = os.environ.get("DB_USERNAME")
    db_server_name = os.environ.get("DB_SERVERNAME")
    db_database = os.environ.get("DB_DATABASE")
    db_database_psw = os.environ.get("DB_PSW")

    db_manager = DatabaseManager(db_user_name, db_server_name, db_database, db_database_psw)
    email_manager = EmailManager()
    event_handler = EventHandler(db_manager, email_manager)

    for event in events:
        event_handler.handle_event(event)