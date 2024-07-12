import os

class Credentials:

    def __init__(self) -> None:
        Credentials.mysql_user = ''
        Credentials.mysql_password = ''
    
    def set_credentials(env):
            # You can put condition here to set different credentials based on the environments
            Credentials.mysql_user = os.environ.get('MYSQL_USER')
            Credentials.mysql_password = os.environ.get('MYSQL_PASSWORD')
