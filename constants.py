import os

# ENV DATA

from dotenv import load_dotenv
load_dotenv('this_is_not_an_env.env')
USER=os.getenv("USER")
PASSWORD=os.getenv("PASSWORD")
HOST=os.getenv('HOST')
PORT=os.getenv("PORT")
DATABASE_NAME=os.getenv("DATABASE_NAME")
CONN_STRING = os.getenv("CONN_STRING").replace('{user}',USER)
CONN_STRING = CONN_STRING.replace('{host}',HOST)
CONN_STRING = CONN_STRING.replace('{password}',PASSWORD)
CONN_STRING = CONN_STRING.replace('{port}',PORT)
CONN_STRING = CONN_STRING.replace('{database_name}',DATABASE_NAME)


DRAW_RESULTS = ['50move',
         'agreed',
         'insufficient',
         'repetition',
         'stalemate',
         'timevsinsufficient']
LOSE_RESULTS = [
    'checkmated',
    'resigned',
    'timeout','abandoned'
]