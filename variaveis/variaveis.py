from airflow.models import Variable
import requests
import os
from dotenv import load_dotenv
import logging
load_dotenv()


# lista_assunto = [
#     'Power BI',
#     'Python AND dados',
#     'Cities Skylines',
#     'Cities Skylines 2',
#     'Linux',
#     'Linux Gamming',
#     'genshin impact',
#     'zelda'
# ]


lista_assunto = [
    'Infection Free Zone',
    'Manor Lords',
    'Linux'
]


logging.basicConfig(
    filename='../log/requisicao.log',
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s %(message)s')


def checar_url(url: str, key: str):
    params = {
        'part': 'snippet',
        'regionCode': 'BR',
        'key':  key

    }
    url = url + '/videoCategories/'
    req = requests.get(url, params=params)

    if req.status_code == 200:
        return True
    return False


# if checar_url(os.environ['URL_API_YOUTUBE'], os.environ['key_youtube']):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube')
# elif checar_url(os.environ['URL_API_YOUTUBE'], os.environ['key_youtube_dois']):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_dois')
# elif checar_url(os.environ['URL_API_YOUTUBE'], os.environ['key_youtube_tres']):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_tres')
# elif checar_url(os.environ['URL_API_YOUTUBE'], os.environ['key_youtube_quatro']):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_quatro')
# elif checar_url(os.environ['URL_API_YOUTUBE'], os.environ(['key_youtube_cinco']):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_cinco')
# else:
#     url_youtube = ''
#     chave_youtube = ''


# url_youtube = Variable.get('URL_API_YOUTUBE')
# chave_youtube = Variable.get('key_youtube')

url_youtube = os.environ['URL_API_YOUTUBE']
chave_youtube = os.environ['key_youtube']


# if checar_url(Variable.get('URL_API_YOUTUBE'), Variable.get('key_youtube')):

#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube')
#     logging.debug('Resultado da requisição')
#     logging.debug(f'key_youtube {chave_youtube}')

# elif checar_url(Variable.get('URL_API_YOUTUBE'), Variable.get('key_youtube_dois')):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_dois')
#     logging.debug('Resultado da requisição')
#     logging.debug(f'key_youtube_dois {chave_youtube}')

# elif checar_url(Variable.get('URL_API_YOUTUBE'), Variable.get('key_youtube_tres')):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_tres')
#     logging.debug('Resultado da requisição')
#     logging.debug(f'key_youtube_tres {chave_youtube}')

# elif checar_url(Variable.get('URL_API_YOUTUBE'), Variable.get('key_youtube_quatro')):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_quatro')
#     logging.debug('Resultado da requisição')
#     logging.debug(f'key_youtube_quatro {chave_youtube}')
# elif checar_url(Variable.get('URL_API_YOUTUBE'), Variable.get('key_youtube_cinco')):
#     url_youtube = Variable.get('URL_API_YOUTUBE')
#     chave_youtube = Variable.get('key_youtube_cinco')
#     print('key_youtube_cinco')
#     logging.debug('Resultado da requisição')
#     logging.debug(f'key_youtube_cinco {chave_youtube}')
# else:
#     url_youtube = ''
#     chave_youtube = ''
