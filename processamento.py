import pandas as pd
import spacy
from unidecode import unidecode
import string

df_comentarios_completo_assunto = pd.read_parquet(
    'dados/ouro/comentarios.parquet')


def remove_stop_words(sentence):
    try:
        nlp = spacy.load("pt_core_news_sm")
        sentence = unidecode(sentence).strip().lower()
        for c in string.punctuation:
            sentence = sentence.replace(c, '')
        doc = nlp(sentence)
        filtered_tokens = [token for token in doc if not token.is_stop]
        palavras = list(filtered_tokens)
        lista_palavras = [palava for palava in palavras if len(palava) > 1]
        return lista_palavras
    except:

        return ''


df_comentarios_completo_assunto['TEXTO_TRATADO'] = df_comentarios_completo_assunto['textOriginal'].apply(
    remove_stop_words)

df_comentarios_completo_assunto.to_parquet(
    'dados/ouro/comentarios_tradado.parquet')
