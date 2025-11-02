import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer



def get_stoplist(filepath="stoplist-1.txt"):

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            stopwords = {line.strip() for line in f}
        return stopwords
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {filepath}")
        print("Asegúrate de que 'stoplist-1.txt' esté en la misma carpeta.")
        return set()


def preprocess(text, stemmer, stopwords):

    tokens = word_tokenize(text.lower(), language='spanish')

    processed_tokens = []
    for token in tokens:
        if not token.isalpha():
            continue
        if token in stopwords:
            continue

        stem = stemmer.stem(token)
        processed_tokens.append(stem)

    return processed_tokens





def calculate_weights(final_index, total_docs):

    pass

def query_ranker(query_string, final_index, idf_scores, doc_norms):

    pass

if __name__ == "__main__":
    print("Cargando herramientas de preprocesamiento...")
    spanish_stemmer = SnowballStemmer('spanish')
    stopwords_es = get_stoplist("stoplist-1.txt")  #

    test_sentence = "El presidente corrió rápido."

    print(f"Frase original: '{test_sentence}'")
    final_tokens = preprocess(test_sentence, spanish_stemmer, stopwords_es)

    print(f"Tokens finales: {final_tokens}")

    test_sentence_2 = "Los gatos corrían y jugaban en el jardín, ¡qué día!"
    print(f"\nFrase original: '{test_sentence_2}'")
    final_tokens_2 = preprocess(test_sentence_2, spanish_stemmer, stopwords_es)
    print(f"Tokens finales: {final_tokens_2}")