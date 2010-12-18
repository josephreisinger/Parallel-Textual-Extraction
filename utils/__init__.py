from cleaner import *
from parse_wikipedia import *
from sim import *
import codecs
from bz2 import *

def get_document_iterator(source_type, file, BannedArticleTypes=[]):
    f = codecs.getreader('utf8')(BZ2File(file))
    if source_type == 'wikipedia':
        documents = enumerate(clean_wikipedia_documents(f, BannedArticleTypes))
    elif source_type == 'wikipedia-strict':
        documents = enumerate(clean_wikipedia_documents(f, BannedArticleTypes,
            filter_extraneous=True))
    elif source_type == 'wikipedia-nospace':
        documents = enumerate(clean_wikipedia_documents(f, BannedArticleTypes,
            space_punctuation=False))
    elif source_type == 'wikipedia-strict-nospace':
        documents = enumerate(clean_wikipedia_documents(f, BannedArticleTypes,
            filter_extraneous=True, space_punctuation=False))
    elif source_type == 'gigaword':
        documents = enumerate(clean_gigaword_documents(f))
    elif source_type == 'gigaword-nospace':
        documents = enumerate(clean_gigaword_documents(f,
            space_punctuation=False))
    elif source_type == 'plain':
        documents = enumerate([x.replace('\n','') for x in f.readlines()])
    else:
        raise 'Unrecognized document source!'

    return documents
