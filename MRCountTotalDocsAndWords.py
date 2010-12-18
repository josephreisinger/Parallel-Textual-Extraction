#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from utils import *
from string import lower
from collections import defaultdict

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

BZ2ShardedMothership.OutputFile = 'COUNT.txt.bz2'
MinDocLength = 100

SourceDataType = 'gigaword'

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:']

# RestrictToCategories = ['Political', 'political'] # Article must contain [[Category:*@@@*]] where @@@ is the set
RestrictToCategories = [] # Article must contain [[Category:*@@@*]] where @@@ is the set

class MyMapper(Mapper):
    def initialize(self, arg):
        pass
        
    def map(self, token):
        logger.info('Mapping token [%r]' % token)

        reader = codecs.getreader('utf8')(BZ2File(token))

        words_found, docs_found = 0, 0
        docs_found = 0 
        for (doc_count, (current_title, document)) in get_document_iterator(SourceDataType, token):
            words = document.split()
            if len(words) > MinDocLength:
                docs_found += 1
                words_found += len(words)
            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        self.output('%d\t%d' % (docs_found, words_found))

        reader.close()

        # Return success
        return True

    def reduce(self, data_heap):
        current = None
        occurrences = set()
        total_docs_found, total_words_found = 0, 0
        while data_heap:
            (docs_found, words_found) = heappop(data_heap).split('\t')
            total_docs_found += docs_found
            total_words_found += words_found

        self.output('%d\t%d' % (total_docs_found, total_words_found))
            


UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
