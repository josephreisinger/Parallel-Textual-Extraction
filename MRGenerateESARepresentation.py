#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from string import lower
from collections import defaultdict
from random import random

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

# HeadWords = 'wordsim-353.heads.bz2'
HeadWords = 'wikipedia-term-5v-10df.txt.bz2'
MinDocLength = 100

BZ2ShardedMothership.OutputFile = 'ESA-%s-min%dw.inverted-index.bz2' % (HeadWords, MinDocLength)

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:', 'File:']

class MyMapper(Mapper):
    def initialize(self, arg):
        self.heads = set()

        logger.info('Reading in clean words...')

        reader = codecs.getreader('utf8')(BZ2File(HeadWords))
        for line in reader.readlines():
            word = line.strip()
            if word:
                self.heads.add(word)
        reader.close()

        logger.info('done.')
            
        
    def map(self, token):
        word_index = defaultdict(int)
        document_size = defaultdict(int)
        vocab_size = defaultdict(int)

        logger.info('Mapping token [%r]' % token)

        reader = codecs.getreader('utf8')(BZ2File(token))

        for (doc_count, (current_title, document, _)) in enumerate(clean_wikipedia_documents(reader, BannedArticleTypes,
             filter_extraneous=False)):
            terms = document.split()
            if len(terms) > MinDocLength:
                for w in terms:
                    if w in self.heads:
                        word_index[(current_title, w)] += 1
                document_size[current_title] = len(terms)
                vocab_size[current_title] = len(set(terms))
            
                # Collect doc size for everything, not just docs we find useful
                # words in
                self.output('_\t%s\t%d\t%d\t%d' %
                        (current_title,0,document_size[current_title],
                            vocab_size[current_title]))

                

            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        reader.close()

        for ((c,w), tf) in word_index.iteritems():
            self.output('%s\t%s\t%d\t%d\t%d' % (w,c,tf,document_size[c], vocab_size[c]))

        # Return success
        return True

UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
