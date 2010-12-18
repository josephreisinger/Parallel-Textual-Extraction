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

MinDocLength = 10000
MinVocabDocThreshold = 10
SamplePercentage = 1.0

Representation = '10gram'
if Representation.endswith('gram'):
    NGramSize = int(Representation.split('gram')[0])
else:
    assert Representation in ['document']
    NGramSize = -1

MinIncomingLinkWeight = 5  # at least this many incoming/outgoing links

Moniker = 'wikipedia-%s-min%dw-%dv-%dlink' % (Representation, MinDocLength, MinVocabDocThreshold, MinIncomingLinkWeight)

if SamplePercentage < 1.0:
    BZ2ShardedMothership.OutputFile = '%s-%.3fsample.docify.bz2' % (Moniker, SamplePercentage)
else:
    BZ2ShardedMothership.OutputFile = '%s.docify.bz2' % (Moniker)

CleanWordsFile = 'en-articles-doc-term-frequency.txt.bz2'
DocumentLinksFile = 'wikipedia-20090929-min15w.link_counts.txt.bz2'

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:', 'File:']

# RestrictToCategories = ['Political', 'political'] # Article must contain [[Category:*@@@*]] where @@@ is the set
RestrictToCategories = [] # Article must contain [[Category:*@@@*]] where @@@ is the set

class MyMapper(Mapper):
    def initialize(self, arg):
        self.clean_words = set()

        logger.info('Reading in clean words...')

        reader = codecs.getreader('utf8')(BZ2File(CleanWordsFile))
        for line in reader.readlines():
            (word,doc_count,_) = line.split('\t')
            doc_count = int(doc_count)
            if word and doc_count > MinVocabDocThreshold:
                self.clean_words.add(word)
        reader.close()

        logger.info('done.')

        # Read in document link weights
        self.clean_docs = set()

        logger.info('Reading in clean docs...')

        reader = codecs.getreader('utf8')(BZ2File(DocumentLinksFile))
        for line in reader.readlines():
            (doc,incoming,outgoing) = line.split('\t')
            incoming = int(incoming)
            outgoing = int(outgoing)
            if doc and incoming >= MinIncomingLinkWeight:
                self.clean_docs.add(doc)
        reader.close()

        logger.info('done.')
            
    def get_lda_rep(self, words, start=None, end=None):
        """
        Output a string of words in the sparse LDA format
        """
        if start != None:
            assert end != None
            words = words[start:end]
        
        word_count = defaultdict(int)
        for word in words:
            if word_count.has_key(word) or word in self.clean_words: 
                word_count[word] += 1
        return '\t'.join(['%s:%d' % (k,v) for (k,v) in word_count.items()])

    def map(self, token):
        logger.info('Mapping token [%r]' % token)

        reader = codecs.getreader('utf8')(BZ2File(token))

        for (doc_count, (current_title, document, flags)) in enumerate(clean_wikipedia_documents(reader, BannedArticleTypes,
             filter_extraneous=False)):
            if current_title not in self.clean_docs:
                continue

            if len(document.split()) > MinDocLength and random() < SamplePercentage:
                # Check to see if we match any categories
                matched = not RestrictToCategories 
                for category in RestrictToCategories:
                    if re.compile('Category:.*%s.*' % category).search(raw_buffer): 
                        matched = True
                        logger.info('Matched [%s]' % current_title)
                        break

                if matched:
                    if NGramSize == -1:  # Entire document
                        self.output(u'%s\t%s' % (current_title,
                            self.get_lda_rep(document.split())))
                    else:
                        words = document.split()
                        for i, word in enumerate(words):
                            if i+NGramSize < len(words):
                                lda_rep = self.get_lda_rep(document.split(), i, i+NGramSize)
                                if lda_rep:
                                    self.output(u'%s-%d-%d\t%s' % (current_title, i,
                                        i+NGramSize, lda_rep))
            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        reader.close()

        # Return success
        return True

UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
