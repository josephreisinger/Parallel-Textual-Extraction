#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils import get_document_iterator
from utils.cleaner import *
from collections import defaultdict

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

# Some extraction tidying parameters
OutputDocumentFrequencyThreshold = 5  # gte than this (>=)
MinDocLength = 100

SourceDataType = 'wikipedia'

BZ2ShardedMothership.OutputFile = '%s-term-doc-freq-%dw-%dv.txt.bz2' % (SourceDataType,
        MinDocLength, OutputDocumentFrequencyThreshold)

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:', 'File:']

class MyMapper(Mapper):
    def map(self, token):
        logger.info('Mapping token [%r]' % token)
        tf = defaultdict(int)
        df = defaultdict(int)

        reader = codecs.getreader('utf8')(BZ2File(token))
        for (doc_count, (current_title, document, _)) in enumerate(clean_wikipedia_documents(reader, BannedArticleTypes,
             filter_extraneous=True)):
            terms = document.split()
            if len(terms) > MinDocLength:
                for word in terms:
                    tf[intern(word)] += 1
                for word in set(terms):
                    df[intern(word)] += 1

            # Report status
            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        # Return results
        for word in tf.iterkeys():
            self.output(u'%s\t%d\t%d' % (word.decode('utf8'), tf[word],
                df[word]))

        # Return success
        return True

    def reduce(self, data_heap):
        """
        Sum-reducer
        """
        total_df = defaultdict(int)
        total_tf = defaultdict(int)
        while data_heap:
            try:
                line = heappop(data_heap)
                (word, tf, df) = line.split('\t')
                total_df[word] += int(df)
                total_tf[word] += int(tf)
            except ValueError:
                logger.error('error on line [%s]\n' % line.encode('ascii','ignore'))

        for word in total_df.iterkeys():
            if total_df[word] >= OutputDocumentFrequencyThreshold:
                self.output('%s\t%d\t%d' % (word.encode('utf8'), total_tf[word],
                    total_df[word]))

UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
