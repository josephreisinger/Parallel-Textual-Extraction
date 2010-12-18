#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from string import lower

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

BannedArticleTypes = ['Image:', 'Wikipedia:']

BZ2ShardedMothership.OutputFile = 'wikipedia-category-graph.txt.bz2'

class MyMapper(Mapper):
    def map(self, token):
        logger.info('Mapping token [%r]' % token)

        inside_body = False  # are we inside the body text or not
        buffer = []
        current_title = None

        doc_count = 0

        reader = codecs.getreader('utf8')(BZ2File(token))

        for (doc_count, (current_title, categories)) in enumerate(extract_categories(reader, BannedArticleTypes)):
            output_set = set()
            for category in categories:
                category = category.replace('[[','').replace(']]','')
                # print ('%s\t%s' % (current_title, category)).encode('utf8','replace')
                output_set.add('%s\t%s' % (current_title, category))

            for x in output_set:
                self.output(x)

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
