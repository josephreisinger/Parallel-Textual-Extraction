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

BadRedirectLinkCountThreshold = 4 

# Output level
logger.setLevel( logging.INFO )

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:']

SourceDataType = 'wikipedia-nospace'
MinDocumentLength = 1000

SkipLanguages = ['fr']

BZ2ShardedMothership.OutputFile = '%s-language-links-%dmin-skip-%s.txt.bz2' % (SourceDataType, MinDocumentLength, '-'.join(SkipLanguages))



class MyMapper(Mapper):

    def process(self, current_title, split_doc, links):
        if len(split_doc) > MinDocumentLength:
            output_set = set()
            for link in links:
                # self.output('%s\t%s' % (current_title, link))
                # print current_title, link.encode('ascii', 'replace')
                for lang in SkipLanguages:
                    if link.startswith('[['+lang+':'):
                        return
            
            self.output(current_title)

    def map(self, token):
        from utils import get_document_iterator
        logger.info('Mapping token [%r]' % token)

        inside_body = False  # are we inside the body text or not
        buffer = []
        current_title = None

        doc_count = 0

        reader = codecs.getreader('utf8')(BZ2File(token))

        for (doc_count, (current_title, document, links, flags)) in get_document_iterator(SourceDataType, token, BannedArticleTypes):
            document = document.replace('<CR>', ' ')
            split_doc = document.split()
            self.process(current_title, split_doc, links)

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
