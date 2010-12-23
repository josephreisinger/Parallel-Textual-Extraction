#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from string import lower

from subprocess import *

ClientRegistry.Port = 69520 # port to connect to the mothership
Mapper.Port         = 69521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

MinDocLength = 1000
DocTruncation = 1000  # Include the first X words
MinIncomingLinks = 0  # at least this many incoming/outgoing links

# SourceDataType = 'wikipedia-nospace'
SourceDataType = 'gigaword-nospace'

# BZ2ShardedMothership.OutputFile = 'wikipedia-%sw-%strunc-clean.txt.bz2' % (MinDocLength, DocTruncation)
BZ2ShardedMothership.OutputFile = '%s-%dw-%dlink-clean-nonstrict.txt.bz2' % (SourceDataType, MinDocLength, MinIncomingLinks)
POSTag = False
DocumentLinksFile = 'wikipedia-20090929-min15w.link_counts.txt.bz2'



# SourceDataType = 'wikipedia-strict-nospace'  # this is what was used for the
# original parser dump

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:']

class MyMapper(Mapper):
    def initialize(self, args):
        if POSTag:
            self.tagger = Popen(['/projects/nn/joeraii/emnlp-2009/postagger-1.0/tagger'],
                    stdin=PIPE, stdout=PIPE)

        # Read in document link weights
        self.clean_docs = set()

        if MinIncomingLinks > 0:
            logger.info('Reading in clean docs...')

            reader = codecs.getreader('utf8')(BZ2File(DocumentLinksFile))
            for line in reader.readlines():
                (doc,incoming,outgoing) = line.split('\t')
                incoming = int(incoming)
                outgoing = int(outgoing)
                if doc and incoming >= MinIncomingLinks:
                    self.clean_docs.add(doc)
            reader.close()

            logger.info('done.')

    def map(self, token):
        # Stick these in here to hide them from ungoliant
        from utils import get_document_iterator

        logger.info('Mapping token [%r]' % token)

        inside_body = False  # are we inside the body text or not
        buffer = []
        current_title = None

        doc_count = 0

        reader = codecs.getreader('utf8')(BZ2File(token))

        for (doc_count, (current_title, document, links, _)) in get_document_iterator(SourceDataType, token):
            document = document.replace('<CR>', ' ')

            split_doc = document.split()

            if MinIncomingLinks > 0 and current_title not in self.clean_docs:
                continue

            if len(split_doc) > MinDocLength:
                document = ' '.join(split_doc[:DocTruncation])
                # Cut to last period
                document = document[:document.rfind('.')+1]
                if POSTag:
                    pieces = []
                    for piece in document.split('.'):
                        self.tagger.stdin.write(('%s\n'%piece).encode('ascii', 'replace'))
                        pieces.append(self.tagger.stdout.readline().strip())
                    document = ' . '.join(pieces)
                # logger.info('outputting %s' % current_title)
                # self.output(u'%s\t%s' % (current_title, unicode(document).decode('utf8', 'replace')))
                self.output(u'%s\t%s' % (current_title.replace('\n', ' '),
                    document.replace('\n', ' ')))
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
