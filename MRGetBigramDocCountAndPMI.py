"""
This guy computes a word co-occurrence matrix for every word in a corpus, taking as input a bunch of documents.
"""

#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from math import log
from utils import *

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

# Some extraction tidying parameters
BigramFrequencyThreshold = 5  # gte than this (>=)
BigramDocumentThreshold  = 5  # gte than this (>=)
BigramPMIThreshold       = None

ContextSize = 50 # How many words to the left and right to search

OutputFile = 'RESULT-Clean-Bigram-DocCount-PMI.txt.bz2'

DocumentSource = 'plain'

# This is a list of words filtered for frequency
CleanWordsFile = None 
# CleanWordsFile = 'en-articles-term-doc-frequency.txt'
MinVocabDocThreshold = 20 # How many documents should this word have appeared in before I use it (over the entire Wikipedia)

class MyShardedMothership(BZ2ShardedMothership):
    def initialize(self, args): 
        super(MyShardedMothership, self).initialize(args)

        self.word_freq = {}

    def end_task(self):
        logger.info('Computing normalization factors...')
        total_bigrams = 0
        for (i, shard) in enumerate(self.shuffle_result_shards):
            logger.info('  processing shard %d' % i)
            f = codecs.getreader('utf8')(BZ2File(shard))
            for (line_no, line) in enumerate(f.readlines()): 
                # print line.encode('utf8','replace'),
                try:
                    (word, word2, freq, doc_freq) = line.split('\t')

                    self.word_freq.setdefault(word, 0)
                    self.word_freq[word] += int(freq)

                    total_bigrams += int(freq)

                except ValueError:
                    logger.info('Line %d of %s is bad.' % (line_no, shard))
            f.close()

        total_words = sum(self.word_freq.values())

        # Now write the output to disk
        logger.info('Writing to disk...')
        writer = codecs.getwriter('utf8')(BZ2File(OutputFile, 'w'))
        for (i, shard) in enumerate(self.shuffle_result_shards):
            logger.info('  processing shard %d' % i)
            f = codecs.getreader('utf8')(BZ2File(shard))
            for (line_no, line) in enumerate(f.readlines()): 
                # print line.encode('utf8','replace'),
                try:
                    (word, word2, co_occurrence_sum, document_freq_sum) = line.split('\t')
                    co_occurrence_sum = int(co_occurrence_sum)
                    document_freq_sum = int(document_freq_sum)

                    try:
                        #pmi = log(co_occurrence_sum) - log(total_bigrams) \
                        #      - log(self.word_freq[word]) - log(self.word_freq[word2]) + 2*log(total_words)
                        # f.write('%s\t%s\t%f\t%d\t%d\n' % (word, word2, pmi, freq, self.document_freq[(word,word2)]))
                        #writer.write('%s\t%s\t%f\t%d\t%d\n' % (word, word2, pmi, co_occurrence_sum, document_freq_sum))
                        writer.write('%s\t%s\t%d\t%d\n' % (word, word2, co_occurrence_sum, document_freq_sum))
                    except KeyError:
                        logger.info('Line %d of %s is bad.' % (line_no, shard))

                except ValueError:
                    logger.info('Line %d of %s is bad.' % (line_no, shard))
            f.close()

        writer.close()
        logger.info('done.')
        sys.exit()

class MyMapper(Mapper):
    def initialize(self):
        """ 
        Read in the set of clean words
        """
        self.clean_words = set()

        logger.info('Reading in clean words...')

        if CleanWordsFile:
            reader = codecs.getreader('utf8')(open(CleanWordsFile))
            for line in reader.readlines():
                (word,doc_count,_) = line.split('\t')
                if word and doc_count > MinVocabDocThreshold:
                    self.clean_words.add(word)
            reader.close()

        logger.info('done.')
    def map(self, token):
        logger.info('Mapping token [%r]' % token)

        doc_count = 0
        found_this_doc = set()
        for document in get_document_iterator(DocumentSource, token):
            # Print out the result
            # print document.encode('utf8','replace')

            # Find co-occurrences within a window around each word.
            words = document.split(' ')
            for (i, word) in enumerate(words):
                if CleanWordsFile == None or word in self.clean_words:
                    start = max(0, i-ContextSize/2)
                    end   = min(len(words), i+ContextSize/2)
                    for word2 in words[start:end]:
                        if CleanWordsFile == None or word2 in self.clean_words:
                            if not (word,word2) in found_this_doc:
                                found_this_doc.add((word,word2))
                                self.output('%s\t%s\t1\t1' % (word, word2))
                            else:
                                self.output('%s\t%s\t1\t0' % (word, word2))
            # Report status
            doc_count += 1
            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

            found_this_doc = set()

        # Return success
        logger.info('Success!')
        return True

    def reduce(self, data_heap):
        """
        Sum up the occurrences
        """
        prev_bigram = ''
        document_freq_sum = 0
        co_occurrence_sum = 0
        while data_heap:
            (word, word2, freq, doc_freq) = heappop(data_heap).split('\t')
            # print 'read', word.encode('utf8','replace'), word2.encode('utf8','replace'), int(freq), int(doc_freq)
            if (word,word2) != prev_bigram:
                if prev_bigram != '':
                    # Only output things that occur in more than OutputDocumentFrequencyThreshold documents 
                    if co_occurrence_sum >= BigramFrequencyThreshold and document_freq_sum >= BigramDocumentThreshold:
                        self.output('%s\t%s' % prev_bigram + '\t%d\t%d' % (co_occurrence_sum, document_freq_sum))
                        # x = '%s\t%s' % prev_bigram
                        # print 'write', x.encode('utf8','replace'), co_occurrence_sum, document_freq_sum

                document_freq_sum = 0
                co_occurrence_sum = 0
                prev_bigram = (word,word2)

            co_occurrence_sum += int(freq)
            document_freq_sum += int(doc_freq)

UFOMapper     = MyMapper
UFOMothership = MyShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
