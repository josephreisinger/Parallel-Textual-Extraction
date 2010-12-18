"""
Given a set of head words and their contexts; we compute running similarity
sufficient statistics that can be interpreted as similarity scores
"""
#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from utils import get_document_iterator
from string import lower
from collections import defaultdict
from sim_utils import *

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

# Basically controls the amount of data we process
MinDocLength = 1000

# HeadWordsFile = 'wikipedia-1000w.headwords50.trimmed'
ContextMoniker = sys.argv[4]
HeadWords = sys.argv[5:]
# SourceDataType = 'gigaword'
# TermFreqFile = 'gigaword-term-doc-freq-5v.txt.bz2'
SourceDataType = 'wikipedia'
TermFreqFile = 'wikipedia-term-doc-freq-5v.txt.bz2'
StopWordsFile = 'stopwords.txt.bz2'
MinFeatureUnigramDocFreq = 5  # for collecting the contexts, what words should we add
MinTargetWordDocFreq = 1000  # we're comparing sim to these words; their minimum freq

ContextSize = 50
ContextType = 'uni'  # full contexts is just the previous ContextSize words
# ContextSize = 1
# ContextType = 'lr'  # currently lr or uni
assert ContextType in ['uni', 'lr']

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:']


BZ2ShardedMothership.OutputFile = 'SIM-%s-%dw-min%dtwf-%d%s-%s.txt.bz2' % (SourceDataType, MinDocLength,\
                                                      MinTargetWordDocFreq,\
                                                      ContextSize, ContextType,\
                                                      os.path.basename(ContextMoniker))


class MyMapper(Mapper):
    def initialize(self, args):
        """ 
        Read in the set of headwords
        """

        self.mapper_initialized = False

    def initialize_mapper(self):
        self.head_words = defaultdict(lambda: defaultdict(int))
        logger.info('Reading in head words...')
        for hw_file in HeadWords:
            logger.info(hw_file)
            # reader = codecs.getreader('utf8')(open(HeadWordsFile))
            # reader = codecs.open(hw_file, 'r', 'utf8', errors='replace')
            reader = codecs.getreader('utf8')(BZ2File('%s-%s.lda.bz2' %
                (ContextMoniker, hw_file)))
            for line in reader.readlines():
                tokens = line.strip().split('\t')
                doc, words = tokens[0], map(parse_lda_entry, tokens[1:])

                for w,c in words:
                    self.head_words[doc][intern(w.encode('ascii', 'replace'))] = c
            reader.close()

        # Read in stop words
        logger.info('Reading in stop words...')
        self.stop_words = set()
        reader = codecs.getreader('utf8')(BZ2File(StopWordsFile))
        for line in reader.readlines():
            word = line.strip()
            self.stop_words.add(word)

        # Read in the term frequency information
        logger.info('Reading in term freq...')
        self.unigram_term_freq = defaultdict(int)
        self.unigram_doc_freq = defaultdict(int)
        self.target_words = set()
        self.good_words = set()
        reader = codecs.getreader('utf8')(BZ2File(TermFreqFile))
        for line in reader.readlines():
            (word, tf, df) = line.split('\t')
            self.unigram_term_freq[word] = int(tf)
            self.unigram_doc_freq[word] = int(df)
            if int(df) >= MinTargetWordDocFreq and not word in self.stop_words:
                self.target_words.add(word)
            if int(df) >= MinFeatureUnigramDocFreq:
                self.good_words.add(word)
        reader.close()

        self.mapper_initialized = True

    def map(self, token):
        logger.info('Mapping token [%r]' % token)

        if not self.mapper_initialized:
            logger.info('Initializing mapper...')
            self.initialize_mapper()

        # Contains Jaccard top, jaccard bottom, wt top, wt bottom
        collected_stats = {}

        for (doc_count, (current_title, document)) in get_document_iterator(SourceDataType, token):
             #keep_punctuation=True, filter_extraneous=True)):
            words = document.replace('<CR>', ' ').decode('ascii', 'replace').split()
            # print current_title, words
            if len(words) > MinDocLength:
                logger.info(current_title)
                for (i,w) in enumerate(words):
                    if w in self.target_words:
                        logger.info(w)
                        try:
                            if ContextType == 'lr':
                                assert False # not using min doc freq
                                context = '_'.join(words[max(0,i-ContextSize):i] + '_<>_' +
                                        words[i+1:min(i+1+ContextSize, len(words))])
                            elif ContextType == 'uni':
                                buffer = defaultdict(int)
                                for k in range(max(0,i-ContextSize), min(i+ContextSize, len(words))):
                                    if k != i and words[k] in self.good_words:
                                        buffer[words[k]] += 1
                                for hw, hw_features in self.head_words.iteritems():
                                    collected_stats.setdefault((hw, w), [0,0,0,0])
                                    for feature, c in buffer.iteritems():
                                        # Increment the tops if we find them
                                        if feature in hw_features:
                                            collected_stats[(hw, w)][0] += 1
                                            collected_stats[(hw, w)][2] += c / float(self.unigram_doc_freq[feature])
                                        # Always increment the bottoms
                                        collected_stats[(hw, w)][1] += 1
                                        collected_stats[(hw, w)][3] += c / float(self.unigram_doc_freq[feature])
                        except UnicodeEncodeError:
                            sys.stderr.write('FAILED\n')

            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        for (hw, tw), stats in collected_stats.iteritems():
            self.output(u'%s\t%s\t%s' % (hw, tw, '\t'.join(map(str, stats))))

        # Return success
        return True

    def reduce(self, data_heap):
        """
        This one collects word/context pairs and outputs to the LDA format
        """
        current = None
        total_jac_bottom, total_jac_top, total_wt_jac_top, total_wt_jac_bottom = 0, 0, 0, 0
        while data_heap:
            (head, target, jac_top, jac_bottom, wt_jac_top, wt_jac_bottom) = heappop(data_heap).split('\t')

            if current != (head,target):
                if current:
                    self.output('%s\t%s\t%d\t%f\t%f\t%d\t%f\t%f' % (head, target,
                        total_jac_top, total_jac_bottom, total_wt_jac_top,
                        total_wt_jac_bottom,
                        total_jac_top/float(total_jac_bottom),
                        total_wt_jac_top/float(total_wt_jac_bottom)))
                    total_jac_bottom, total_jac_top, total_wt_jac_top, total_wt_jac_bottom = 0, 0, 0, 0

                current = (head,target)

            total_jac_top += float(jac_top)
            total_jac_bottom += float(jac_bottom)
            total_wt_jac_top += float(wt_jac_top)
            total_wt_jac_bottom += float(wt_jac_bottom)

        self.output('%s\t%s\t%d\t%d\t%f\t%f\t%f\t%f' % (head, target,
            total_jac_top, total_jac_bottom, total_wt_jac_top,
            total_wt_jac_bottom,
            total_jac_top/float(total_jac_bottom),
            total_wt_jac_top/float(total_wt_jac_bottom)))
                
UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
