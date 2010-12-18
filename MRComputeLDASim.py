#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from string import lower
from collections import defaultdict

from sim_utils import *

ClientRegistry.Port = 65520 # port to connect to the mothership
Mapper.Port         = 65521 # port for the RPC server on the child

ToShow = 50

# Output level
logger.setLevel( logging.INFO )

BZ2ShardedMothership.OutputFile = 'SIM.result.bz2'

class MyMapper(Mapper):
    def initialize(self, args):
        self.args = args
        self.map_initialized = False

    def map_initialize(self):
        """
        Only do this expensive step if we're running a mapper
        """

        logger.info('Initializing mapper')
        self.map_initialized = True

        self.RawFeatureVectors = self.args[0]
        # os.popen('cp %s /tmp/' % (self.RawFeatureVectors))
        # self.RawFeatureVectors = '/tmp/%s' % os.path.basename(self.RawFeatureVectors)

        if self.RawFeatureVectors.endswith('.bz2'):
            # self.RawFeatureVectors = codecs.getreader('utf8')(BZ2File(self.RawFeatureVectors))
            self.RawFeatureVectors = BZ2File(self.RawFeatureVectors)
        else:
            # self.RawFeatureVectors = codecs.getreader('utf8')(open(self.RawFeatureVectors))
            self.RawFeatureVectors = open(self.RawFeatureVectors)

        # HeadWords = set([x.strip('\n').split('\t')[0] for x in open(self.args[1]).readlines()])
        Similarity = self.args[1]

        # In the case of using post-WSD input, we need to merge the sense-tagged data
        # with the original data, removing the untagged version of the word
        self.SecondaryRawFeatureVectorFiles = None
        if len(self.args) > 2:
            self.SecondaryRawFeatureVectorFiles = map(BZ2File, self.args[2:])

        # Load the appropriate feature weighting and distance metric
        self.sim = load_similarity_metric(Similarity, self.RawFeatureVectors, self.SecondaryRawFeatureVectorFiles)

        # self.raw_feature_pipe = self.RawFeatureVectors
        # self.RawFeatureVectors.seek(0)
        # self.raw_feature_pipe = [map(intern, x.strip('\n').split('\t')) for x in
        #         self.RawFeatureVectors]

        # sys.stderr.write('Found %d universe features\n' % len(self.sim.universe_features))
        # sys.stderr.write('Found %d of %d headwords\n' %
        #         (len(self.sim.guy_to_features.keys()), len(HeadWords)))


    def map(self, token):
        """
        We load in each target and then compute its distance to /every/ head,
        outputting the results
        """
        logger.info('Mapping token [%r]' % token)
        
        if not self.map_initialized:
            self.map_initialize()

        # shard_heads = [x.strip('\n') for x in
        #         codecs.getreader('utf8')(BZ2File(token))]
        head_features = {}
        for line in BZ2File(token):
            tokens = line.strip().split('\t')
            head, raw_head_features = tokens[0], tokens[1:]
            head_features[head] = self.sim.compute_feature_vector(head, raw_head_features)

        sim_ranking_for = defaultdict(list)

        # Now do the similarity computations
        # self.raw_feature_pipe.seek(0)
        self.RawFeatureVectors.seek(0)
        for (i, line) in enumerate(self.RawFeatureVectors):
            if i % 100 == 0:
                sys.stderr.write('Computed %d similarities\n' % i)
            tokens = line.strip().split('\t')
            target, raw_target_features = tokens[0], tokens[1:]

            target_features = self.sim.compute_feature_vector(target, raw_target_features)
            # target_feature_key_set = set(target_features.keys())

            for hw, hf in head_features.iteritems():
                # assert self.sim.p_word.has_key(hw)
                if hw != target:
                    dist = self.sim.dist(target_features, hf)
                            
                    # dist = self.sim.dist(target_features,
                    #         target_feature_key_set, self.sim.guy_to_features[hw],
                    #         set(self.sim.guy_to_features[hw].keys()))

                    if len(sim_ranking_for[hw]) < ToShow:
                        heappush(sim_ranking_for[hw], (dist, target))
                    else:
                        heappushpop(sim_ranking_for[hw], (dist, target))

        # Print out the results
        for (hw, friends) in sim_ranking_for.items():
            for (score, target) in nlargest(ToShow, friends):
                self.output(unicode('%s\t%s\t%f' % (hw, target, score), 'utf8'))

        # Return success
        return True

UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
