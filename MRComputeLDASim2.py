#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from utils.cleaner import *
from string import lower
from collections import defaultdict

from sim_utils import *

from MRComputeLDASim import MyMapper

ClientRegistry.Port = 75520 # port to connect to the mothership
Mapper.Port         = 75521 # port for the RPC server on the child

ToShow = 50

# Output level
logger.setLevel( logging.INFO )

BZ2ShardedMothership.OutputFile = sys.argv[3]

UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    print ClientRegistry.Port
    print UFOMapper.Port
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
