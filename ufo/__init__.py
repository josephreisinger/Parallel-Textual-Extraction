import socket, threading, time, sys
import xmlrpclib, xmlrpcserver
import os
import codecs 
import string
from SocketServer import *
from utils import logger, group
from random import *
from heapq import *
from bz2 import *

FarmRate = 0.4
UFOMothership = None
UFOMapper     = None

ShuffleKeysPerShard = 5  # The number of simultaneous shuffle steps to perform
                          # on one data pass. (tradeoff speed for memory)
# LoadPreviousResults = True
LoadPreviousResults = False

# Globals containing the current state of all the clients
live_servers = []
idle_servers = []
live_servers_lock = threading.Lock()

def get_rpc(server, port):
    return xmlrpclib.ServerProxy("http://" + server + ":" + str(port) + "/")

class ClientRegistry(threading.Thread):
    """ Class to hold the list of currently available servers split into live
    and idle. """ 
    Port = None
    
    class Handler(StreamRequestHandler):
        def handle(self):
            server = self.rfile.readline(1024)
            #logger.info( 'Got:' + server )

            (server, port, updown) = server.split(' ')
            port = int(port)
            
            live_servers_lock.acquire()
            if updown == 'UP':
                if not (server, port) in live_servers:
                    live_servers.append((server, port))
                if not (server, port) in idle_servers:
                    idle_servers.append((server,port))
            else:
                if (server, port) in idle_servers:
                    idle_servers.remove((server,port))
                if (server,port) in live_servers:
                    live_servers.remove((server,port))
            
            logger.info( "New server %s:%d" % (server,port) + " Live servers: " + str(len(live_servers)) + " idle: " + str(len(idle_servers)) )
            live_servers_lock.release()
        
    def run(self):
        address = ('', ClientRegistry.Port)
        server = ThreadingTCPServer(address, self.Handler)
        logger.info( "starting server at (%s,%s) " % (address) )
        try:
            server.serve_forever()
        except:
            logger.error( "Handle server died!" )

def sendto(request, address):
    """
    Send a message request to address 
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
        s.send(request)
	s.close()
        return True
    except IOError, (errno, strerr):
        logger.error( strerr )
        return False


class Token:
    """ Tokens hold information regarding what evals have been farmed out, and
    how many are remaining for each organism."""
    def __init__(self):
        self.farmed    = False


class Mapper:
    """ Abstract base class for a generic client """
    Port = None
    Handlers = None # Handler function for each kind of token
    
    def __init__(self, mothership, args):
        """ 
        Carefully initialize the connection to the Mothership and register all
        methods defined by the subcless.
        """
        self.initialize(args)

        self.shuffle_keys = set()  # Contains the local set of shuffle keys
        self.stopped = False

        self.Port = randint(40000,90000)
        self.mothership = mothership
    
        try:
            self.hostname = socket.gethostname()

            # Ensure that we can set up a server or we die trying
            down = True
            while down:
                try:
                    logger.info( '%s:%d notifying %s of startup' % (socket.gethostname(), self.Port, mothership) )
                    sendto(socket.gethostname()+' '+str(self.Port)+' UP', (mothership, ClientRegistry.Port))
                    rpcserver = xmlrpcserver.XmlRpcHTTPServer((socket.gethostname(),self.Port))
                    down = False
                except socket.error:
                    logger.warning( 'Couldnt connect to mothership ' + mothership )
                    self.Port = randint(40000,90000)
                    time.sleep(1)
            
            # to register an object, do rpcserver.register('rpcname',object)
            # to register a method, do rpcserver.register('rpcname',method)
            rpcserver.register('map',self.rpc_map)
            rpcserver.register('shuffle',self.rpc_shuffle)
            rpcserver.register('terminate',self.rpc_terminate)

            logger.info( 'Client setup complete.' )

            while not self.stopped:
                rpcserver.handle_request()
            # rpcserver.serve_forever()
        finally: # cleanup no matter what happens
            self.terminate()

    def terminate(self):
        command = '%s %d DOWN' % (socket.gethostname(), self.Port)
        logger.info(command)
        sendto(command, (self.mothership,ClientRegistry.Port))
        sys.exit()

    def initialize(self, args):
        pass

    def output(self, string):
        """
        Add string to our output buffer, using a heap to maintain
        invariance
        """
        # self.writer.write(u'%s\n' % unicode(string, 'utf8'))
        self.writer.write('%s\n' % string)

        # This has to be in here b/c we don't have access to the output data
        # anywhere else
        if string[0:2].encode('ascii','replace').isalpha():
            self.shuffle_keys.add(string[0:2])

    def shuffle_reduce(self, token, base_path, shards):
        logger.info('Processing shuffle shard [%s] over %d mapper shards' % (token, len(shards)))

        # The basic idea here is to open each shard, find things that start with token and output them 
        shuffle(shards)  # randomize access order
        data = []
        for shard_no, shard in enumerate(shards):
            sys.stderr.write('%d / %d\n' % (shard_no, len(shards)))
            for line in codecs.getreader('utf8')(BZ2File(shard)): 
                if len(line) >= 2:
                    for t in token: 
                        if line.startswith(t):
                            heappush(data, line.strip())
                            # logger.info('on line [%s]' % line.encode('utf8'))

        output_file = '%s/REDUCE-%s-%s-%d-results.txt.bz2' % (base_path,
                hash(tuple(token)), self.hostname,self.Port)

        self.writer = codecs.getwriter('utf8')(BZ2File(output_file, 'w'))
        self.reduce(data)
        self.writer.close()

        return output_file

    def reduce(self, data_heap):
        """
        This is the generic passthru reducer
        """
        while data_heap:
            self.output(heappop(data_heap))


    def rpc_map( self, meta, token):
        output_file = token+'-%s-%d-results.txt.bz2' % (self.hostname,self.Port)
        self.writer = codecs.getwriter('utf8')(BZ2File(output_file, 'w'))
        data = self.load_previous_result(token)
        if not data:
            logger.info('Couldnt load previous result')
            data = self.map(token)
        self.writer.close()
            
        # logger.info( 'Got results: %s' % str(data) )
        if data:
            return {str(token):(output_file, list(self.shuffle_keys))}
        else:
            return {'FAILED':0}

    def load_previous_result(self, token):
        """
        Look through all the results files on disk and try to load them
        """
        if LoadPreviousResults:
            data = False
            (path, file_stem) = os.path.split(token)
            print path, file_stem

            for candidate in [x for x in os.listdir(path) if x.startswith(file_stem) and
                    x.find('result') >= 0]:
                candidate_path = os.path.join(path, candidate)
                logger.info('Trying to reload from candidate [%s]...' %
                        candidate_path)
                try:
                    self.reader = codecs.getreader('utf8')(BZ2File(candidate_path, 'r'))

                    for line in self.reader:
                        self.output(line.strip())
                except EOFError:
                    logger.info('Fail')
                    continue
                # except:
                #     logger.info('Really Fail')
                #     continue

                logger.info('Loaded successfully from [%s]' % candidate_path)
                return True

        return False



    def rpc_shuffle(self, meta, token, base_path, shards):
        output_file = self.shuffle_reduce(token, base_path, shards)
            
        # logger.info( 'Got results: %s' % str(data) )
        return {str(token):output_file}

    def rpc_terminate(self, meta):
        self.stopped = True
        return self.stopped

class Mothership(threading.Thread):
    """
    Abstract base class for a Mothership. Supports an arbitrary number of
    currently running tasks, each separated by a unique Moniker.

    Tokens are the universal currency required to initiate a transaction with a
    client. When a client completes evalutions for a particular token, the
    mothership decrements a counter for that token. When the counter reaches
    zero, the token is removed from the list of active tokens. When all tokens
    are consumed in this way, the generation is complete.
    """

    def __init__( self ):
        threading.Thread.__init__(self)
        
        self.base_path = ''  # Holds the base directory
            
        self.Task = {} 

        self.Tokens = {}
        self.Tokens_lock = threading.Lock()

        # Start up a thread for the Client Registry
        self.condor_server = ClientRegistry()
        self.condor_server.daemon = True
        self.condor_server.start()

        self.data_lock = threading.Lock()

        self.shuffled = False  # Have we run shuffle step
        self.skip_shuffle = False # should we skip the shuffle altogether and merge unsorted?
        self.shuffle_keys = set()  # For now we key off of the first letter

        self.map_result_shards = [] # Keep track of the output shard files that were successful
        self.shuffle_result_shards = [] # Keep track of the output shard files that were successful
        

    def handle_token_on_server(self, (server, port), token):
        """ A procedure for a thread waiting on an rpc call. Returns a function
        that uses the specified arguments."""
        try:
            if token[0] == 'map':
                res = get_rpc(server, port).map(token[1])
            elif token[0] == 'shuffle':
                res = get_rpc(server, port).shuffle(token[1], self.base_path, self.map_result_shards)
            else:
                raise 'Unknown token type'
        except Exception, detail:
            logger.warning( 'Dropping server: %r %r' % (Exception, detail) )
            return 
        
        # Add the server back to idle immediately
        live_servers_lock.acquire()
        try:
            idle_servers.append( (server,port) )
        finally:
            live_servers_lock.release()

        # Lock the tokens so it can be processed 
        self.Tokens_lock.acquire()
        try:
            self.process_result( res, (server,port), token )
        finally:
            self.Tokens_lock.release()

    def initialize(self, args):
        raise 'Need to implement initialize'
   
    def start_task(self, base_path, shards):
        """ Adds a task to the mothership's current tasks, infusing the first set of tokens. """
        
        self.Tokens_lock.acquire()

        self.initialize(base_path, shards)
        self.Tokens = self.get_map_tokens()

        self.Tokens_lock.release()

    def get_map_tokens(self):
        raise 'Need to implement get_map_tokens'

    def get_shuffle_tokens(self):
        #shuffle_keys = []
        #for s in string.lowercase:
        #    for s2 in string.lowercase:
        #        shuffle_keys.append(s+s2)
        #for s in string.uppercase:
        #    for s2 in string.lowercase+string.uppercase:
        #        shuffle_keys.append(s+s2)
        #
        #return dict([(('shuffle', shard), Token()) for shard in shuffle_keys])
        # return dict([(('shuffle', shard), Token()) for shard in string.lowercase+string.uppercase])
        return dict([(('shuffle', tuple(g)), Token()) for g in
            group(list(self.shuffle_keys), ShuffleKeysPerShard)])

    def end_task(self):
        raise 'Need to implement end_task'

    def process_result(self, result, live_server, token):
        """
        Munge the result from a client and add the data into our repository.
        """
        # Record the data from our transaction in the Tokens
        # Ensures that duplicated work is not accepted after the
        # token has been consumed (to prevent previous generation's
        # evals from affecting the current gen)
        if not result.has_key('FAILED'):
            if token in self.Tokens.keys():
                self.data_lock.acquire()
                if token[0] == 'map':
                    (shard, shuffle_keys) = result.values()[0]
                    self.shuffle_keys.update(shuffle_keys)
                    logger.info('Have %d shuffle keys (only alpha)' % (len(self.shuffle_keys)))
                    self.map_result_shards.append(shard)
                elif token[0] == 'shuffle':
                    self.shuffle_result_shards.append(result.values()[0])
                self.data_lock.release()

                # Consume the token if we've performed enough Evals
                self.print_complete(token, self.Tokens, live_server, live_servers, idle_servers)
                del self.Tokens[token]
            else: # Delete extraneous data
                if token[0] == 'map':
                    (shard, shuffle_keys) = result.values()[0]
                    os.remove(shard)
            

    def print_complete(self, token, tokens, live_server, live_servers, idle_servers):
        pass

    def farm_eval_to(self, (server,port)):
        """ Precondition: we're inside the live_servers_lock and token lock """
        # First farm out the unfarmed tokens, then farm out the incomplete tokens
        unfarmed_tokens = [x for x in self.Tokens.keys() if not self.Tokens[x].farmed]
        if unfarmed_tokens:
            token = choice(unfarmed_tokens)
            self.Tokens[token].farmed = True
        else:
            token = choice(self.Tokens.keys()) 
            
        logger.debug( "Assigning Token %s to %s:%d" % (str(token), server, port) )

        # p = threading.Thread(target=self.handle_token_on_server((server,port), token))
        p = threading.Thread(target=self.handle_token_on_server, args=((server,port), token))
        p.daemon = True
        p.start()

    def run(self):
        """ Run forever, taking into account that various NEAT instances can
        join and leave. The universal interface to the clients is through the
        tokens identified by a moniker. """
        logger.info( "Starting mothership..." )
        while True:
            time.sleep(FarmRate)

            # Check to see if we spent all the tokens for a particular game
            self.Tokens_lock.acquire()
            # End the current epoch and begin the next
            if not self.Tokens.keys():
                if self.shuffled or self.skip_shuffle:
                    # If we skip the shuffle step, send the map shards directly
                    # to the output
                    if self.skip_shuffle:
                        self.shuffle_result_shards = self.map_result_shards
                            
                    break
                else:
                    self.Tokens = self.get_shuffle_tokens()
                    logger.info('Starting on %d shuffle shards...' % len(self.Tokens))
                    self.shuffled = True

            # Try periodically to farm out jobs
            live_servers_lock.acquire()
            if self.Tokens.keys(): # Don't proceed unless we have tokens
                for idle_server in idle_servers:
                    try:
                        idle_servers.remove(idle_server)
                        self.farm_eval_to(idle_server)
                    except Exception, detail:
                        logger.error( 'Could not start thread for %s because of %s' % (idle_server, detail) )
                        
            live_servers_lock.release()
            self.Tokens_lock.release()

        self.end_task()


class BZ2ShardedMothership(Mothership):
    """
    For simple sharded mothership, look in the data directory and make as tokens all of the
    files that you see in there.
    """
    def initialize(self, base_path, shards_to_use):
        self.base_path = base_path
        assert os.path.exists(self.base_path)

        self.shards = ['%s/%s' % (self.base_path, file) for file in os.listdir(self.base_path) if file.endswith('.bz2') and file.find('result') < 0]
        if shards_to_use < len(self.shards):
            self.shards = sample(self.shards, shards_to_use)

        logger.info('Got %d shards from [%s]' % (len(self.shards), self.base_path))

    def get_map_tokens(self):
        return dict([(('map',shard), Token()) for shard in self.shards])

    def print_complete(self, token, tokens, live_server, live_servers, idle_servers):
        logger.info('COMPLETE [%r] (%d remaining) on server %s:%d (%d total, %d idle)' %
                    (token, len(tokens), live_server[0], live_server[1], len(live_servers), len(idle_servers)))

    def end_task(self):
        """
        This default end_task just merges all of the shuffled data to the local disk
        """
        # Kill remaining servers
        logger.info('killing')

        # Tell all the condor jobs to terminate
        live_servers_lock.acquire()
        for (i, (server, port)) in enumerate(live_servers):
            try:
                logger.info('killing %d: %s:%d' % (i, server, port ))
                get_rpc(server, port).terminate()
            except:
                logger.info('not killing %s:%d' % (server, port ))
        live_servers_lock.release()

        # Now write the output to disk
        logger.info('Merging to local disk...')
        self.merge_results()
        logger.info('done writing.')

        logger.info('done.')
        sys.exit()

    def merge_results(self):
        """
        Just do a passthru merge
        """
        writer = codecs.getwriter('utf8')(BZ2File(self.OutputFile, 'w'))
        for (i, shard) in enumerate(self.shuffle_result_shards):
            logger.info('  processing shard %d' % i)
            f = codecs.getreader('utf8')(BZ2File(shard))
            for (line_no, line) in enumerate(f): 
                # print line.encode('utf8','replace'),
                writer.write(line)
            f.close()

        writer.close()

def start_ufo(UFOMapper, UFOMothership):
    """
    Actually handles starting up the client or server depending on command line context.
    """
    import sys
    if sys.argv[1] == '--client':
        if len(sys.argv) < 3:
            print "usage: %s --client 'mothership'" % sys.argv[0]
            sys.exit()
        UFOMapper(sys.argv[2], sys.argv[3:])

    else:
        import os
        runner = UFOMothership()
        assert sys.argv[3] in ['--no-shuffle', '--shuffle']
        if sys.argv[3] == '--no-shuffle':
            runner.skip_shuffle = True
        elif sys.argv[3] == '--shuffle':
            runner.skip_shuffle = False
        runner.start_task(sys.argv[1], sys.argv[2])
        time.sleep(1) 

        # Monitor for the oending condition
        runner.start()
        runner.join()
