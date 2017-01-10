import sys, getopt, os
from xmltodict import parse as parseToDict
import ujson as json
import pickle
import redis
from collections import OrderedDict
from datetime import datetime
import logging
import signal
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue as Queue

inputdir = ''
maxThreads = 20
exitThread = False
redisPool = redis.ConnectionPool(host='localhost')


def enableLogging(logLevel):
    numeric_level = getattr(logging, logLevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % logLevel)
    logging.basicConfig(level=numeric_level, format='(%(processName)-10s) %(message)s')
    
# Json for the header file
def makeExlObjectTplMap(tree, redisConn):
    try:
        redisConn.hmset('EXLCache', {tree['exl']['name'] : json.dumps(tree['exl']['exlHeader'])})
        return tree['exl']['name']
    except:
        logging.error("Unable to build the ExlTplDict for file " + inputfile)
        raise

def insertIntoQueue(inst, redisConn):
    redisConn.hmset('RICCache', {inst['it:RIC'] : json.dumps(inst)})
    redisConn.hmset('SYMBOLCache',{inst['it:SYMBOL'] : json.dumps(inst)})
    if 'it:UP_ISIN' in inst['exlObjectFields']:
        redisConn.hmset('ISINCache', {inst['exlObjectFields']['it:UP_ISIN'] : json.dumps(inst)})
    
#Json for the product record    
def makeExlObjectMap(tree, tplName, redisConn):
    totInstRecord = 0
    if not isinstance(tree['exl']['exlObjects'], dict):
        logging.warning("Doesn't contain instrument entires for file " + inputfile)
        return totInstRecord

    try:
        instDict = tree['exl']['exlObjects']['exlObject']
        if not isinstance(instDict, list):
            instDict['ExlTplKey'] = tplName
            insertIntoQueue(instDict, redisConn)
            totInstRecord = 1
        else:
            for inst in instDict:
                inst['ExlTplKey'] = tplName
                insertIntoQueue(inst, redisConn)
                totInstRecord += 1
    except:
        logging.error("Unable to build object map for file " + inputfile)
        raise
    return totInstRecord

# the main function which processes the file and inserts into redis
def convertXmlToDictJson(inputfile, redisConn):
    treeDict = parseToDict(open(inputfile, 'r').read())
    tplName = makeExlObjectTplMap(treeDict, redisConn)
#    with open(inputfile + '.json', 'w') as wfile:
#        wfile.write(json.dumps(treeDict, indent=4 * ' '))
    return makeExlObjectMap(treeDict, tplName, redisConn)

def run(fileList, resultQueue):
    global redisPool
    thTotRecord = 0
    redisConn = redis.StrictRedis(connection_pool=redisPool)
    for inputfile in fileList:
        totInstRecord = 0
        try:
            totInstRecord = convertXmlToDictJson(inputfile, redisConn)
        except (KeyboardInterrupt, SystemExit):
            logging.info("Ending Thread...")
            break
        except:
            logging.warning("Failed processing file [" + inputfile + "]")
        logging.debug("Wrote [" + str(totInstRecord) + "] product records for [" + inputfile + "]")
        thTotRecord += totInstRecord
        if (exitThread):
            logging.info("Ending Thread ...")
            break
    logging.info("Thread processed [" + str(thTotRecord) + "] records")
    resultQueue.put(thTotRecord)
    
def signalExit(sig, frame):
    global exitThread
    logging.info("Received signal ... marking exit")
    exitThread = True;
    sys.exit(0)
        
def main(argv):
    global maxThreads, inputdir
    logLevel = "ERROR"
    try:
        opts, args = getopt.getopt(argv,"hi:t:l:",["idir=", "threads=", "log="])
    except getopt.GetoptError:
        print 'convertXmlToJson.py -i <input dir> -t <max threads> -l <log level>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'convertXmlToJson.py -i <input dir> -t <max threads> -l <log level>'
            sys.exit(0)
        elif opt in ("-i", "--idir"):
            inputdir = arg
        elif opt in ("-t", "--threads"):
            maxThreads = int(arg)
        elif opt in ("-l", "--log"):
            logLevel = arg

    signal.signal(signal.SIGTERM, signalExit )
    signal.signal(signal.SIGINT, signalExit)
            
    if (inputdir == ''):
        print 'Error : usage convertXmlToJson.py -i <input dir (mandatory)> -t <max threads> -l <log level>'
        sys.exit(1)

    enableLogging(logLevel)
            
    productList = [f for f in os.listdir(inputdir) if f.endswith('.exl')]
    productList = [inputdir + '/' + s for s in productList]
    threadListSize = -(-len(productList) // maxThreads)
    threadFileList = [productList[x:x+threadListSize] for x in xrange(0, len(productList), threadListSize)]
    thList = []

    startTime = datetime.now()
    logging.info("Started processing at " + str(startTime))

    resultQueue = Queue();

    for threadno in xrange(maxThreads):
        th = Process(target=run, args=(threadFileList[threadno], resultQueue), name='ExlThread' + str(threadno))
        th.start()
        thList.append(th)

    for th in thList:
        th.join()

    endTime = datetime.now()
    logging.info("Started as [" + str(startTime) + "] Completed process at [" + str(endTime) + "]")
    logging.info("Total Time [" + str((endTime - startTime).seconds) + "]")

    TotalRecords = 0
    while not resultQueue.empty():
        TotalRecords += resultQueue.get()
    logging.info("Total Product Records [" + str(TotalRecords) + "]")
        
if __name__ == "__main__":
    main(sys.argv[1:])
