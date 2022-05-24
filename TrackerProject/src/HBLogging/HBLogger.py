import os
import time
from pprint import pprint as pp
import logging


class HBLogger:
    def __init__(self, src, logsPath, print_output=True):
        self.src = src
        self.print_output = print_output
        self.logsPath = logsPath

    def log(self, msg,  level="INFO"):
        getLevel = {"CRITICAL":logging.CRITICAL,
                    "ERROR":logging.ERROR, 
                    "WARNING":logging.WARNING,
                    "INFO":logging.INFO, 
                    "DEBUG":logging.DEBUG, 
                    "NOTSET":logging.NOTSET
                    }   

        FORMAT = "{} - {}: {}".format(time.asctime(), self.src, msg)
        if not os.path.exists(self.logsPath):
            os.mkdir(self.logsPath)
        srcLogFolder = os.path.join(self.logsPath, self.src)
        if not os.path.exists(srcLogFolder):
            os.mkdir(srcLogFolder)
        timeStamp = str(int(time.time()))
        logLevel = getLevel[level]
        srcLogPath = os.path.join(srcLogFolder, "{}.log".format(timeStamp))
        logging.basicConfig(filename=srcLogPath, level=logLevel, format=FORMAT)
        logger = logging.getLogger("{}".format(self.src))
        logger.log(logLevel, msg)
        if self.print_output:
            print(FORMAT)



# a = HBLogger('test')
# a.log('msg')