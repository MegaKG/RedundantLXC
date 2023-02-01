#!/usr/bin/env python3
import threading
import time
import struct
import platform
import random

class responseArray:
    def __init__(self):
        self.t = time.time()
        self.a = []

    def append(self,D):
        self.a.append(D)

    def getall(self):
        return self.a

    def cleanup(self):
        if (len(self.a) == 0) and (time.time() - self.t > 10):
            return True
        return False

class clusterTalk:
    def __init__(self,Underlay):
        self.Underlay = Underlay
        self.FetchFunction = self.Underlay.getRow

        #Format: {Response Code: [Responses]}
        self.Responses = {}

        self.Fetcher = threading.Thread(target=self.msgHandler,name="Fetcher")
        self.Fetcher.start()

        self.GarbageCollector = threading.Thread(target=self.gc,name="Garbage Collector")
        self.GarbageCollector.start()

        self.isMaster = False

    def sendRequest(self,D,MSG_Type,ResponseID):
        print("Send",D,"Type",MSG_Type,"ID",ResponseID)
        if ResponseID not in self.Responses:
            self.Responses[ResponseID] = responseArray()

        self.Underlay.sendmsg(struct.pack('!B',MSG_Type) + struct.pack('!H',ResponseID) + D)

    def _decodeRequest(self,D):
        return {
            'SENDTYPE':struct.unpack('!B',D[:1])[0],
            'RESPID':struct.unpack('!H',D[1:3])[0],
            'DATA':D[3:]
        }

    def getResponses(self,ID):
        Out = self.Responses[ID].getall()
        del self.Responses[ID]
        return Out

    def findNodes(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'SCAN',1,RespID)
        time.sleep(1)
        print(self.Responses)

        Out = self.getResponses(RespID)
        Out.append(platform.node().encode())
        return Out

    def setMaster(self,Master):
        self.isMaster = Master

    def findMaster(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'FNDMSTR',2,RespID)
        time.sleep(1)
        print(self.Responses)

        Out = self.getResponses(RespID)
        if self.isMaster:
            Out.append(platform.node().encode())
        return Out

    

        

    def msgHandler(self):
        while True:
            IN = self.FetchFunction()
            
            Decoded = self._decodeRequest(IN)
            print("Got",Decoded)

            if Decoded['SENDTYPE'] > 10:
                #Process as Response
                if Decoded['RESPID'] in self.Responses:
                    self.Responses[Decoded['RESPID']].append(Decoded['DATA'])

            else:
                #Process as Request
                if Decoded['SENDTYPE'] == 1:
                    self.sendRequest(platform.node().encode(),11,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 2:
                    if self.isMaster:
                        self.sendRequest(platform.node().encode(),12,Decoded['RESPID'])
                
    def gc(self):
        while True:
            time.sleep(1)
            for i in list(self.Responses.keys()):
                if self.Responses[i].cleanup():
                    del self.Responses[i]




def test():
    Clients = [('10.0.2.4',5000),('10.0.2.5',5000)]
    import MessageUnderlay
    Underlay = MessageUnderlay.messageTransport(Clients,'0.0.0.0',5000)
    Talk = clusterTalk(Underlay)

    while True:
        time.sleep(1)
        print(Talk.findNodes())

if __name__ == '__main__':
    test()