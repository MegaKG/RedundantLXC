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

    def append(self,From,D):
        self.a.append({'From':From,'Data':D})

    def getAll(self):
        Out = []
        for i in self.a:
            Out.append(i['Data'])
        return Out
        
    
    def getNamedAll(self):
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

    def sendRequest(self,D,MSG_Type,ResponseID,Destination):
        #print("Send",D,"Type",MSG_Type,"ID",ResponseID)
        if ResponseID not in self.Responses:
            self.Responses[ResponseID] = responseArray()

        self.Underlay.sendmsg(struct.pack('!B',MSG_Type) + struct.pack('!H',ResponseID) + struct.pack('!B',len(Destination)) + Destination + struct.pack('!B',len(platform.node())) + platform.node().encode() + struct.pack('!H',len(D)) + D)

    def _decodeRequest(self,D):
        DestLength = struct.unpack('!B',D[3:4])[0]
        FromLength = struct.unpack('!B',D[4+DestLength:5+DestLength])[0]
        DataLength = struct.unpack('!H',D[5+FromLength+DestLength:7+FromLength+DestLength])[0]
        return {
            'SENDTYPE':struct.unpack('!B',D[:1])[0],
            'RESPID':struct.unpack('!H',D[1:3])[0],
            'DEST':D[4:4+DestLength],
            'FROM':D[5+DestLength:5+DestLength+FromLength],
            'DATA':D[7+FromLength+DestLength:7+FromLength+DestLength+DataLength]
        }

    def getResponses(self,ID):
        if ID in self.Responses:
            Out = self.Responses[ID].getAll()
            del self.Responses[ID]
        else:
            Out = []
        return Out
    
    def getNamedResponses(self,ID):
        if ID in self.Responses:
            Out = self.Responses[ID].getNamedAll()
            del self.Responses[ID]
        else:
            Out = []
        return Out

    def findNodes(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',1,RespID,b'*')
        time.sleep(1)

        Out = self.getResponses(RespID)
        Out.append(platform.node().encode())
        return Out

    def setMaster(self,Master):
        self.isMaster = Master

    def findMaster(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',2,RespID,b'*')
        time.sleep(1)

        Out = self.getResponses(RespID)
        if self.isMaster:
            Out.append(platform.node().encode())
        return Out

    

        

    def msgHandler(self):
        while True:
            IN = self.FetchFunction()
            
            Decoded = self._decodeRequest(IN)
            if (Decoded['DEST'] == platform.node().encode()) or (Decoded['DEST'] == b'*'):
                #print("Got",Decoded)

                if Decoded['SENDTYPE'] > 10:
                    #Process as Response
                    if Decoded['RESPID'] in self.Responses:
                        self.Responses[Decoded['RESPID']].append(Decoded['FROM'],Decoded['DATA'])

                else:
                    #Process as Request
                    if Decoded['SENDTYPE'] == 1:
                        self.sendRequest(platform.node().encode(),11,Decoded['RESPID'],Decoded['FROM'])
                    elif Decoded['SENDTYPE'] == 2:
                        if self.isMaster:
                            self.sendRequest(platform.node().encode(),12,Decoded['RESPID'],Decoded['FROM'])
                
    def gc(self):
        while True:
            time.sleep(1)
            for i in list(self.Responses.keys()):
                if self.Responses[i].cleanup():
                    del self.Responses[i]




def test():
    Clients = [('10.0.2.4',5000),('10.0.2.5',5000)]
    import MessageUnderlay
    Underlay = MessageUnderlay.messageTransport(Clients,'0.0.0.0',5000,True)
    Talk = clusterTalk(Underlay)

    while True:
        time.sleep(1)
        print(Talk.findNodes())

if __name__ == '__main__':
    test()