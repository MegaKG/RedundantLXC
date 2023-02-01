#!/usr/bin/env python3
import ClusterTalk
import MessageUnderlay
import random
import json
import lxc
import time

class LXC_Failover(ClusterTalk.clusterTalk):
    def _getRunningLocalContainers(self):
        Running = []
        for i in lxc.list_containers():
            C = lxc.Container(i)
            if C.running:
                Running.append(i)
        return Running

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
                elif Decoded['SENDTYPE'] == 3:
                    self.sendRequest(json.dumps(self._getRunningLocalContainers()).encode(),13,Decoded['RESPID'])


    
    def findRunningContainers(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'LISTCNTR',3,RespID)
        time.sleep(1)
        print(self.Responses)

        Out = json.loads(self.getResponses(RespID))
        Out += self._getRunningLocalContainers()
        return Out

    


class Server:
    def __init__(self,ConfigFile):
        f = open(ConfigFile,'r')
        self.Config = json.loads(f.read())
        f.close()

        self.Underlay = MessageUnderlay.messageTransport(self.Config)
        self.MainServer = LXC_Failover(self.Underlay)

    def _upgradeToMaster(self):
        print("I got a Promotion! Yay!")
        pass


    def _downgradeToNode(self):
        print("I got Demoted! Bugger!")
        pass
        
    def run(self):
        LastMasterCheck = time.time()
        while True:
            if time.time() - LastMasterCheck > 60:
                masters = self.MainServer.findMaster()

                if len(masters) == 0:
                    self._upgradeToMaster()
                elif len(masters) > 1:
                    self._downgradeToNode()
                    
                LastMasterCheck = time.time()



if __name__ == '__main__':
    SRV = Server('FailConfig.json')
    SRV.run()

    