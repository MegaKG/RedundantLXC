#!/usr/bin/env python3
#To Do: Resource Reallocation when new nodes connect
#Clean up
#Error Handling
#Security Hardening


import ClusterTalk
import MessageUnderlay
import random
import json
import lxc
import time
import platform

MasterCheckFrequency = 60
ContainerCheckFrequency = 10
CheckDelay = 10

class LXC_Failover(ClusterTalk.clusterTalk):
    def __init__(self,Underlay,FailProt=[]):
        self.FailProtectedContainers = FailProt
        super().__init__(Underlay)

    def getRunningLocalContainers(self):
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
            #print("Got",Decoded)

            if Decoded['SENDTYPE'] > 10:
                #Process as Response
                if Decoded['RESPID'] in self.Responses:
                    self.Responses[Decoded['RESPID']].append(Decoded['DATA'])

            else:
                #Process as Request
                if Decoded['SENDTYPE'] == 1: #Get Nodes
                    self.sendRequest(platform.node().encode(),11,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 2: #Get Masters
                    if self.isMaster:
                        self.sendRequest(platform.node().encode(),12,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 3: #Get Running Containers
                    self.sendRequest(json.dumps(self.getRunningLocalContainers()).encode(),13,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 4: #Set Containers
                    self.FailProtectedContainers = json.loads(Decoded['DATA'].decode())
                    print("Update Container List",self.FailProtectedContainers)
                    self.sendRequest(b'DONE',14,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 5: #Get Running Score, lower = more likely to be assigned more
                    if Decoded['DATA'].decode() == platform.node():
                        self.sendRequest(str(float(len(self.getRunningLocalContainers()))).encode(),15,Decoded['RESPID'])
                elif Decoded['SENDTYPE'] == 6: #Start a Container
                    Data = json.loads(Decoded['DATA'].decode())
                    if Data['Host'] == platform.node():
                        lxc.Container(Data['Container']).start()
                        print("Start Container",Data['Container'])
                        self.sendRequest(b'DONE',16,Decoded['RESPID'])


    
    def findRunningContainers(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'LISTCNTR',3,RespID)
        time.sleep(1)

        Out = []
        for i in self.getResponses(RespID):
            Out += json.loads(i)
        Out += self.getRunningLocalContainers()
        return Out

    def broadcastContainers(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(json.dumps(self.FailProtectedContainers).encode(),4,RespID)
        time.sleep(1)
        
        self.getResponses(RespID)

    def getResourceLevel(self,Machine):
        #Return a Resource Score
        #For now it comes from the container count

        #Check if we are querying ourselves
        if Machine == platform.node():
            return float(len(self.getRunningLocalContainers()))

        
        #Otherwise get it from another machine
        RespID = random.randint(1,65534)
        
        self.sendRequest(Machine,5,RespID)
        time.sleep(1)
        
        Response = self.getResponses(RespID)
        if len(Response) == 0:
            return -1

        return float(Response[0].decode())


    def startContainerOn(self,Node,Container):
        #Start a Container

        #Check if it is us
        if Node == platform.node():
            print("Start Container",Container)
            lxc.Container(Container).start()
        else:
            #Otherwise get it from another machine
            RespID = random.randint(1,65534)
            
            self.sendRequest(json.dumps({'Host':Node.decode(),'Container':Container}).encode(),6,RespID)
            time.sleep(1)
            
            self.getResponses(RespID)

    def stopContainer(self,Container):
        RespID = random.randint(1,65534)
        
        self.sendRequest(Container.encode(),4,RespID)
        time.sleep(1)
        
        self.getResponses(RespID)



        

        
        

    #Override Functions
    def enqueueContainer(self,Name):
        self.FailProtectedContainers.append(Name)
        self.broadcastContainers()

    def dequeueContainer(self,Name):
        self.FailProtectedContainers.remove(Name)
        self.broadcastContainers()
    


class Server:
    def __init__(self,ConfigFile):
        f = open(ConfigFile,'r')
        self.Config = json.loads(f.read())
        f.close()

        self.Underlay = MessageUnderlay.messageTransport(self.Config['Peers'],self.Config['RecvIP'],self.Config['RecvPort'],self.Config['ForwardTraffic'])
        self.MainServer = LXC_Failover(self.Underlay,self.Config['ProtectedContainers'])


    def _upgradeToMaster(self):
        print("I got a Promotion! Yay!")
        self.MainServer.setMaster(True)


    def _downgradeToNode(self):
        print("I got Demoted! Bugger!")
        self.MainServer.setMaster(False)
        
    def run(self):
        LastMasterCheck = time.time()-MasterCheckFrequency+CheckDelay
        LastContainerCheck = time.time()-ContainerCheckFrequency+CheckDelay
        while True:
            if time.time() - LastMasterCheck > MasterCheckFrequency:
                masters = self.MainServer.findMaster()
                print("Masters Running",masters)

                if len(masters) == 0:
                    self._upgradeToMaster()
                elif len(masters) > 1:
                    self._downgradeToNode()
                else:
                    if self.MainServer.isMaster:
                        print("I am Master")
                    else:
                        print("I Am Node")

                LastMasterCheck = time.time()

            if time.time() - LastContainerCheck > ContainerCheckFrequency:
                MyContainers = self.MainServer.getRunningLocalContainers()
                print("Local Audit")
                for i in MyContainers:
                    if i not in self.MainServer.FailProtectedContainers:
                        #Could Shut it off, but we just notify for now
                        print("Warning,",i,"Is Active but shouldn't be")


                if self.MainServer.isMaster:
                    print("Container List Sync")
                    self.MainServer.broadcastContainers()

                    print("Audit Containers...")
                    Running = self.MainServer.findRunningContainers()
                    #print("Running Containers:",Running)

                    #Check if all containers are running as intended
                    for i in self.MainServer.FailProtectedContainers:
                        #If not, Start it
                        if i not in Running:
                            print("Warning,",i,"Is not Active, starting")

                            #Determine which host is least utilised
                            BestScore = self.MainServer.getResourceLevel(platform.node())
                            BestNode = platform.node()
                            for a in self.MainServer.findNodes():
                                print("Checking node,",a)
                                Score = self.MainServer.getResourceLevel(a)
                                if (Score >= 0) and (Score < BestScore):
                                    BestScore = Score
                                    BestNode = a

                            print("Selected Node",BestNode) 
                            self.MainServer.startContainerOn(BestNode,i)

                    #Check if we can move any containers to lower the overall load
                    MyPeers = self.MainServer.findNodes()
                    PeerLoads = {}
                    HighestLoad = self.MainServer.getResourceLevel(platform.platform())
                    LowestLoad = self.MainServer.getResourceLevel(platform.platform())
                    HighestName = platform.platform()
                    LowestName = platform.platform()

                    for i in MyPeers:
                        Load = self.MainServer.getResourceLevel(i)
                        if Load >= 0:
                            PeerLoads[i] = Load

                            if Load < LowestLoad:
                                LowestLoad = Load
                                LowestName = i
                            
                            if Load > HighestLoad:
                                HighestLoad = Load
                                HighestName = i

                    #Check if we can Move one
                    if HighestLoad - 1 > LowestLoad:
                        #Move from Highest to Lowest
                        ContainerToMove = 'A'
                        self.MainServer.stopContainer(ContainerToMove)
                        self.MainServer.startContainerOn(LowestName,ContainerToMove)
                    
                        



                LastContainerCheck = time.time()

            

            time.sleep(1)



if __name__ == '__main__':
    SRV = Server('FailConfig.json')
    SRV.run()

    