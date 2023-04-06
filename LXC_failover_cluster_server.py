#!/usr/bin/env python3
#ToDo: Security Hardening


import ClusterTalk
import MessageUnderlay
import random
import json
import lxc
import time
import platform
import psutil

MasterCheckFrequency = 60
ContainerCheckFrequency = 10
CheckDelay = 10

#This is the underlay
class LXC_Failover(ClusterTalk.clusterTalk):
    def __init__(self,Underlay,FailProt=[]):
        self.FailProtectedContainers = FailProt
        super().__init__(Underlay)

    #Local Utility for getting containers running on this machine
    def getRunningLocalContainers(self):
        Running = []
        for i in lxc.list_containers():
            C = lxc.Container(i)
            if C.running:
                Running.append(i)
        return Running
    

    #Get the resource score
    def getLocalResourceLevel(self):
        RunningContainers = len(self.getRunningLocalContainers())
        Memory = psutil.virtual_memory()
        return (Memory.used/Memory.total) * RunningContainers


    #Handles Incoming Messages
    def msgHandler(self):
        while True:
            #Get a message
            IN = self.FetchFunction()
            
            try:
                #Decode the Message into its fields
                Decoded = self._decodeRequest(IN)

                #If it was us that was addressed, act on it
                if (Decoded['DEST'] == platform.node().encode()) or (Decoded['DEST'] == b'*'):
                    #print("Got",Decoded)

                    #Any Message with type above 10 is a response
                    if Decoded['SENDTYPE'] > 10:
                        #Process as Response

                        #Check if we are waiting for the message
                        if Decoded['RESPID'] in self.Responses:
                            #Attach it to the queue
                            self.Responses[Decoded['RESPID']].append(Decoded['FROM'],Decoded['DATA'])

                    else:
                        #Process as Request

                        #Determine actions

                        if Decoded['SENDTYPE'] == 1: #Get Nodes
                            self.sendRequest(platform.node().encode(),11,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 2: #Get Masters
                            if self.isMaster:
                                self.sendRequest(platform.node().encode(),12,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 3: #Get Running Containers
                            self.sendRequest(json.dumps(self.getRunningLocalContainers()).encode(),13,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 4: #Set Required Containers
                            self.FailProtectedContainers = json.loads(Decoded['DATA'].decode())
                            print("Update Container List",self.FailProtectedContainers)
                            self.sendRequest(b'DONE',14,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 5: #Get Running Score, lower = more likely to be assigned more
                            Score = self.getLocalResourceLevel()
                            self.sendRequest(str(Score).encode(),15,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 6: #Start a Container
                            ContainerName = Decoded['DATA'].decode()
                            
                            lxc.Container(ContainerName).start()
                            print("Start Container",ContainerName)
                            self.sendRequest(b'DONE',16,Decoded['RESPID'],Decoded['FROM'])

                        elif Decoded['SENDTYPE'] == 7: #Stop a Container
                            ContainerName = Decoded['DATA'].decode()
                            
                            lxc.Container(ContainerName).stop()
                            print("Stop Container",ContainerName)
                            self.sendRequest(b'DONE',17,Decoded['RESPID'],Decoded['FROM'])
            
            #Error Handling
            except Exception as E:
                print("Receiver Error",E)


    #Here are the requests available

    #Gets running containers on a node, returns a list. If b'*' is the target, an array of all containers in the network is returned.
    def findRunningContainers(self,Node):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',3,RespID,Node)
        time.sleep(1)

        Out = []
        for i in self.getResponses(RespID):
            Out += json.loads(i)
            
        if (Node == platform.node().encode()) or (Node == b'*'):
            Out += self.getRunningLocalContainers()
        return Out
    
    #Same as above, but queries all nodes and returns a dictionary {b'nodeName':[ContainerArrayOfStringNames]}
    def findRunningContainersByNode(self):
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',3,RespID,b'*')
        time.sleep(1)

        Out = {}
        for i in self.getNamedResponses(RespID):
            Out[i['From']] = json.loads(i['Data'])
        Out[platform.node().encode()] = self.getRunningLocalContainers()
        return Out

    #Announce my protected containers to the rest of the cluster
    def broadcastContainers(self,Node):
        RespID = random.randint(1,65534)
        
        self.sendRequest(json.dumps(self.FailProtectedContainers).encode(),4,RespID,Node)
        time.sleep(1)
        
        self.getResponses(RespID)

    #Get the resource score of a node, returns float. b'*' cannot be used here
    def getResourceLevel(self,Node):
        #Return a Resource Score
        #For now it comes from the container count

        #Check if we are querying ourselves
        if Node == platform.node().encode():
            return float(len(self.getRunningLocalContainers()))

        
        #Otherwise get it from another machine
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',5,RespID,Node)
        time.sleep(1)
        
        Response = self.getResponses(RespID)
        if len(Response) == 0:
            return -1

        return float(Response[0].decode())

    #Get the resource scores of the whole cluster and returns a dictionary {b'nodeName':FloatValue}
    def getResourceLevelByNode(self):
        #Return a Resource Score
        #For now it comes from the container count
        Out = {}

        #Check if we are querying ourselves
        Out[platform.node().encode()] = self.getLocalResourceLevel()

        
        #Otherwise get it from another machine
        RespID = random.randint(1,65534)
        
        self.sendRequest(b'',5,RespID,b'*')
        time.sleep(1)
        
        Response = self.getNamedResponses(RespID)
        for i in Response:
            Out[i['From']] = float(i['Data'].decode())
        
        return Out

    #Starts a container on a node. Node is bytes, Container is string. Node cannot be b'*'
    def startContainerOn(self,Node,Container):
        #Start a Container

        #Check if it is us
        if Node == platform.node().encode():
            print("Start Container",Container)
            lxc.Container(Container).start()
        else:
            #Otherwise get it from another machine
            RespID = random.randint(1,65534)
            
            self.sendRequest(Container.encode(),6,RespID,Node)
            time.sleep(1)
            
            self.getResponses(RespID)

    #Stops a container on a node. Node is bytes, Container is string. Node cannot be b'*'
    def stopContainer(self,Node,Container):
        RespID = random.randint(1,65534)
        
        self.sendRequest(Container.encode(),7,RespID,Node)
        time.sleep(1)
        
        self.getResponses(RespID)

    #Override Functions

    #Add a container to the Protected Array
    def enqueueContainer(self,Name):
        self.FailProtectedContainers.append(Name)
        self.broadcastContainers()

    #Delete a container from the Protected Array
    def dequeueContainer(self,Name):
        self.FailProtectedContainers.remove(Name)
        self.broadcastContainers()
    

#This is the Server implementation
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

        #This ensures that we don't move the same container multiple times
        LastMoveTarget = {}
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
                    self.MainServer.broadcastContainers(b'*')

                    print("Audit Containers...")
                    #Get a List of all Running Containers
                    Running = self.MainServer.findRunningContainers(b'*')
                    #Get the Resource Usage Dictionary by Node
                    Resources = self.MainServer.getResourceLevelByNode()
                    #print("Running Containers:",Running)

                    #Check if all containers are running as intended
                    for i in self.MainServer.FailProtectedContainers:
                        #If not, Start it
                        if i not in Running:
                            print("Warning,",i,"Is not Active, starting")

                            #Determine which host is least utilised 
                            BestNode = min(Resources,key=Resources.get) #Magic


                            print("Selected Node",BestNode) 
                            #Start the Container
                            self.MainServer.startContainerOn(BestNode,i)

                            #Refresh the Resource Level
                            Resources[BestNode] = self.MainServer.getResourceLevel(BestNode) 


                            

                    #Check if we can move any containers to lower the overall load
                    MyPeers = list(Resources.keys())
                    
                    HighestLoad = max(Resources.values())
                    LowestLoad = min(Resources.values())
                    HighestName = max(Resources,key=Resources.get)
                    LowestName = min(Resources,key=Resources.get)

                    #Check if we can Move one
                    if HighestLoad > LowestLoad: # ToDo: Fix this Assignment
                        #Clean the No Move Registry
                        for name in list(LastMoveTarget.keys()):
                            if time.time() - LastMoveTarget[name] > 3600:
                                del LastMoveTarget[name]

                        
                        #Move from Highest to Lowest

                        #Get Containers on the Highest
                        HContainers = self.MainServer.findRunningContainers(HighestName) 

                        #Check if we can move it
                        ContainerToMove = False
                        for name in HContainers:
                            if (HighestName not in LastMoveTarget):
                                ContainerToMove = name
                
                        if ContainerToMove != False:
                            print("Moving",ContainerToMove,"From",HighestName,"To",LowestName)
                            self.MainServer.stopContainer(HighestName,ContainerToMove)
                            self.MainServer.startContainerOn(LowestName,ContainerToMove)

                            LastMoveTarget[ContainerToMove] = time.time()
                    
                        



                LastContainerCheck = time.time()

            

            time.sleep(1)



if __name__ == '__main__':
    SRV = Server('FailConfig.json')
    SRV.run()

    