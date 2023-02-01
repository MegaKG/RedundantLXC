#!/usr/bin/env python3
import UDPstreams2 as udp
import threading
import random
import struct
import time

class messageTransport:
    #PeerList: list of Tuples [('IP',Port)]

    def startPeer(self,IP,Port):
        print("Starting Poller",IP,':',Port)
        self.Senders.append(udp.udpsend(IP,Port))

    def insblock(self,V):
        self.MSGBlock = self.MSGBlock[1:]
        self.MSGBlock.append(V)


    def sendmsg(self,V,ID=None):
        if ID is None:
            ID = random.randint(1,65534)

        #print("Send MSG",ID,V)

        self.insblock(ID)
        for i in self.Senders:
            i.senddat(struct.pack('!H',self.ID) + struct.pack('!H',ID) + V)

        
        

    def getter(self):
        print("Start Receiver")
        while True:
            D = self.con.getdat()
            ID = struct.unpack('!H',D[:2])[0]
            MID = struct.unpack('!H',D[2:4])[0]
            #print("MSG: ",MID,D[4:],'from',ID)
            if (MID not in self.MSGBlock) and (ID != self.ID):
                #print(MID,"Forward")
                self.InBuffer.append(D[4:])
                self.insblock(MID)

                if self.Forward:
                    self.sendmsg(D[4:],MID)
            else:
                #print(MID,"Block")
                pass


    def __init__(self,PeerList,RecvAddr,RecvPort,Forward):
        self.InBuffer = []
        self.Senders = []
        self.MSGBlock = [0]*128
        self.Forward = Forward

        self.ID = random.randint(1,65534)

        for i in PeerList:
            self.startPeer(i[0],i[1])

        print("Receiver",RecvAddr,':',RecvPort)
        self.con = udp.udpget(RecvAddr,RecvPort)
        self.RecvThread = threading.Thread(target=self.getter,name="Receiver")
        self.RecvThread.start()

    def getBuffer(self):
        return self.InBuffer
    
    def getRow(self):
        while len(self.InBuffer) == 0:
            time.sleep(0.1)
        Out = self.InBuffer[0]
        self.InBuffer = self.InBuffer[1:]
        return Out
    

        
