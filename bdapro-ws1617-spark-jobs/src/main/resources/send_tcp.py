import socket
import time
import sys
import thread

# Sends a N lines every M seconds



def on_new_client(clientsocket):
    lineNumber = 0
    with open(fileInputSource) as f:
        while True:
            for line in f:
                clientsocket.send(line)

                lineNumber += 1
                if lineNumber is nrLines:
                    time.sleep(sleepTime)
                    lineNumber = 0



try:
    sys.argv[1]
    int(sys.argv[2])
    float(sys.argv[3])
    sys.argv[4]
except Exception as e:
    print "Usage: python send_tcp.py <sparkHost> <nrLinesInBatch> <sleepMS> <fileInputSource>"
    print e
    sys.exit(1)

sparkHost = sys.argv[1]
nrLines = int(sys.argv[2])
sleepTime = float(sys.argv[3])
fileInputSource = sys.argv[4]

TCP_IP = ''
TCP_PORT = 9999
BUFFER_SIZE = 1024  # Normally 1024, but we want fast response

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

while True:
    c, addr = s.accept()
    thread.start_new_thread(on_new_client, (c,))

# meh ???
s.close()
