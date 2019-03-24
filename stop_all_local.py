from psutil import process_iter
from signal import SIGABRT

for proc in process_iter():
    for conns in proc.connections(kind='inet'):
        if conns.laddr.port >= 50137 and conns.laddr.port <= 50147:
            proc.send_signal(SIGABRT)