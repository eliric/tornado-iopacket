# tornado-iopacket
Packet(udp,raw_socket etc) based io support for tornado<br>
Coroutine is supported as well<br>

# Usage:
<br>

```python
from socket import *
import iopacket
import tornado.gen

_sock = socket(AF_INET, SOCK_DGRAM)
_sock.bind(('localhost', 18888))
sock = iopacket.IOPacket(_sock, max_packet_size=4096)


@tornado.gen.coroutine
def write(data, dest_ip, port):
    yield sock.write(packet , (dest_ip,port))
    
@tornado.gen.coroutine
def read():
    data, address = yield sock.read()
```
