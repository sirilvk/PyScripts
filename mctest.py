#!/usr/bin/python3
# To trigger remotely on a server try (sending) : ssh <remote host> onload python -u - -send testmc -port 1234 < mctest.py
# To trigger remotely on a server try (receiving) : ssh -q <remote host> onload python -u - -receive < mctest.py

import time
from datetime import datetime
import socket
import sys
import struct
import argparse
import logging
import pickle
import fcntl
import signal

def handler(signum, frame):
  print("Times up! exiting..")
  exit(0)

def get_ip_address(ifname):
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24]) # 0x8915 - SIOCGIFADDR

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.WARNING)
logger = logging.getLogger('mctest')
interface = get_ip_address(b'eth0')
group = '239.253.1.105'
mport = 1234
mttl = 10
message = 'multicast test tool'
parser = argparse.ArgumentParser(description='Multicast send/receive test tool')
parser.add_argument("-send", metavar="string", help="Send a message", type=str)
parser.add_argument("-receive", help="Receive message from group", action="store_true")
parser.add_argument("-interface", metavar="Interface (defaults to eth0 ip)", type=str, default=get_ip_address(b'eth0'))
parser.add_argument("-group", metavar="Multicast group", type=str)
parser.add_argument("-port", metavar="UDP Port", help="UDP Port to receive on (default=1234)", default=1234)
parser.add_argument("-ttl", metavar='int', help="Multicast TTL (default 10)", type=int)
parser.add_argument("-v", help="Verbose output", action="store_true")
args = parser.parse_args()

def receiver(mgroup):
  'Receive on a multicast group'
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  # windows workaround
  try:
    sock.bind(mgroup, mport)
  except socket.error:
    sock.bind('', mport)
  mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton(interface))
  sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
  print('Listening on ' + mgroup + ' port ' + str(mport) + ' on interface ' + interface)
  while True:
    (data, address) = sock.recvfrom(12000)
    # try to unpickle log record from a Datagramhandler
    try:
      lrtxt = pickle.loads(data[4:])
      lr = logging.makeLogRecord(lrtxt)
      logger.handle(lr)
    except Exception as e:
      print('Exception ', str(e))
      print(socket.gethostname() + ': Received on ' + mgroup + ' from ' + address[0] + ' from port ' + str(address[1]) + ' on [' + interface + ']: ' + data)
      exit(0)


def sender(mgroup):
  'Send to a multicast group'
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  ttl_bin = struct.pack('@i', mttl)
  sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)

  host = socket.inet_aton(interface)
  sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, host)

  while True:
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mcast_msg = 'msg from [' + socket.gethostname() + ']:' + message + ' : ' + time_now
    print("Broadcasting to " + mgroup + "(TTL " + str(mttl) + "): " + mcast_msg)
    sock.sendto(mcast_msg, (mgroup, mport))
    time.sleep(1)

signal.signal(signal.SIGALRM, handler)
signal.alarm(600)

if args.interface:
  interface = args.interface
if args.group:
  group = args.group
if args.ttl:
  mttl = int(args.ttl)
if args.port:
  mport = int(args.port)
if args.send:
  message = args.send
  sender(group)
elif args.receive:
  receiver(group)
else:
  parser.print_help()
