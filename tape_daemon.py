#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 2018 Daniel R. Rudolph for EMA group at research center caesar, Bonn

"""
Created on Thu Apr 12 16:37:20 2018

@author: daniel
"""

import logging
import time
import os
import subprocess
import pathlib
import collections

from configobj import ConfigObj

from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.protocol import Factory
from twisted.internet import reactor

from util import Error, yes_or_no, script_fail


class TapeError(Error):
    """Errors regarding tape drive or autoloader"""
    def __init__(self, message):
#        super().__init__(message)
        #self.expression = expression
        self.message = message
        logging.debug(message)


def get_tape_labels(changer):
    """read label of tape in drive"""
    try:
        output = subprocess.check_output(["mtx", "-f", changer, "status"])
    except:
        logging.error("error reading mtx status")
    
    tapelabels = collections.OrderedDict()
    #try:
    line = output.splitlines()[1]
    #logging.debug("type of output:",type(output))
    #logging.debug(output)
    logging.debug(line.decode()[-5:])
    #logging.debug(line.decode().split[3])
    #drive_label = line.decode().split[3]
    drive_label = line.decode()[-5:-2]
    #logging.debug(":".join("{:02x}".format(ord(c)) for c in drive_label))
    #logging.debug(type(drive_label))
    if drive_label.strip() == "Empty":
        logging.debug("No Tape in Drive")
        tapelabels['Drive'] = None
    else:
        tapelabels['Drive'] = line.decode().split()[9]
    logging.debug("tapelabels: %s", tapelabels)
        #except:
    #        tapelabel = "ERROR"
    #        logging.error("error decoding tape label")
    for i, line in enumerate(output.splitlines()[2:18]):
        if line.decode().strip()[-5:] == "Empty":
            logging.debug("if branch")
            tapelabels['Slot'+ str(i+1)] = None
        else:
            tapelabels['Slot'+ str(i+1)] = line.decode().strip()[-8:-2]
            
    logging.info(tapelabels)
    return tapelabels


def change_tape(changer, slotnum):
    """change tape in drive"""
    labels = get_tape_labels(changer)
    if labels['Drive'] != None:
        result = subprocess.check_call(["mtx", "-f", changer, "unload"])
        time.sleep(60)
    result = subprocess.check_call(["mtx", "-f", changer, "load", str(slotnum)])
    if result:
        script_fail("tape change failed")
        return result
    logging.debug("MTX: %s", result)
    time.sleep(60)
    #TODO: more checks
    return result


def format_tape(tapedevice, changer, tapeserial, ask_to_confirm=True):
    """format tape in drive with LTFS"""
    #alias mkltfs-loader2='mkltfs --device=/dev/tape/by-id/scsi-3500507631211505c-nst
    # --rules="size=500K/name=indexfile.txt"'
    #TODO: check label for expected label
    result = False
    command_path = "/opt/QUANTUM/ltfs/bin/"
    command = "mkltfs"
    format_rules = "size=500K/name=indexfile.txt"
    logging.debug(format_rules)
    #confirm formatting
    loaded_tapes = get_tape_labels(changer)
    if loaded_tapes['Drive'] is None:
        raise TapeError('Error formatting tape: No tape in drive')
    if ask_to_confirm is True:
        confirm = yes_or_no("FORMAT tape labeled " + loaded_tapes['Drive'] + " to LTFS, serial " + tapeserial + "?")
        if confirm is True:
            result = subprocess.check_call([command_path+command, "-d",
                                            tapedevice, "-r", format_rules, "-s", tapeserial, "-n", tapeserial])
        else:
            print("formatting cancelled by user")
            script_fail(errormsg="formatting cancelled")
    else:
        try:
            logging.info("Formatting tape to LTFS, barcode: %s serial: %s", loaded_tapes['Drive'], tapeserial)
            result = subprocess.check_call([command_path+command, "-d",
                                            tapedevice, "-r", format_rules,
                                            "-s", tapeserial, "-n", tapeserial])
            if result != 0:
                raise TapeError("Error formatting tape")
        except subprocess.CalledProcessError:
            logging.warning("Error formatting tape")
            raise TapeError('Error formatting tape')
    logging.debug(result)
    return result


def mount_tape(tapedevice, mountpoint):
    #alias mountltfs-loader2='/opt/QUANTUM/ltfs/bin/ltfs-singledrive -o devname=/dev/tape/by-id/scsi-3500507631211505c-nst /mnt/ltfs-loader2'
    result = False
    command_path = "/opt/QUANTUM/ltfs/bin/"
    command = "ltfs-singledrive" + ' -o devname=' + tapedevice + ' ' + mountpoint
    logging.debug("mount command: %s", command)
    #check if mountpoint exits
    mountpoint = pathlib.Path(mountpoint)
    logging.debug("test")
    if not mountpoint.is_dir():
        logging.error("Mount directory not found")
        raise TapeError('Mount directory not found')
    #check if mountpoint is not already mounted
    logging.debug("test ismount")
    #if os.path.ismount(mountpoint): python 3.6
    if os.path.ismount(mountpoint.as_posix()): #python 3.4
        raise TapeError('Mount directory already mounted ?')
    try:
        result = subprocess.check_call(command_path+command, shell=True)
        logging.debug("mount result: %s", result)
    except subprocess.CalledProcessError:
        raise TapeError('Error mounting tape')
    #result.c
    #check if mountpoint is mounted
    if os.path.ismount(mountpoint.as_posix()):
        return mountpoint
    else:
        raise TapeError('Error while mounting tape')


def unmount_tape(mountpoint):
    if not os.path.ismount(mountpoint):
        raise TapeError('Mount directory not mounted ?')
    command = "umount " + mountpoint
    try:
        result = subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError:
        raise TapeError('Error mounting tape')

def get_tape_free(mountpoint):
    """return free space on tape (bytes)"""
    if not os.path.ismount(mountpoint):
        raise TapeError('tape not mounted ?')
    try:
        stats = os.statvfs(mountpoint)
    except:
        raise TapeError('can not get free space on mounted tape')
    free_space = stats[0] * stats[3]
    return free_space


class TapeServer(Protocol):
    def __init__(self, factory):
        self.factory = factory
        logger.debug("Starting tape_daemon")
        config = ConfigObj('/etc/tape_daemon.conf')
    
        self.TAPEDEVICE = config['tapedevice']
        self.CHANGERDEVICE = config['changerdevice']
        self.LTFSMOUNTPOINT = config['ltfsmountpoint']

    def connectionMade(self):
        self.factory.numProtocols = self.factory.numProtocols + 1
        self.transport.write(
            "Tape Daemon:\n")

    def dataReceived(self, data):
        pass

class TapeServerFactory(Factory):
    def buildProtocol(self, addr):
        return TapeServer()
        
if __name__ == '__main__':
    
    logger = logging.getLogger('tape daemon')
    #logger = logging.Logger('root':{'handlers':('console', 'file'), 'level':'DEBUG'})
    #logger = logging.Logger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)

    logging.debug("creating endpoint")
    endpoint = TCP4ServerEndpoint(reactor, 8601)
    endpoint.listen(TapeServerFactory())
    logging.debug("running reactor")
    reactor.run()
