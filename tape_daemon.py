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

from multiprocessing import Process
from iterable_queue import IterableQueue
from configobj import ConfigObj

from util import Error, yes_or_no, script_fail


class TapeError(Error):
    """Errors regarding tape drive or autoloader"""
    def __init__(self, message):
#        super().__init__(message)
        #self.expression = expression
        self.message = message


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
    drive_label = line.decode()[-5:]
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

get_tape_labels(CHANGER)


def change_tape(slotnum):
    """change tape in drive"""
    result = subprocess.check_call(["mtx", "-f", CHANGERDEVICE, "load", str(slotnum)])
    if result:
        script_fail("tape change failed")
        return result
    time.sleep(60)
    #TODO: test if successful
    return result


def format_tape(tape, changer, ask_to_confirm=True):
    """format tape in drive with LTFS"""
    #alias mkltfs-loader2='mkltfs --device=/dev/tape/by-id/scsi-3500507631211505c-nst
    # --rules="size=500K/name=indexfile.txt"'
    #TODO: check label for expected label
    result = False
    command_path = "/opt/QUANTUM/ltfs/bin/"
    command = "mkltfs"
    format_rules = "size=500K/name=indexfile.txt"
    print(format_rules)
    #confirm formatting
    loaded_tape = get_tape_label(changer)
    if ask_to_confirm is True:
        confirm = yes_or_no("FORMAT tape labeled " + loaded_tape + " to LTFS, serial " + tape + "?")
        if confirm is True:
            result = subprocess.check_call([command_path+command, "-d",
                                            TAPEDEVICE, "-r", format_rules, "-s", tape, "-n", tape])
        else:
            print("formatting cancelled by user")
            script_fail(errormsg="formatting cancelled")
    else:
        try:
            result = subprocess.check_call([command_path+command, "-d",
                                            TAPEDEVICE, "-r", format_rules, "-s", tape, "-n", tape])
        except subprocess.CalledProcessError:
            raise TapeError('Error formatting tape')
    print(result)
    return result


def mount_tape(mountpoint):
    #alias mountltfs-loader2='/opt/QUANTUM/ltfs/bin/ltfs-singledrive -o devname=/dev/tape/by-id/scsi-3500507631211505c-nst /mnt/ltfs-loader2'
    result = False
    command_path = "/opt/QUANTUM/ltfs/bin/"
    command = "ltfs-singledrive" + ' -o devname=' + TAPEDEVICE + ' ' + mountpoint
    #check if mountpoint exits
    if not mountpoint.is_dir():
        raise TapeError('Mount directory not found')
    #check if mountpoint is not already mounted
    if os.path.ismount(mountpoint):
        raise TapeError('Mount directory already mounted ?')
    try:
        result = subprocess.check_call(command_path+command, shell=True)
    except subprocess.CalledProcessError:
        raise TapeError('Error mounting tape')
    #result.c
    #check if mountpoint is mounted
    if os.path.ismount(mountpoint):
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


if __name__ == 'main':

    NUM_DRIVES = 1
    NUM_PRODUCERS = 4
    
    logger = logging.Logger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    logger.debug("Starting tape_daemon")    
    config = ConfigObj('/etc/tape_daemon.conf')

    TAPEDEVICE = config['tapedevice']
    CHANGERDEVICE = config['changerdevice']
    LTFSMOUNTPOINT = config['ltfsmountpoint']
    iq = IterableQueue()
    
    consumer_endpoint = iq.get_consumer()
    p = Process(target=consumer_func, args=(consumer_endpoint, 0))
    
    iq.close()
    test = get_tape_label(CHANGERDEVICE)
    print(test)