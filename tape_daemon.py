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


def get_tape_label(changer):
    """read label of tape in drive"""
    tapelabel = "ERROR"
    output = subprocess.check_output(["mtx", "-f", changer, "status"])
    line = output.splitlines()[1]

    #print("type of output:",type(output))
    #print(output)
    #print(line)

    tapelabel = line.decode().split()[9]
    #print(tapelabel)
    return tapelabel


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
    
    config = ConfigObj('/etc/tape_daemon.conf')

    TAPEDEVICE = config['tapedevice']
    CHANGERDEVICE = config['changerdevice']
    LTFSMOUNTPOINT = config['ltfsmountpoint']
    iq = IterableQueue()
    
    consumer_endpoint = iq.get_consumer()
    p = Process(target=consumer_func, args=(consumer_endpoint, 0))
    
    iq.close()