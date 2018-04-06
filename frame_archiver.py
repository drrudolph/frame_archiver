#!/usr/bin/python36

import os
import sys
import time
import pwd
import pathlib
import email
import collections
from smtplib import SMTP
import subprocess
import logging

import bagit


TAPEDEVICE = "/dev/tape/by-id/scsi-3500507631211505c-nst"
CHANGERDEVICE = "/dev/tape/by-id/scsi-3500e09efff107510"
LTFSMOUNTPOINT = "/mnt/ltfs-archive/"
INDEXFILESDIR = "/root/TAPES/"
MAILADDRESS = ""



#class FrameDataset:
#    def __init__(self, path):
#        self.path = path
#        self.uid = 0
#        self.gid = 0
#        self.tapes = []

Testdata = collections.namedtuple('FrameDataset',
                                  ['path', 'username', 'uid', 'gid',
                                   'bagged', 'tapes'])

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class TapeError(Error):
    """Errors regarding tape drive or autoloader"""
    def __init__(self, message):
#        super().__init__(message)
        #self.expression = expression
        self.message = message


def adjust_dir_permissions(path):
    '''change user and permissions according to username in dir name'''
    username = path.name.split('-')[3]
    logging.debug("username: %s", username)
    uid = pwd.getpwnam(username).pw_uid
    gid = pwd.getpwnam(username).pw_gid
    logging.debug("uid: %s", uid)
    logging.debug("gid: %s", gid)
    #https://stackoverflow.com/questions/2853723/what-is-the-python-way-for-recursively-setting-file-permissions
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chown(d, uid, gid)
            os.chmod(d, 0o750)
        for file in files:
            fname = os.path.join(root, file)
            os.chown(fname, uid, gid)
            os.chmod(fname, 0o640)



def yes_or_no(question):
    reply = str(input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
#    else:
#        return yes_or_no("please enter y or n")


def mail_message(message):
    """mail error messages to admin"""
    msg = email.message_from_string(message)
    msg['Subject'] = 'frame archiver: '
    msg['From'] = 'python@ema3013'
    msg['To'] = MAILADDRESS
    server = SMTP('localhost')
    server.sendmail("From: ", "To: ", message)
    server.quit()


def script_fail(errormsg="unknown error"):
    """"""
    print("script failed with error:", errormsg)


def lock_directory(path):
    #TODO
    pass


def unlock_directory(path):
    #TODO
    pass


def get_tape_label(changer=CHANGERDEVICE):
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


def format_tape(tape, ask_to_confirm=True):
    """format tape in drive with LTFS"""
    #alias mkltfs-loader2='mkltfs --device=/dev/tape/by-id/scsi-3500507631211505c-nst
    # --rules="size=500K/name=indexfile.txt"'
    result = False
    command_path = "/opt/QUANTUM/ltfs/bin/"
    command = "mkltfs"
    format_rules = "size=500K/name=indexfile.txt"
    print(format_rules)
    #confirm formatting
    loaded_tape = get_tape_label()
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


def create_bag(dataset):
    info = {'Authors': dataset.username}
    bag = bagit.make_bag(dataset.path, info)
    bag.save(manifests=True)
    dataset.bagged = True

def split_bag(bag):
    pass


def copy_to_tape(dataset):
    pass


def validate_tape(path):
    pass



if __name__ == "__main__":

    #logger = logging.Logger()
    logging.basicConfig(level=logging.DEBUG)

    logging.info("starting ...")

    FRAME_DIR = pathlib.PosixPath("./TEST-IGNORE").resolve()
    ARCHIVE_DIR = pathlib.PosixPath("./TEST-IGNORE/ARCHIVE").resolve()

    #sanity checks
    #if not os.path.ismount(FRAME_DIR):
    #   logging.debug("framedir not mounted: %s", FRAME_DIR)
    #   sys.exit()

    logging.debug("using %s", FRAME_DIR)
    os.chdir(FRAME_DIR)

    dirs = []
    for d in os.listdir(FRAME_DIR):
        if os.path.isdir(d) and not os.path.islink(d):
            dirs.append(pathlib.PosixPath(d))
    print(dirs)

    dir_names = []

    for d in dirs:
        #starts with 20
        if d.name == "ARCHIVE":
            continue
        elif ( d.name[:2] == "20" and
               d.name[4] == "-" and
               d.name[10] == "-"):
            dir_names.append(d)
        else:
            logging.warn("found malformed directory name:%s", d)
    print(dir_names)

    for path in dir_names:
        adjust_dir_permissions(path)

    #test if metadata file is present:
    