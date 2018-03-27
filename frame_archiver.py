#!/usr/bin/python36

import os
import sys
import pwd
import pathlib
import smtplib
import logging
#import bagit


class FrameDataset:
    def __init__(self, path, uid, gid):
        self.path = path
        self.uid = uid
        self.gid = gid


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

#logger = logging.Logger()
logging.basicConfig(level=logging.DEBUG)

logging.info("starting ...")

frame_dir=pathlib.PosixPath("./TEST-IGNORE").resolve()
archive_dir=pathlib.PosixPath("./TEST-IGNORE/ARCHIVE").resolve()


#sanity checks

#if not os.path.ismount(frame_dir):
#   logging.debug("framedir not mounted: %s", frame_dir)
#   sys.exit()

logging.debug("using %s", frame_dir)
os.chdir(frame_dir)

dirs = []
for d in os.listdir(frame_dir):
    if os.path.isdir(d) and not os.path.islink(d):
        dirs.append(pathlib.PosixPath(d))
print(dirs)

dir_names = []

for d in dirs:
#starts with 20
    if ( d.name == "ARCHIVE"): continue
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
