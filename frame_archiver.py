#!/usr/bin/python36/
# 2018 Daniel R. Rudolph for EMA group at research center caesar, Bonn

import os
import argparse
import sys
import time
import datetime
import pathlib
import collections
import logging
from pathlib import Path
import shutil
import pwd
import io
import hashlib
import queue
import shelve

import inotify
import bagit
from configobj import ConfigObj
import inotify.adapters
from celery import Celery
from daemonize import Daemonize

from util import get_dir_size

#Testdata = collections.namedtuple('FrameDataset',
#                                  ['path', 'username', 'uid', 'gid',
#                                   'bagged', 'tapes'])

class FrameDataset():

    def __init__(self, path=None, username=None, uid=None, gid=None, 
              bagged=None, tapes=None):
        self.path = path
        self.username = username
        self.uid = uid
        self.gid = gid
        self.bagged = bagged
        self.tapes = tapes


def get_userdata_from_path(pathname):
    logging.debug("get_userdata_from_path: %s", pathname)
    try:
        username = pathname.name.split('-')[3]
    except IndexError: 
        username = None
        logging.warning("can't get username from path %s", pathname.name )
        return (None, None, None)
    try:
        if username != None:  uid = pwd.getpwnam(username).pw_uid
    except (IndexError, KeyError):
        uid = None
        logging.warning("can't get uid from path %s", pathname )
    try:
        if username != None:  gid = pwd.getpwnam(username).pw_gid
    except (IndexError, KeyError):
        gid = None
        logging.warning("can't get gid from path %s", pathname )
    logging.debug("uid: %s", uid)
    logging.debug("gid: %s", gid)
    logging.debug("username: %s", username)
    return (username, uid, gid)
       

def lock_directory(path):
    #TODO
    pass


def unlock_directory(path):
    #TODO
    pass


def adjust_dir_permissions(path):
    '''change user and permissions according to username in dir name'''
    username, uid, gid = get_userdata_from_path(path)
    #username = path.name.split('-')[3]
    #uid = pwd.getpwnam(username).pw_uid
    #gid = pwd.getpwnam(username).pw_gid
    #https://stackoverflow.com/questions/2853723/what-is-the-python-way-for-recursively-setting-file-permissions
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chown(d, uid, gid)
            os.chmod(d, 0o750)
        for file in files:
            fname = os.path.join(root, file)
            os.chown(fname, uid, gid)
            os.chmod(fname, 0o640)


def create_bag(dataset):
    info = {'Authors': dataset.username}
    bag = bagit.make_bag(dataset.path, info)
    bag.save(manifests=True)
    dataset.bagged = True


class HashTransparentFile():
    """based on https://stackoverflow.com/questions/14014854/python-on-the-fly-md5-as-one-reads-a-stream"""
    """stream file from remote fs, calculate checksum on the fly"""
    def __init__(self, source, bufsize, hashlist):
        self.hashlist = hashlist
        self._sigmd5 = hashlib.md5()
        self._sigsha1 = hashlib.sha1()
        self._sigsha256 = hashlib.sha256()
        self._sigsha512 = hashlib.sha512()
        self._source = source

    def read(self, bufsize):
        # we ignore the buffer size, just use the `.next()` value in the source iterator
        try:
            line = self._source.read()
            
            #TODO: only calculate requested
            self._sigmd5.update(line)
            self._sigsha1.update(line)
            self._sigsha256.update(line)
            self._sigsha512.update(line)
            
            return line
        except StopIteration:
            return b''

    def hexdigest(self):
        hashes = []
        if 'md5' in self.hashlist:
            hashes.append({'md5': self._sigmd5.hexdigest()})
        if 'sha1' in self.hashlist:
            hashes.append({'sha1': self._sigsha1.hexdigest()})
        if 'sha256' in self.hashlist:
            hashes.append({'sha256': self._sigsha256.hexdigest()})
        if 'sha512' in self.hashlist:
            hashes.append({'sha512': self._sigsha512.hexdigest()})
        return hashes


def copy_dataset(dataset, dstpath, chunksize, hashlist, retries=1):
    for file in dataset.list:
        #srcfile = 
        #dstfile = 
        hashed = HashTransparentFile(srcfile, chunksize, hashlist)
        shutil.copyfileobj(hashed, dstfile, chunksize)
        #compare hashes:
        #retransmit if mismatch


start=time.time()
testfile=open('/home/daniel/d8323ewghs_labfolder_mpi_v_1.16.3.zip', 'br')
dstfile=open('/tmp/dst-test', 'bw')
shutil.copyfileobj(testfile, dstfile, 400000000)
testfile.close()
dstfile.close()
end=time.time()
t1=end-start


start=time.time()
testfile=open('/home/daniel/d8323ewghs_labfolder_mpi_v_1.16.3.zip', 'br')
dstfile=open('/tmp/dst-test', 'bw')
hashed = HashTransparentFile(testfile, 'sha256')
shutil.copyfileobj(hashed, dstfile, 400000000)
testfile.close()
dstfile.close()
end=time.time()
t2=end-start


#def process_dir(path, msize, move=False):
#    """split dir into several dirs with msize (MB), simple splitting,
#        returns number of chunks"""
#    msize = msize*1024*1024
#    #total is small enough
#    if os.path.getsize(path) < msize:
#            return 1
#    else:
#        #find all subdirs larger than msize
#        largedirs = []
#        smalldirs = []
#        bags = []
#        current_size = 0
#        current_bag = []
#        for subdirs in os.scandir(path):
#            for entry in subdirs:
#                if entry.is_dir():
#                    entrysize = os.path.getsize(entry)
#                    if entrysize + current_size <= msize:
#                        current_bag.append(entry)
#                        current_size += os.path.getsize(entry)
#                    else: 
#                        #bag full, new bag
#                        bags.append(current_bag)
#                        current_bag = [entry]
#                        current_size = entrysize
#
#                        
##                    try:
##                        if os.path.getsize(subdir) > msize:
##                            largedirs.append(subdir)
##                        else:
##                           smalldirs.append(subdir) 
##                    except:
##                        pass#TODO
#
#            #case: only one directory too large, pack files
#            if len(largedirs) == 1:
#                for entry in os.scandir(largedirs[0]):
#                    #process_dir rekursiv ?
#            #case: several subdirs, at least one larger than msize
#            elif len(largedirs) > 1:
#                
#            #case: several subdirs, all smaller than msize, pack subdirs
#            else:
#                bins = binpacking.to_constant_volume(smalldirs, msize)



def split_bag(bag, msize, copy=True, mode='Simple'):
    """split bag into several bags with max size msize (MB), simple splitting,
        returns list of bags
        """
    #msize = msize*1024*1024
    #total is small enough
    if get_dir_size(bag.path) <= msize:
        bags = [bag]
    else:
        if mode == 'Simple':
            splits = []
            current_size = 0
            current_bag = []
            orig_path = Path(bag.path)
            
            for item in list(bag.entries.keys()):
                itempath = Path(item)
                itemsize = os.path.getsize(orig_path.joinpath(itempath))
                #logger.debug(itempath, itemsize)
                print("itempath, itemsize:", itempath, itemsize)

                if (itemsize + current_size) <= msize:
                    print("current_size before", current_size)
                    print("appending ", item)
                    current_bag.append(itempath)
                    current_size += itemsize
                    print("current_size after", current_size)

                    #print("current_bag: ", current_bag)
                else: 
                    #bag full, new bag
                    #print("bag full, current_bag: ", current_bag)
                    bag_full = True
                    splits.append(current_bag)
                    current_bag = [itempath]
                    current_size = itemsize
                    
            #print("end, current_bag: ", current_bag)
            if not bag_full: splits.append(current_bag)
        elif mode == 'Packing':
            raise NotImplementedError

        #TODO: check number of bags

        #copy items to bags
        print("splits: ",splits)
        for split in splits:
            #print("split:", split)
            for i, file in enumerate(split):
                print("i: %s file: %s", i, file)
                suffix = '_' + chr(i+97)
                #print(suffix)
                new_bag_path = Path(orig_path.name + suffix)
                #print(new_bag_path)
                new_file_path = new_bag_path.joinpath(file.parent)
                pathlib.Path.mkdir(new_file_path, parents=True, exist_ok=True)
                #print("src:", orig_path.joinpath(file))
                #print("dst:", new_bag_path.joinpath(file))
                shutil.copy2(orig_path.joinpath(file), new_bag_path.joinpath(file))

        bags = splits

    return bags



def copy_to_tape(bag):
    pass


def validate_tape(path):
    pass


#@Celery.task(name='workers.watch_dir')
#def watch_dir(path):atasets
#    i = inotify.adapters.Inotify(path)
#    #try:
#    for event in i.event_gen():
#        if event is not None:
#            (header, type_names, watch_path, filename) = event
#            logger.info("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s " "WATCH-PATH=[%s] FILENAME=[%s]",
#            header.wd, header.mask, header.cookie, header.len, type_names,
#            watch_path.decode('utf-8'), filename.decode('utf-8'))
#            print(event)
#            #adjust_dir_permissions(event)



def archiver():
    
    logger.debug("Starting Archiver")

    #sanity checks
    #if not os.path.ismount(FRAME_DIR):
    #   logging.debug("framedir not mounted: %s", FRAME_DIR)
    #   sys.exit()

    logging.debug("using %s as frame directory", FRAME_DIR)
    os.chdir(FRAME_DIR)


    logging.debug(datetime.datetime.now().isoformat())

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
        elif (d.name[:2] == "20" and
              d.name[4] == "-" and
              d.name[10] == "-"):
            dir_names.append(d)
        else:
            logging.warn("found malformed directory name:%s", d)
    print(dir_names)
    for path in dir_names:
        adjust_dir_permissions(path)

    #        logging.debug(datetime.datetime.now().isoformat())
    #test if metadata file is present:

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='EMA Frame Archiver')
    parser.add_argument('-d', '--daemonize', action='store_true', 
                        help="run as daemon")
    args = parser.parse_args()
    
    app = Celery('watch_dir', backend='rpc://', broker='pyamqp://')
    app.config_from_object('celeryconfig')
    
    #logger = logging.getLogger(__name__)
    logger = logging.Logger(__name__)
    logging.basicConfig(level=logging.DEBUG)
    logger.propagate = False
    fh = logging.FileHandler("/tmp/test.log", "w")
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    keep_fds = [fh.stream.fileno()]
    logging.info("starting ...")

    config = ConfigObj('/etc/frame_archiver.conf')
    #config['DEFAULT'] 
    #with open('/etc/frame_archiver.conf', 'r') as configfile:
        #TODO: config file checking
    #    config.read(configfile)
    #    archiverconf = config['Archiver']
    FRAME_DIR = config['framedir']
    ARCHIVE_DIR = config['archivedir']
    pid = config['pidfile']


    INDEXFILESDIR = config['indexfilesdir']
    MAILADDRESS = config['mailaddress']
    INTERVAL = config.as_int('interval')


    #TODO: Error checking
    FRAME_DIR_PATH = pathlib.PosixPath(FRAME_DIR).resolve()
    ARCHIVE_DIR_PATH = pathlib.PosixPath(ARCHIVE_DIR).resolve()


    #bagging = queue.Queue()
    bagging = collections.deque()
    copying = queue.Queue()

    #run_as_daemon = False
    if args.daemonize == True:
        
        daemon = Daemonize(app="frame_archive", pid=pid,
                           action=archiver, keep_fds=keep_fds, logger=logger,
                           verbose=True, foreground=True)
        
        i = inotify.adapters.Inotify()
        i.add_watch(FRAME_DIR)
        
        daemon.start()
        #while True:
        halt = False
            
        while not halt:
            logging.debug("starting daemon")
            #time.sleep(5)
            events = i.event_gen(yield_nones=False, timeout_s=1)
            #time.sleep(INTERVAL)
            events = list(events)
            for event in events:
                if 'IN_CREATE' in event[1]:
                    logging.debug(event)
                    newdir = pathlib.PosixPath(event[2]).joinpath(pathlib.PosixPath(event[3]))
                    username, uid, gid = get_userdata_from_path(newdir)
                    if username is not None and uid is not None and gid is not None:
                        d = FrameDataset(path=newdir, username=username, uid=uid, gid=gid, bagged=False, tapes=None)
                        logging.debug("adding dataset: %s", d)
                        bagging.append(d)
                        with shelve.open('bagging.shelf') as bagging_shelf:
                            bagging_shelf['bagging'] = bagging
            logging.debug("inotify events: %s",events)

    else:
        #run once
        archiver()