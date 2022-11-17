#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import stat
import libzfs_core
import simplezfs

from fuse import FUSE, FuseOSError, Operations, fuse_get_context
from subprocess import Popen, PIPE, STDOUT
import subprocess


def zfs_call(args):
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if proc.returncode != 0 or len(proc.stderr) > 0:
        raise RuntimeError(proc.stderr)
    for line in proc.stdout.strip().split('\n'):
        yield line
        
def find_closest_snapshot(snapshot):
    args = ['zfs', 'list', '-H', '-p', '-o', 'name,creation', '-t', 'snapshot']

    dataset = snapshot.split('@')[0]
    ctimes = []
    snap_names = []
    snap_ctime = None
    for line in zfs_call(args):
        name, ctime = line.split('\t')
        if name.startswith(dataset):
            if name == snapshot:
                snap_ctime = ctime
            else:
                snap_names.append(name)
                ctimes.append(ctime)
    closest_snap, closest_ctime = None, None

    if snap_ctime is None:
        return RuntimeError(f"snapshot {snapshot} not found")
    
    # find closest snapshot to perform incremental backup
    for snap, ctime in zip(snap_names, ctimes):
        if ctime < snap_ctime:
            if not closest_ctime or ctime > closest_ctime:
                closest_snap = snap
                closest_ctime = ctime
                
    # by default returns None to send the non-incremental initial backup
    return closest_snap


def get_size(zpath):
    from_snap = find_closest_snapshot(zpath)
    if from_snap:
        args = ['zfs', 'send', '-n', '-v', '-P', '-i', from_snap, zpath]
    else:
        args = ['zfs', 'send', '-n', '-v', '-P', zpath]
    lines = list(zfs_call(args))
    return int(lines[1].split('\t')[1])


class SendBuffer(object):
    def __init__(self, zpath):
        self.pointer = 0
        self.from_snap = find_closest_snapshot(zpath)
        if self.from_snap:
            cmd = ['zfs', 'send', '-v', '-P', '-i', self.from_snap, zpath]
        else:
            cmd = ['zfs', 'send', '-v', '-P', zpath]
        print(cmd)
        self.send_proc = Popen(cmd, stdout=PIPE, close_fds=True)
        
    def read(self, length, offset):
        #if offset < self.pointer: # cannot seek
        #    print('rev seek error', offset, self.pointer)
        #    raise FuseOSError(errno.ENOSYS)
        if offset > self.pointer: # can seek but why should we skip data?
            #print('WARNING: seeking')
            self.send_proc.stdout.read(offset - self.pointer)
            self.pointer = offset
        data = self.send_proc.stdout.read(length)
        self.pointer += len(data)
        print(offset, length, len(data), data.__class__)
        return data

    def close(self):
        #TODO: maybe stop process more gracefully.
        self.send_proc.terminate()

class FuseSnapshot(Operations):
    def __init__(self, zpool):
        self.zpool = zpool
        self.zfs = simplezfs.zfs.get_zfs()
        self._open_buffers = dict()
        self._max_buffer_id = 0

    def _path2zpath(self, path):
        zpath = self.zpool+path.rstrip('/')
        return zpath

    # Filesystem methods
    # ==================

#    def access(self, path, mode):
#        pass    

    def getattr(self, path, fh=None):
        zpath = self._path2zpath(path)
        line = next(zfs_call(['zfs', 'list', '-H', '-p', '-o', 'name,type,creation', zpath]))
        name, ztype, ctime = line.split("\t")

        ctime = float(ctime)
        inode_type = stat.S_IFREG if ztype=='snapshot' else stat.S_IFDIR

        st_size = get_size(zpath) if ztype=='snapshot' else 0
        #st_size = 9223372036854775807 if ztype=='snapshot' else 0
        st_size *= 2 # double the estimated size, to avoid truncation

        
        # TODO
        return {
            'st_atime' : ctime,
            'st_ctime' : ctime,
            'st_gid' : 0,
            'st_mode': inode_type | stat.S_IRUSR | stat.S_IXUSR,
            'st_mtime': ctime,
            'st_nlink': 1,
            'st_size': st_size,
            'st_uid': 0
            }

    def readdir(self, path, fh):
        zpath = self._path2zpath(path)
        yield '.'
        yield '..'
        for d in self.zfs.list_datasets(parent=zpath):
            if d.parent == zpath:
                yield d.full_path.lstrip(self.zpool+'/')
        
    def statfs(self, path):
        stv = {
            'f_bsize' : 4096,
            'f_blocks' : 2**16,
            'f_bfree': 0,
            'f_ffree': 0,
            'f_bavail': 0,
            'f_namelen': 1024,
            'f_flag': '',
        }
        return dict((key, getattr(stv, key, 0)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))


    # File methods
    # ============
    
    def open(self, path, flags):
        zpath = self._path2zpath(path)
        print('open', zpath)
        buffer_id = self._max_buffer_id + 1
        self._max_buffer_id = buffer_id
        self._open_buffers[buffer_id] = SendBuffer(zpath)
        return buffer_id
        
    def read(self, path, length, offset, fh):
        print('read', fh, length, offset)
        return self._open_buffers[fh].read(length, offset)
        
    def release(self, path, fh):
        self._open_buffers[fh].close()
        del self._open_buffers[fh]

def main(mountpoint, zpool):
    FUSE(FuseSnapshot(zpool), mountpoint, nothreads=True, foreground=True, allow_other=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
