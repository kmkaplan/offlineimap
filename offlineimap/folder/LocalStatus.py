# Local status cache virtual folder
# Copyright (C) 2002 - 2008 John Goerzen
# <jgoerzen@complete.org>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

from Base import BaseFolder
import os
from contextlib import closing
import anydbm
import shelve
import threading
import whichdb
import sys

magicline = "OFFLINEIMAP LocalStatus CACHE DATA - DO NOT MODIFY - FORMAT 1"

class _MessageList():
    @staticmethod
    def _from_key(key):
        return str(key)

    @staticmethod
    def _to_key(index):
        return long(index)

    def __init__(self, filename, flag):
        self.shelf = shelve.open(filename, flag)

    def __del__(self):
        self.close()

    def __len__(self):
        return len(self.shelf)

    def __getitem__(self, key):
        return self.shelf[_MessageList._from_key(key)]

    def __setitem__(self, key, value):
        self.shelf[_MessageList._from_key(key)] = value

    def __delitem__(self, key):
        del self.shelf[_MessageList._from_key(key)]

    def __contains__(self, key):
        return _MessageList._from_key(key) in self.shelf

    def close(self):
        self.shelf.close()

    def keys(self):
        return [_MessageList._to_key(k) for k in self.shelf.keys()]

class LocalStatusFolder(BaseFolder):
    def __init__(self, root, name, repository, accountname, config):
        self.name = name
        self.root = root
        self.sep = '.'
        self.config = config
        self.dofsync = config.getdefaultboolean("general", "fsync", True)
        self.filename = os.path.join(root, name)
        self.filename = repository.getfolderfilename(name)
        self.messagelist = None
        self.filename_2 = self.filename + '.shelve'
        self.repository = repository
        self.savelock = threading.Lock()
        self.accountname = accountname
        BaseFolder.__init__(self)
        self.migrate_format1_to_format2()

    def is_format1(self):
        if not os.path.exists(self.filename):
            return False
        with open(self.filename, "rt") as file:
            line = file.readline()
        return line.strip() == magicline

    def migrate_format1_to_format2(self):
        if not self.is_format1():
            return
        already_done = False
        try:
            with closing(shelve.open(self.filename_2, 'r')):
                # The file opens correctly: we are already in version 2
                already_done = True
        except:
            None
        if already_done:
            raise StandardError, "LocalStatus: both format 1 and 2 exist"
        # Need to migrate
        self.savelock.acquire()
        try:
            with open(self.filename, "rt") as file:
                line = file.readline().strip()
                assert(line)
                assert(line == magicline)
                with closing(_MessageList(self.filename_2, 'n')) as db:
                    for line in file:
                        line = line.strip()
                        uid, flags = line.split(':')
                        uid = _MessageList._to_key(uid)
                        flags = [x for x in flags]
                        db[uid] = { 'uid': uid, 'flags': flags }
            os.unlink(self.filename)
        finally:
            self.savelock.release()

    def getaccountname(self):
        return self.accountname

    def storesmessages(self):
        return 0

    def isnewfolder(self):
        try:
            with closing(shelve.open(self.filename_2, 'r')):
                return False
        except anydbm.error:
            return True
        assert False

    def getname(self):
        return self.name

    def getroot(self):
        return self.root

    def getsep(self):
        return self.sep

    def getfullname(self):
        return self.filename

    def deletemessagelist(self):
        self.savelock.acquire()
        try:
            if not self.isnewfolder():
                with closing(shelve.open(self.filename_2, 'n')) as db:
                    None
        finally:
            self.savelock.release()

    def cachemessagelist(self):
        self.savelock.acquire()
        try:
            if self.messagelist == None:
                if self.isnewfolder():
                    self.messagelist = _MessageList(self.filename_2, 'n')
                else:
                    self.messagelist = _MessageList(self.filename_2, 'w')
            return self.messagelist
        finally:
            self.savelock.release()

    def save(self):
         None

    def getmessagelist(self):
        return self.messagelist

    def savemessage(self, uid, content, flags, rtime):
        if uid < 0:
            # We cannot assign a uid.
            return uid

        if uid in self.messagelist:     # already have it
            self.savemessageflags(uid, flags)
            return uid

        self.messagelist[uid] = {'uid': uid, 'flags': flags, 'time': rtime}
        return uid

    def getmessageflags(self, uid):
        return self.messagelist[uid]['flags']

    def getmessagetime(self, uid):
        return self.messagelist[uid]['time']

    def savemessageflags(self, uid, flags):
        self.savelock.acquire()
        try:
            desc = self.messagelist[uid]
            desc['flags'] = flags
            # XXX assigning to messagelist is required to persist. See
            # shelve.open
            self.messagelist[uid] = desc
        finally:
            self.savelock.release()

    def deletemessage(self, uid):
        self.deletemessages([uid])

    def deletemessages(self, uidlist):
        # Weed out ones not in self.messagelist
        uidlist = [uid for uid in uidlist if uid in self.messagelist]
        for uid in uidlist:
            del(self.messagelist[uid])
