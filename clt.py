#!/usr/bin/env python
import unicodedata
pinst='/usr/local/apps/ecflow/current/lib/python2.7/site-packages/'
import sys, os
sys.path.append(pinst)
import ecflow as ec
import os.path
from subprocess import Popen, PIPE
try:
    import redis
except: pass
import unittest
ECFLOW  = "/usr/local/apps/ecflow/current/bin/ecflow_client"
CLIENTS = dict()


class Client(object):
    def __init__(self, node, port, path):               
        self.node= node
        self.port= port
        self.path= path
        self.client = ec.Client(self.node, port)
        try:
            if type(path) is str:
                self.client.ch_register(False,
                                        [ str(path.split('/')[1]) ])
            elif type(path) is array:
                # print(path, "array")
                self.client.ch_register(False, [ path ])
            else: self.client.ch_register(False, [ str(path[1]) ])
        except: pass

    def update(self):
        try:
            self.client.sync_local()
            defs = self.client.get_defs()
            return defs, defs.find_abs_node(str(self.path))
        except: pass
        return None, None


def tostr(source):   
    if type(source) == str: return source
    return unicodedata.normalize('NFKD', source).encode('ascii', 'ignore')


def todef(node, port, path):
    global CLIENTS
    full = "%s@%s%s" % ( node, port, path )
    if not CLIENTS.has_key(full):
        if '/' in path:
            loc = '/' + '/'.join(path.split('/')[1:])
            cl = Client(tostr(node), int(port), tostr(loc))
        else: cl = Client(tostr(node), int(port), path.split())
        CLIENTS[full] = cl
    elif len(CLIENTS) == 0: return
    else: cl = CLIENTS[full]
    return cl.update()


def find_script(node, dct, ext=".sms", seed="ECF_FILES"):
    path = node.get_abs_node_path()
    #start = dct[seed].replace("/mc/", "/mc.old/")
    #start = dct[seed].replace("/o/", "/o.old/")
    #start = dct[seed].replace("/eda/", "/eda.old/")
    if ext == "sub":   ext = ".job1.sub"
    elif ext == "job": ext = ".job1"
    elif ext == "out": ext = ".1"
    script = path.split('/')[-1]
    for start in (
            dct[seed].replace("/mc/", "/mc.old/"
                          ).replace("/o/", "/o.old/"
                                ).replace("/eda/", "/eda.old/"),
            dct[seed],
    ):
      for d in path.split('/'):
        test = start + '/' + script + ext
        if os.path.isfile(test): return test
        elif start[-1] != '/': start += '/'
        start += d

    if seed != "ECF_HOME":
        return find_script(node, dct, ext, "ECF_HOME")

    return None

           
def get_node_include(defs, node):
    output = dict()
    if not defs: return output
    for item in defs.get_all_nodes():
        pitem = item.get_abs_node_path()
        if type(item) != ec.Task: continue
        elif not node: continue
        elif node.get_abs_node_path() not in pitem:
            continue
        dct = fill_var(item)
        output[pitem] = dct
        where = find_script(item, dct)
        output[pitem]['include'] = list_include(where, dct)
    return output


def get_stamp(fname):
    import dateutil.parser as parser
    import time
    if not fname: return 0
    text = time.ctime(os.path.getctime(fname))
    date = parser.parse(text)
    date = date.replace(tzinfo=None)
    return date.strftime('%m%d%H%M%S')

"""
        red = redis.StrictRedis() # localhost 6379
        red.set("key", "value")
        red.get("key")
        print(red)

export PATH=/tmp/map/work/redis-2.8.19/src:$PATH; cd /tmp/$USER; redis-server

"""


def get_node_times(defs, node):
    output = dict()
    if not defs: return output

    try: red = redis.StrictRedis() # localhost 6379
    except: red = None
    is_in_redis = False

    for item in defs.get_all_nodes():
        pitem = item.get_abs_node_path()
        if type(item) != ec.Task: continue
        elif node.get_abs_node_path() not in pitem:
            continue
        dct = jobs_var(item)
        output[pitem] = dct
        output[pitem] = [ pitem ] # dct
        # is_in_redis = False
        if red: # is_in_redis:
            item = "%s" % red.get(pitem)
            got = item.replace("[", "").replace(
                "]", "").replace("'", "").replace(
                    "\"", "").replace(", ", ",")
            # if ",0,0,0" in got: continue
            # if ",0" not in got: got += ",0,0,0"
            # print(got)
            output[pitem] = got
        else:
            where = find_script(item, ext="job", dct=dct, seed="ECF_HOME")
            # output[pitem]['job'] = where, get_stamp(where)
            # output[pitem].append( get_stamp(where) )
            job = get_stamp(where)            
            where = find_script(item, ext="sub", dct=dct, seed="ECF_HOME")
            # output[pitem]['sub'] = where, get_stamp(where)
            # output[pitem].append( get_stamp(where) )
            sub = get_stamp(where)            
            where = find_script(item, ext="out", dct=dct, seed="ECF_HOME")
            # output[pitem]['out'] = where, get_stamp(where)
            # output[pitem].append( get_stamp(where) )
            out = get_stamp(where)            
           
            check = "%s" % red.get(pitem)
            if (", %s, %s, %s]" % (job,sub,out) in check or
                ", '%s', '%s', '%s', '%s', ]" % (
                    job,sub,out, out) in check):
                is_in_redis = True
            else:
                for item in (job, sub, out):
                    output[pitem].append(item)
                # if ",0,0,0" in "%s" % output[pitem]: continue               
                red.append(pitem, output[pitem])
    return output


def get_node_vars(defs, item, node, port):
    output = dict()
    if not defs: return output
    pitem = item.get_abs_node_path()
    for this in defs.get_all_nodes():
        if type(this) != ec.Task: continue
        tpath = this.get_abs_node_path()
        if pitem not in tpath: continue
        out, err = edit(node, port, tpath)
        got = []
        if out:
          for line in out.split("\n"):
            if "%comment - ecf user" in line: continue
            elif "%end - ecf user variables" in line: break
            got.append(line)
        output[this.get_abs_node_path()]= got
    return output


def edit(node, port, path, preprocess=False):
        proc = Popen([ECFLOW, "--port", str(port), "--host", node,
                      "--edit_script", path, "edit" ],
                     stdout= PIPE, stderr= PIPE)
        return proc.communicate("")


def file(node, port, path, kind="script"):
        proc = Popen([ECFLOW, "--port", str(port), "--host", node,
                      "--file", path, kind ],
                     stdout= PIPE, stderr= PIPE)
        return proc.communicate("")


def get_tasks(defs, item):
    output = []
    if not defs: return output
    pitem = item.get_abs_node_path()
    for this in defs.get_all_nodes():
        if type(this) != ec.Task: continue
        tpath = this.get_abs_node_path()
        if pitem not in tpath: continue
        output.append(tpath)
    return output


def fill_var(node, out=None):
    if not out: out = dict()
    if not node: return out
    for var in node.variables:
        if var.name() in ("ECF_HOME", "ECF_FILES", "ECF_INCLUDE"):
            if out.has_key(var.name()): continue
            out[var.name()]= var.value()
    if len(out) == 3:
        LISTFILES.ingest(out["ECF_FILES"], ".sms")
        if 0: LISTFILES.ingest(out["ECF_HOME"], ".sms") # FIXME too large for oper
        LISTFILES.ingest(out["ECF_INCLUDE"], "inc")
        return out
    node = node.get_parent()
    return fill_var(node, out)


def jobs_var(node, out=None):
    if not out: out = dict()
    if not node: return out
    for var in node.variables:
        if var.name() in ("ECF_HOME", "ECF_OUT", ):
            if out.has_key(var.name()): continue
            out[var.name()]= var.value()
    if len(out) == 2: return out
    node = node.get_parent()
    return jobs_var(node, out)


class ListFiles(object):
    def __init__(self):
        self.froms = dict()
        self.files = dict()
        self.usedf = dict()
        self.incls = dict()

    def ingest(self, dir, ext=None):
        if dir in self.froms.keys(): return
        self.froms[dir] = ()
        for root, dirs, files in os.walk(dir):
            for file in files:
                if ext:
                    if ".old" in file: continue
                    elif "~" in file: continue
                    if ext == "inc":
                        self.incls[file] = ()
                    elif file.endswith(ext):
                        self.files[file] = ()
                    else: pass

    def clear(self):
        self.froms.clear()
        self.files.clear()
        self.usedf.clear()
        self.incls.clear()

    def report(self):
        out = []
        if 0:
            out.append("\n# FROM " + pprint.pformat( self.froms.keys(), indent=2))
            out.append("\n# FILE " + pprint.pformat( self.files.keys(), indent=2))
            out.append("\n# INCL"  + pprint.pformat( self.incls.keys(), indent=2))
            out.append("\n# USED"  + pprint.pformat( self.usedf.keys() ,indent=2))
        out.append("# UNUSED:")
        for item in self.incls.keys():
            if item not in self.usedf.keys(): out.append(item)
        if 0:
            for item in self.files.keys():
                if item not in self.usedf.keys(): out.append(item)
        return out


def list_include(where, dct, out=None):
    if not where: return
    if not out: out = dict()
    if not os.path.isfile(where): return dict()
    key = "%include "
    commons = ("config.h", "setup.h", "step1.h", "step2.h", "trap.h", "qsub.h",
               "endt.h", "rcp.h")
    with open(where, "r") as source:
        for line in source.readlines():
            next = None
            if "%include <" in line: # below INCLUDE
                next = dct["ECF_INCLUDE"] + '/'
                next += line.replace("%include <", "").split('>')[0]
            elif "%include \"" in line: # below HOME               
                next = dct["ECF_HOME"] + '/'
                next += line.replace("%include \"", "").split('"')[0]
                list_include(dct["ECF_HOME"] + '/' + next, dct, out)
            elif key in line: # absolute path              
                next = line.replace(key, "").split(' ')[0]
            if next:
                list_include(next, dct, out);
                if where in commons:
                    pass
                elif next in out.keys():
                    out[next].append(where)
                for key in commons:
                    if key in next:
                        out[next] = ()
                        break
                else: out[next] = [where]
    # import pprint; print pprint.pformat(out, indent=2)
    out[where] = []
    LISTFILES.usedf.update(out)
    return out.keys()

class Test0069(unittest.TestCase):
    """ a test case """
    def test_0069(self):
        node = "vsms3"
        port = 31415
        task = "/eda/main/00"
        defs, item = todef(node, port, task)
        res = get_node_times(defs, item),
        print(res)

if __name__ == '__main__':
    unittest.main()

