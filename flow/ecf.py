#!/usr/bin/env python
# coding=utf-8
# This software is provided under the ECMWF standard software license
# agreement.
""" a layer over raw ecflow api

use 'export ECF_DEBUG_LEVEL=10' to remove warning message related to
variables overwrite

specialisation module for ecFlow nodes/attributes:
initially it was simple derivation
then it is composition


https://pypi.python.org/pypi/anytree
pip install anytree --user

20180126 https://www.python.org/dev/peps/pep-0343/
"""
import sys
import pwd
import os
import unittest
try:
    import ecflow
except ImportError:
    loc = "/usr/local/apps/ecflow/current/lib/python2.7/site-packages/ecflow"
    sys.path.append(loc)  # centre
    loc = "/usr/local/lib/python2.7/site-packages"
    sys.path.append(loc)  # elearning
    import ecflow
ecflow.Ecf.set_debug_level(3)

global DEFS
DEFS = ecflow.Defs()
# DEBUG = True
DEBUG = False
DECORATE = "ALL"
# DECORATE = "ONLY_TRIGGER"
# DECORATE = "NO_TRIGGER"
# DECORATE = "ONLY_EVENT"
# DECORATE = "NO_ATTRIBUTE"

USE_TIME = True
USE_LATE = False
USE_TRIGGER = True
USE_LIMIT = True
USE_LABEL = True
CPUTIME = True
USE_VERIFY = True


class DefError(Exception):
    pass


if DECORATE == "NO_TRIGGER":
    USE_TRIGGER = False
    USE_LIMIT = True
    USE_EVENT = True
elif DECORATE == "ALL":
    USE_TRIGGER = True
    USE_LIMIT = True
    USE_EVENT = True
else:
    raise DefError

deployed = []


def deploy(script="echo", pathname=os.getenv("HOME") + "ecflow_servers/files", taskname="t1", extn=".ecf"):
    if taskname is not None:
        pathname += "/%s" % taskname
    if extn is not None:
        pathname += extn

    if pathname in deployed:
        return  # create once
    else:
        deployed.append(pathname)
    if extn in pathname:  # surround .ecf with head, tail:
        script = "%include <head.h>\n" + script + "\n%include <tail.h>"
    with open(pathname, "w") as destination:  # overwrite!
        print(script, file=destination)
        print("#MSG: created", pathname)


def get_username():
    return pwd.getpwuid(os.getuid())[0]


def get_uid():
    return pwd.getpwnam(get_username()).pw_uid


home = os.getenv("HOME") + "/ecflow_server"
user = os.getenv("USER")

ECF_PORT = os.getenv("ECF_PORT", 1500 +
                     int(pwd.getpwnam(get_username()).pw_uid))
ECF_HOME = os.getenv("ECF_HOME", "localhost")
CLIENT = ecflow.Client(ECF_HOME + ":%s" % ECF_PORT)  # PYTHON CLIENT
deployed = []


def def_port():
    return 1500 + int(pwd.getpwnam(get_username()).pw_uid)


def deploy(script, pathname, extn=".ecf"):
    if pathname in deployed:
        return  # create once
    else:
        deployed.append(pathname)
    if extn in pathname:  # surround .ecf with head, tail:
        script = "%include <head.h>\n" + script + "\n%include <tail.h>"
    with open(pathname, 'w') as destination:  # overwrite!
        print(script, file=destination)
        print("#MSG: created", pathname)
# deploy("echo acq %TASK%", files + acq + extn)  # create wrapper
# deploy("ecflow_client --label info %TASK%", files + post + extn)

##########################################################################
head_h = """#!%SHELL:/bin/bash%
#set -e # stop the shell on first error
#set -u # fail when using an undefined variable
#set -x # echo script lines as they are executed

# Defines the variables that are needed for any communication with ECF
export ECF_PORT=%ECF_PORT%    # The server port number
export ECF_HOST=%ECF_HOST%    # where the server is running
export ECF_NAME=%ECF_NAME%    # The name of this current task
export ECF_PASS=%ECF_PASS%    # A unique password
export ECF_TRYNO=%ECF_TRYNO%  # Current try number of the task
export ECF_RID=$$             # record the process id. Also used for
                              # zombie detection

# Define the path where to find ecflow_client
# make sure client and server use the *same* version.
# Important when there are multiple versions of ecFlow
export PATH=/usr/local/apps/ecflow/%ECF_VERSION%/bin:$PATH
export PATH=$PATH:/usr/local/apps/ecflow/bin:/usr/local/bin

# Define a error handler
ERROR() {
   set +e                      # Clear -e flag, so we don't fail
   wait                        # wait for background process to stop
   ecflow_client --abort=trap  # Notify ecFlow that something went
                               # wrong, using 'trap' as the reason
   trap 0                      # Remove the trap
   exit 0                      # End the script
}

# Tell ecFlow we have started
ecflow_client --init=$$
set -eux
"""

tail_h = """
set +x
wait           # wait for background process to stop
ecflow_client --complete  # Notify ecFlow of a normal end
trap 0                    # Remove all traps
exit 0                    # End the shell
"""


def create_head_and_tail(ecf_home=None, head="head.h", tail="tail.h"):
    if ecf_home is None:
        HOME = os.getenv("HOME")
        ecf_home = HOME + "/ecflow_server/include/"

    if not os.path.exist(head):
        with open(ecf_home + head, 'w') as fip:
            write(head_h, file=fip)

    if not os.path.exist(tail):
        with open(ecf_home + tail, 'w') as fip:
            write(tail_h, file=fip)


class CWN(object):
    """ CWN is for current working node
    so that new attributes and node are attached to it,
    when there is wish not to use .add() FP syntax """

    __CDP = False
    __CWN = []

    @classmethod
    def cdp(cls, active=False):
        CWN.__CDP = active
        if active and CWN.is_empty():
            return
        if DEBUG:
            print("#DBG: CWN reset")
        CWN.__CWN = []

    @classmethod
    def is_empty(cls):
        return len(CWN.__CWN) == 0

    @classmethod
    def pop(cls):
        if CWN.is_empty():
            return
        if DEBUG:
            print("#BDG: pop")
        CWN.__CWN.pop()

    @classmethod
    def last(cls):
        if CWN.is_empty():
            return None
        return CWN.__CWN[-1]

    def __init__(self, item=None):

        def name(item):
            if DEBUG:
                if type(item) in (Event, Meter, Label):
                    print("#BDG:", type(item), "%s" % item.real)
                elif type(item) in (Clock, ):
                    print("#BDG:", type(item), "%s" % item.real)
                else:
                    print("#BDG:", type(item), item.real.name())

        if item is None or not CWN.__CDP:
            # print("#BDG: no item")
            return

        if type(item) == Suite and CWN.is_empty():
            DEFS.add(item)  # aka add_suite
            CWN.__CWN.append(item)
            print("#BDG: append suite", item.name())
            return

        if id(item) == id(CWN.last()):  # __eq__ is used for trigger str
            return

        if type(item) == Suite:
            if not CWN.is_empty():
                print("#WAR: changing CWN from", CWN.__CWN.name(), "to",
                      item.name())
            CWN.__CWN.append(item)
        elif item is None:
            pass
        elif item.real == None:
            pass
        elif CWN.is_empty():
            raise DefError("#ERR: no node!")
        else:
            if type(CWN.last()) == Task and type(item) in (Task, Family):
                CWN.pop()
            # print("#DBG:", CWN.last().name(), type(item))
            name(item)
            item.add_to(CWN.last())
            if type(item) in (Task, Family):
                # print("#DBG: append", item.name(), type(item))
                CWN.__CWN.append(item)
            # else: print("#DBG: add", type(item), "to", CWN.last().name())


def obsolete():
    if 1:
        return
    raise DefError


def raise_int(arg):
    if 0:
        return
    if "/none" in arg:
        raise DefError("None!!!", arg)
    if arg in ("1==0", "0==1", "1==1", "0==0"):
        return
    if arg[0] in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
        raise DefError("please add ./", arg)


def set_raw_mode(kind=True):
    import sms2ecf
    sms2ecf.RAW_MODE = kind


LINKTASK = False  # FD
# LINKTASK = True  # RD mode v we turn task into families,
# with a leaf a task matching script name


def linktask(name, real="get", add=None, fam=None, task=True):
    """ give a change to avoid UNIX links time consuming for maintenance, update
    perforce git
    name: original name
    real: script/task wrapper name
    add: attributes that shall be added to the leaf task,
         eg Label, Event, Meter
    """
    if fam is None:
        fam = name
    try:
        import parameters as ip
        # if ip.USER == "rdx": global LINKTASK; LINKTASK = True  # acceptance
        # if ip.USER == "emos": global LINKTASK; LINKTASK = False  # acceptance
    except ImportError:
        pass
    if LINKTASK:
        return Task(name)  # RD???
    return Family(fam).add(
        Task(real).add(add, If(task, Edit("TASK", name))))


def translate(name, value=None):
    """ Translate from sms to ecflow """
    try:
        import sms2ecf
    except ImportError:
        return name, value

    def is_sms():
        if sys.argv[-1] in ("sms", "ligarius"):
            sms2ecf.ECF_MODE = "sms"
            return True
        return sms2ecf.ECF_MODE == "sms"
    if is_sms():
        sms2ecf.ECF_MODE = "sms"
        return sms2ecf.translate(name, value)
    return name, value


# USE_EXTERN = 1
# NO_EXTERN_ALONE = 0 # set to 1 for test/activate


class Extern(object):
    """ extern may be collected and added as soon as Defs is created """

    def __init__(self, path):
        # if type(path) not in (tuple, list, str): raise DefError(type(path))

        if type(path) in (tuple, list):
            for ppp in path:
                Extern(ppp)

        elif type(path) in (str, ):
            if DEBUG:
                print("#MSG: extern", path)
            if ".Extern" in path:
                raise DefError(".Extern?")
            if ":" in path:
                # define parent so user does not have to
                Extern(path.split(":")[0])

            if path == "":
                pass
            elif DEFS is not None:
                if ".Extern" in path:
                    raise DefError(type(path))
                DEFS.real.add_extern(str(path))
            else:
                raise DefError

        elif path is None:
            pass

        else:
            raise DefError(type(path), path)

    def add_to(self, node):
        return None


def protect(num):
    if type(num) is str:
        if num[0].isdigit():
            return './' + num
    return num


class State(object):
    """ this class aims at affording a user the possibility to add Triggers as
    t1 = Task("t1")
    Task("ts").add(Trigger(t1 == COMPLETE))

    SUBMITTED, ACTIVE, SUSPENDED, ABORTED, QUEUED, COMPLETE, UNKNOWN
    are instance of this class.
    """

    def __init__(self, state):
        """ store the status """
        self.state = str(state)

    def __str__(self):
        """ translate into string """
        return "%s" % self.state

    def __eq__(self, arg):
        """ when == is used,
        we should care about task name starting with 0-9 """
        if type(arg) == str:
            return protect(arg) + " == " + self.state
        elif isinstance(arg, Node):
            return arg.fullname() + " == " + self.state
        elif isinstance(arg, ecflow.Node):
            return arg.get_abs_node_path() + " == " + self.state
        return False

    def __ne__(self, arg):
        """ aka != """
        if type(arg) == str:
            return protect(arg) + " != " + self.state
        elif isinstance(arg, Node):
            return arg.load.get_abs_node_path() + " != " + self.state
        return False

    def value(self):
        """ return state """
        return self.state

    def eval(self, node):
        """ return state """
        return self.state == node.get_state()


SUBMITTED = State("submitted")
ACTIVE = State("active")
SUSPENDED = State("suspended")
ABORTED = State("aborted")
QUEUED = State("queued")
COMPLETE = State("complete")
UNKNOWN = State("unknown")


class Attribute(object):
    """ generic attribute to be attached to a node """

    def __init__(self):
        self.load = None

    @property
    def real(self):
        return self.load

    def __enter__(self):
        CWN(self)

    def __exit__(self, type, value, traceback):
        pass

    def add_to(self, node):
        """ use polymorphism to attach attribute to a node """
        raise DefError("ERR: virtual class")

    def __get_attr__(self, attr):
        return getattr(self.load, attr)

    def __iadd__(self, other): self.add_to(other)


class Label(Attribute):
    """ wrap around label """

    def __init__(self, name, msg=""):
        self.load = ecflow.Label(name, msg)
        CWN(self)

    def add_to(self, node):
        """ add_label """
        if USE_LABEL:
            node.load.add_label(self.load)
        return node


class Meter(Attribute):
    """ wrap around meter """

    def __init__(self, name, beg, end, thr=None):
        if thr is None:
            thr = end
        self.load = ecflow.Meter(
            name, int(beg), int(end), int(thr))
        CWN(self)

    def add_to(self, node):
        """ add_meter """
        node.load.add_meter(self.load)
        return node


class Event(Attribute):
    """ wrap around event """

    def __init__(self, name="", num=0):
        self.load = ecflow.Event(name)
        if 1:
            return

        if type(name) == str:
            if " " in name:
                num, name = name.split(" ")
        try:
            if int(num) > 100000:
                num = 0
        except:
            pass
        self.load = None
        if type(name) is int and num == 0:
            num = name
            name = ""

        if type(name) is int and type(num) == str:
            self.load = ecflow.Event(name, num)
        elif type(name) is str and type(num) == int:
            self.load = ecflow.Event(num, name)
        elif type(name) is str and type(num) == str:
            if num == "":
                self.load = ecflow.Event(name)
            elif name == "":
                self.load = ecflow.Event(num)
            else:
                self.load = ecflow.Event(int(num), name)  # print("#str str")
        else:
            self.load = ecflow.Event(name)
        CWN(self)

    def add_to(self, node):
        """ add_event """
        for test in node.load.events:
            if test.name() == self.load.name():
                if (test.number() == self.load.number() and
                        test.name() == self.load.name()):
                    return
        node.load.add_event(  # self.load.number(),
            # self.load.name(),
            self.load,
        )
        return node


def minus2underscore(name):
    if type(name) is not str:
        return name
    if '-' in name:
        return name.replace('-', '_')
    return name


class Inlimit(Attribute):
    """ a class to host a path for a limit
        silently ignore if USE_LIMIT is False,
        (in debug mode) """

    def __init__(self, fullpath, tokens=1):
        self.load = None
        if USE_LIMIT:
            if fullpath is None:
                # print("#WAR: no limit")
                return
            try:
                (path, name) = fullpath.split(":")
                if name is None:
                    # print("#WAR: no limit", path)
                    return
                if name == "":
                    print("#WAR: no limit", path)
                    return
            except ValueError:
                name = fullpath
                path = ""
            if name is None:
                raise DefError
            # if '-prod' in name: name = name.split('-')[0]  # vdiss-prod
            name = minus2underscore(name)
            if "None" in path or "None" in name:
                raise DefError(path, name, fullpath)
            if " " in name:
                name, tokens = name.split(" ")
            self.load = ecflow.InLimit(name, path, int(tokens))
            self.path_ = path
            self.name_ = name
            self.tokens_ = tokens
            CWN(self)

    def add_to(self, node):
        """ add_inlimit """
        if not USE_LIMIT:
            return
        if self.load is None:
            return
            # raise DefError
        for dup in node.real.inlimits:
            if (dup.name() == self.load.name() and
                    dup.path_to_node() == self.load.path_to_node()):
                return None
        if node.name() == "ifstraj" and "eda" in "%s" % self.load:
            raise Exception
        if "/none" in "%s" % self.load:
            # raise Exception(self.load)
            print("#WAR: ignoring inlimit", self.load)
            return node
        node.load.add_inlimit(self.load)
        return node

    def value(self):
        """ get limit fullpath-name """
        return self.path_ + ":" + self.name_

    def name(self):
        """ get limit name """
        return self.name_


class InLimit(Inlimit):
    pass


def item_to_string(prev, name=None):
    """ well... """
    if type(name) == str:
        if name in ("T", "P", "Z", "a", "b", "c", "o", 0, 1, 2, 3,
                    "0", "1", "2", "3",
                    ):
            pass
        elif len(name) == 1:
            raise DefError("please avoid", name)
    if name in (None, ''):
        return prev
    if prev is None:
        prev = ''
    AND = ''
    if type(prev) in (list, set, tuple):
        out = ""
        for item in prev:
            out += AND + item_to_string("", item)
            AND = ' and '
        return item_to_string(out, name)
    elif type(name) in (list, set, tuple):
        out = ""
        for item in name:
            out += item_to_string(out, item)
        return item_to_string(prev, out)
    if prev != '':
        AND = ' and '
    if '<main>' in name:
        raise DefError
    if ' or ' in name:
        AND = ''
    if not (' or ' in name or
            '==' in name or
            ':' in name or  # event: and x:1
            'complete' in name):
        if len(name) > 0:
            # if name[0].isdigit():
            # pre = './'  # '0123456789':
            name = protect(name)
        name += " == complete"
    # import parameters as ip
    # if ip.LFSOBS and "emos/main" in prev + AND + name: raise
    return prev + AND + name


class Trigger(Attribute):
    """ add trigger (string, list of task names, or directly
           expression and: and'ed (True) or or'ed (False) unk: add or
           [name]==unknown for RD """

    def __init__(self, expr, unk=False, anded=True):
        self.expr = ""
        self.load = None
        if expr is None or expr == "":
            return
        if type(expr) == str:
            if "%s" in expr:
                raise DefError("ERR:", expr)
            if "%d" in expr:
                raise DefError("ERR:", expr)
            self.expr = expr
            try:
                import parameters as ip
                dim = len(ip.SELECTION)
                for ploc in expr.split():
                    if ploc[0] == '/' and ip.SELECTION != ploc[1:dim + 1]:
                        if ploc in ("eq", "ne", "==", "!=", "and", "or",
                                    "active", "complete", "queued", ):
                            continue
                        if ploc.isdigit():
                            continue
                        # Extern(ploc.replace("==complete", "").replace(")", ""))
            except AttributeError:
                pass
            except ImportError:
                pass
            # NO_EXTERN_ALONE = 0 # set to 1 for test/activate
            return
        if type(expr) == tuple:
            prep = list(expr)
            expr = prep

        if type(expr) == Task:
            pnode = expr.fullname()
            if pnode[0] != '/':
                pnode = expr.name()
            try:
                import inc_common as ic
                if ic.psel() not in pnode:
                    pnode = expr.name()
            except ImportError:
                pass
            # if 1: raise DefError("really?", pnode)
            self.expr = item_to_string(self.expr, pnode)

        elif type(expr) in (list, set, tuple):
            for index, name in enumerate(expr):
                if name is None:
                    continue
                pre = ""
                if name is None:
                    continue
                if type(name) in (str, list, set, tuple):
                    pass
                elif type(name) in (Node, Task, Family, Suite):
                    fullname = name.fullname()
                    if fullname[0] == '/':
                        try:
                            import inc_common as ic
                            if ic.psel() not in fullname:
                                fullname = name.name()
                        except:
                            print("#WAR: trigger except")
                    name = fullname
                elif type(name) in (ecflow.Task, ecflow.Family):
                    name = name.name()
                elif type(name) == str:
                    pass
                else:
                    raise DefError(type(name), name)
                self.expr = item_to_string(self.expr, name)
                continue

        elif type(expr) in (ecflow.Expression,
                            ecflow.PartExpression):
            self.expr = ecflow.Expression(str(item))

        elif type(expr) in (Family, Task):
            fullname = expr.fullname()
            if fullname[0] == '/':
                try:
                    import inc_common as ic
                    if ic.psel() not in fullname:
                        fullname = name.name()
                except ImportError:
                    print("#WAR: trigger expcept")
            self.expr = fullname
            # check full name is really full name, not /name
            # print("#REM:", self.expr)
            if self.expr == "/%s" % expr.name():
                self.expr = expr.name()
        else:
            raise DefError("what? trigger?", type(expr))

        if "YMD+1" in self.expr:
            raise DefError(self.expr)
        # self.load = "trigger"  # ecflow.Trigger(self.expr)
        CWN(self)
        # NO_EXTERN_ALONE = 0 # set to 1 for test/activate

    def expression(self):
        return self.expr

    def add_to(self, node):
        if self.expr is None or self.expr == "" or not USE_TRIGGER:
            return None
        if 1 and " :YMD " in "%s" % self.expr:
            raise DefError(self.expr, node.fullname())  # help DEBUG KEEPME
        # if 1 and "(prep_chem eq complete" in self.expr: raise DefError(self.expr, node.fullname())  # help DEBUG KEEPME
        # if 1 and "/lw/lag" in "%s" % self.expr: raise DefError(self.expr, node.fullname())  # help DEBUG KEEPME
        # if 1 and "fb/archive_prepare" in "%s" % self.expr: raise
        # DefError(self.expr, node.fullname())  # help DEBUG KEEPME
        import inc_common as ic
        if "%s" in self.expr:
            raise DefError(self.expr)
        if "%d" in self.expr:
            raise DefError(self.expr)
        import parameters as ip
        # if ip.LFSOBS and "/emos/main" in "%s" % self.expr: raise DefError()
        if "/none" in "%s" % self.expr:
            raise DefError("#WAR: most certainly something to fix", self.expr)
        import parameters as ip
        if ip.LFSOBS and "emos/main" in self.expr:
            raise DefError(self.expr)
        if (type(node) == Suite or
                type(node.load) == ecflow.Suite):
            raise DefError("#ERR: cannot add trigger to a suite",
                           node.name(), "\n===", self.expr, "===")
        elif "None" in self.expr:
            raise Exception("#ERR, may you please change this?", self.expr)
        elif node.load.get_trigger() is None:
            node.load.add_trigger(self.expr)
        else:
            node.load.add_part_trigger(ecflow.PartExpression(self.expr, True))
        return node


class TriggerImpossible(Trigger):
    """ attribute to be added to node when it is not expected to run any task """

    def __init__(self):
        """ add an 'impossible trigger', for a task not to run """
        super(TriggerImpossible, self).__init__("1==0")


class TriggerAlways(Trigger):
    """ attribute to be added to node when it is not expected to run any task """

    def __init__(self):
        """ add an 'impossible trigger', for a task not to run """
        super(TriggerAlways, self).__init__("1==1")


class TriggerOr(Trigger):
    """ attribute to be added to node when it is not expected to run any task """

    def __init__(self, expr):
        """ add an 'impossible trigger', for a task not to run """
        super(TriggerOr, self).__init__(expr)

    def add_to(self, node):
        if node.real.get_trigger() == None:
            raise DefError("a trigger shall be added first")
        node.real.add_part_trigger(ecflow.PartExpression(self.expr, False))


class Complete(Trigger):
    """ class to host complete expression, added later to a node """

    def __init__(self, expression, unk=False, anded=False):
        super(Complete, self).__init__(expression, unk, anded)

    def add_to(self, node):
        if USE_TRIGGER and self.expr is not None:
            if "%s" in self.expr:
                raise DefError
            if "%d" in self.expr:
                raise DefError
            node.load.add_complete(self.expr)
        return node


class CompleteOr(Complete):
    """ attribute to be added to node when it is not expected to run any task """

    def __init__(self, expr):
        """ add an 'impossible trigger', for a task not to run """
        super(CompleteOr, self).__init__(expr)

    def add_to(self, node):
        if node.real.get_complete() == None:
            raise DefError("a trigger shall be added first")
        node.real.add_part_complete(ecflow.PartExpression(self.expr, False))


class CompleteAlways(Complete):
    """ attribute """

    def __init__(self):
        """ always True as soon as evaluated """
        super(CompleteAlways, self).__init__("1==1")


class Clock(Attribute):
    """ wrapper to add clock """

    def __init__(self, arg="24:00", hybrid=0):
        self.load = None
        if arg == "real":
            self.load = ecflow.Clock(False)
        elif arg == "hybrid":
            self.load = ecflow.Clock(True)
        elif type(arg) == str:
            hybrid = "hybrid" in arg
            if "hybrid " in arg:
                arg = nat(arg, "hybrid")
                hybrid = 1
            elif "real " in arg:
                arg = nat(arg, "real")
                hybrid = 0
            if " -s" in arg:
                arg = arg.replace(" -s", "")
                sync = True
            else:
                sync = False
            hhh, mmm, sss = [0, 0, 0]
            if 1:
                if "." in arg and " " in arg:
                    ymd, hhh = arg.split(" ")
                    ddd, mmm, yyy = ymd.split('.')
                    self.load = ecflow.Clock(int(ddd), int(mmm), int(yyy),
                                             hybrid)
                    rel = "+" in hhh
                    if hhh != "":
                        self.load.set_gain_in_seconds(int(hhh), rel)
                elif "+" in arg:
                    rel = "+" in arg
                    self.load = ecflow.Clock(hybrid)
                    self.load.set_gain_in_seconds(int(arg), rel)
                elif arg is None and hybrid is False:
                    pass
                elif arg == "None" and hybrid is False:
                    pass
                else:
                    rel = "+" in arg
                    from datetime import date
                    # print("#Clock+", arg, type(arg), hybrid, self.load, )
                    if 0:
                        ymd = "%s" % date.today()
                        yyy, mmm, ddd = ymd.split("-")
                        self.load = ecflow.Clock(
                            int(ddd), int(mmm), int(yyy), hybrid)
                    else:
                        self.load = ecflow.Clock(hybrid)
                    if arg != "":
                        self.load.set_gain_in_seconds(int(arg), rel)
            # except ValueError:
            #    print("#Value Error", arg, hybrid)
            #    self.load = ecflow.Clock(hybrid)
            if self.load and sync:
                self.load.set_virtual(True)
        else:
            # print("#else", arg, hybrid)
            self.load = ecflow.Clock(arg, hybrid)

        # print("#Clock", arg, hybrid, self.load)
        if arg is None and hybrid is False:
            self.load = None
        CWN(self)

    def add_to(self, node):
        if type(node) != Suite:
            print("#WAR: clock can only be attached to suite node,\n",
                  "#WAR: clock is ignored")
            return
        if node.real.get_clock() is None and self.load is not None:
            # print("#clock", node.real.get_clock(), self.load)
            node.load.add_clock(self.load)
        return node


class Autocancel(Attribute):
    """ wrapper to add time """

    def __init__(self, arg):
        if type(arg) == str:
            if ':' in arg:  # hh:mm +hh:mm
                hhh, mmm = arg.split(':')
                rel = '+' in arg
                self.load = ecflow.Autocancel(int(hhh), int(mmm), rel)
            else:
                self.load = ecflow.Autocancel(int(arg))
        else:
            self.load = ecflow.Autocancel(arg)  # days

    def add_to(self, node):
        node.load.add_autocancel(self.load)
        return node


class Autocancel(Autocancel):
    pass


class Script(Attribute):

    def __init__(self, script):
        self._script = script

    def add_to(self, node):
        print("#WAR: Script attribute, not yet ... ignored")
        return node


class Verify(Attribute):
    """ wrapper to add time """

    def __init__(self, arg):
        if "verify " in arg:
            arg = nat(arg, "verify")
        y, z = str(arg).split(":")
        kinds = {
            "aborted": ecflow.State.aborted,
            "complete": ecflow.State.complete,
            "active": ecflow.State.active,
            "submitted": ecflow.State.submitted,
            "unknown": ecflow.State.unknown,
            "queued": ecflow.State.queued,
        }
        self.load = ecflow.Verify(kinds[y], int(z))

    def add_to(self, node):
        if USE_VERIFY and self.load is not None:
            node.load.add_verify(self.load)
        return node


class Time(Attribute):
    """ wrapper to add time """

    def __init__(self, arg):
        self.load = arg

    def add_to(self, node):
        if USE_TIME and self.load is not None:
            node.load.add_time(self.load)
        return node


class Today(Time):
    """ wrapper to add time """

    def add_to(self, node):
        if USE_TIME and self.load is not None:
            node.load.add_today(self.load)
        return node


class Cron(Time):
    """ wrapper to add time """

    def __init__(self, bes, wdays=None, days=None, months=None):
        import argparse
        self.load = ecflow.Cron()

        if not ("-w" in bes or
                "-m" in bes or
                "-d" in bes or
                "-s" in bes):
            self.load.set_time_series(bes)
            return

        parser = argparse.ArgumentParser()
        parser.add_argument("-w", nargs=1, default=None, help="weekdays")
        parser.add_argument("-m", nargs=1, default=None, help="months")
        parser.add_argument("-d", nargs=1, default=None, help="days")
        parser.add_argument("-s", action="store_true", help="sync")
        parser.add_argument("begin", nargs='+', help="begin")
        if " -s" in bes:
            bes = bes.replace(" -s", "")
            sync = True
        else:
            sync = False
        parsed = parser.parse_args(bes.split())

        if wdays is not None:
            self.load.set_week_days(wdays)
        if parsed.w:
            self.load.set_week_days([
                int(x) for x in str(parsed.w[0]).split(',')])
        if days is not None:
            self.load.set_day_of_month(days)
        if parsed.d:
            self.load.set_days_of_month([
                int(x) for x in parsed.d[0].split(',')])
        if months is not None:
            self.load.set_months(month)
        if parsed.m:
            self.load.set_months([
                int(x) for x in parsed.m[0].split(',')])
        self.load.set_time_series(' '.join(parsed.begin))

    def add_to(self, node):
        if USE_TIME and self.load is not None:
            node.load.add_cron(self.load)
        else:
            print("#WAR: ignoring: %s" % self.load)
        return node


class Date(Time):
    """ wrapper to add date """

    def __init__(self, arg, mask=False):
        super(Date, self).__init__(arg)
        self.mask = mask

    def add_to(self, node):
        if USE_TIME and self.load is not None:
            # ??? emos avoids dates, datasvc would not
            if self.mask:
                node.load.add_variable("DATEMASK", self.load)
            else:
                ddd, mmm, yyy = self.load.split('.')
                if ddd == '*':
                    ddd = 0
                if mmm == '*':
                    mmm = 0
                if yyy == '*':
                    yyy = 0

                node.load.add_date(int(ddd), int(mmm), int(yyy))
        return node


class Day(Date):
    """ wrapper to add day """

    def add_to(self, node):
        if not USE_TIME or self.load is None:
            return
        if isinstance(self.load, str):
            days = {"monday": ecflow.Days.monday,
                    "sunday": ecflow.Days.sunday,
                    "tuesday": ecflow.Days.tuesday,
                    "wednesday": ecflow.Days.wednesday,
                    "thursday": ecflow.Days.thursday,
                    "saturday": ecflow.Days.saturday,
                    "friday": ecflow.Days.friday,

                    "mon": ecflow.Days.monday,
                    "sun": ecflow.Days.sunday,
                    "tue": ecflow.Days.tuesday,
                    "wed": ecflow.Days.wednesday,
                    "thu": ecflow.Days.thursday,
                    "sat": ecflow.Days.saturday,
                    "fri": ecflow.Days.friday, }
            node.load.add_day(ecflow.Days(days[self.load]))
        else:
            node.load.add_day(ecflow.Days(self.load))
        return node


class Defcomplete(Attribute):  # seasonal
    """ wrapper to add defstatus complete """

    def __init__(self):
        pass

    def add_to(self, node):
        node.defstatus("complete")


class DefcompleteIf(Defcomplete):  # seasonal
    """ wrapper to add conditional defstatus complete 
    just change name to make it explicit
    """

    def __init__(self, arg=True):
        # super(DefcompleteIf, self).__init__()
        self.data = arg

    def add_to(self, node):
        if self.data:
            node.defstatus("complete")
        else:
            node.defstatus("queued")


class Defstatus(Attribute):
    """ add defstatus attribute """

    def __init__(self, kind):
        if type(kind) == str:
            kinds = {
                "suspended": ecflow.DState.suspended,
                # "halted": ecflow.DState.halted,
                # "shutdown": ecflow.DState.shutdown,
                "aborted": ecflow.DState.aborted,
                "complete": ecflow.DState.complete,
                "active": ecflow.DState.active,
                "submitted": ecflow.DState.submitted,
                "unknown": ecflow.DState.unknown,
                "queued": ecflow.DState.queued, }
            self.load = kinds[kind]
        elif type(kind) in (ecflow.DState, ):
            self.load = kind
        else:
            raise DefError(type(kind), kind)

    def add_to(self, node):
        # if node.name() == "ml" and ecflow.DState.complete==self.load: raise
        # Exception  # DEBUG
        if type(node) in (Suite, Family, Task):
            node.load.add_defstatus(self.load)
        else:
            node.add_defstatus(self.load)
        return node


SORT = False


def sorted_or_not(items, sort=False):
    if SORT or sort:
        return sorted(items)
    return items


class Limit(Attribute):
    """ wrapper to add limit """

    def __init__(self, name=None, size=1, inlimit=0):
        self.name = name
        self.size = int(size)
        self.addi = inlimit

    def add_to(self, node):
        if USE_LIMIT and self.name is not None:
            def add_limit(name, size):
                node.load.add_limit(minus2underscore(name), size)
                if self.addi:
                    node.load.add_inlimit(name)

            if type(self.name) is dict:
                for name, size in sorted_or_not(list(self.name.items()),
                                                sort=not LINKTASK):
                    add_limit(name, self.name[name])
            else:
                add_limit(self.name, self.size)

        return node


class Limits(Attribute):   # seasonal
    """ dedicated class to enable limits addition with different syntax """

    def _set_tvar(self, key, val):
        """ append limits """
        if self.data is None:
            self.data = Limit(key, val)
        else:
            next = self.__next__
            self.__next__ = Limits(key, val, next)

    def __init__(self, __a=None, __b=None, __next=None, *args, **kwargs):
        self.data = None
        self.__next__ = __next

        if len(args) > 0:
            if type(args) == list:
                for item in list(args.items()):
                    self._set_tvar(item.name(), item.value())
            elif type(args) == tuple:
                for key, val in list(args.items()):
                    self._set_tvar(key, val)
        elif len(kwargs) > 0:
            for key, val in list(kwargs.items()):
                self._set_tvar(key, val)
        elif type(__a) == dict:
            for key, val in list(__a.items()):
                self._set_tvar(key, val)

        if __a is not None and __b is not None:
            self._set_tvar(__a, __b)

    def add_to(self, node):
        if self.data is not None:
            # node.add(Limit(self.data))
            node.add(self.data)
        if self.__next__ is not None:
            self.__next__.add_to(node)


class Late(Attribute):
    """ wrapper around late, to be add'ed to families and tasks """

    def __init__(self, arg):
        self.load = None
        if not USE_LATE:
            # print("#MSG: late is disabled")
            return
        sub = False
        act = False
        com = False
        rel = False
        self.load = ecflow.Late()
        for item in arg.split(" "):
            if item == "-s":
                sub = True
            elif item == "-c":
                com = True
            elif item == "-a":
                act = True
            else:
                hour, mins = item.split(":")
                rel = "+" in hour
                if "+" in hour:
                    hour = hour[1:]
                if sub:
                    self._add_sub(hour, mins)
                elif com:
                    self._add_com(hour, mins, rel)
                elif act:
                    self._add_act(hour, mins)
                sub = False
                act = False
                com = False

    def _add_sub(self, hour, mins):
        """ submitted """
        self.load.submitted(ecflow.TimeSlot(int(hour), int(mins)))

    def _add_com(self, hour, mins, rel):
        """ complete """
        self.load.complete(ecflow.TimeSlot(int(hour), int(mins)), rel)

    def _add_act(self, hour, mins):
        """ active"""
        self.load.active(ecflow.TimeSlot(int(hour), int(mins)))

    def add_to(self, node):
        if USE_LATE and self.load is not None:
            if type(node) in (ecflow.Family, ecflow.Task):
                node.add_late(self.real)
            else:
                node.real.add_late(self.real)
        return node


vrecord = dict()


def python_true(key, val):
    if "%s" % val == "True":
        if key in("HYPERTHREADING",
                  "CLIM5YR",
                  "FLEX_SUBMIT",
                  "USE_HUGEPAGE"):
            return  # OK
        if key not in list(vrecord.keys()):
            print("#WAR: really???", key, val)
        vrecord[key] = val


class Edit(Attribute):
    """ dedicated class to enable variable addition with different
    syntax """

    def __init__(self, name, value=""):
        self.load = ecflow.Edit(name, value)

    def _set_tvar(self, key, val):
        """ to facilitate loading a ecflow suite to SMS, 
        translating variable names"""
        keyt, edit = translate(str(key), str(val))
        # if keyt == "SCHOST" and val == "ccb": raise DefError
        if self.load is None:
            self.load = ecflow.Variable(keyt, edit)
        else:
            next = self.__next__
            self.__next__ = Edit(keyt, edit, next)

    def __init__(self, __a=None, __b=None, __next=None, *args, **kwargs):
        self.load = None
        self.__next__ = __next

        if len(args) > 0:
            if type(args) == list:
                for item in list(args.items()):
                    key = item.name()
                    val = item.value()
                    python_true(key, val)
                    self._set_tvar(item.name(), item.value())
            elif type(args) == tuple:
                for key, val in list(args.items()):
                    python_true(key, val)
                    self._set_tvar(key, val)
            else:
                raise DefError
        if len(kwargs) > 0:
            for key in sorted_or_not(list(kwargs.keys())):
                val = kwargs[key]
                python_true(key, val)
                self._set_tvar(key, val)
        if type(__a) == dict:
            for key in sorted_or_not(__a.keys()):
                val = __a[key]
                python_true(key, val)
                self._set_tvar(key, val)
        elif type(__a) in (tuple, list):
            raise DefError
        elif type(__a) == Variable:
            self.load = __a
        elif __a is not None and __b is not None:
            python_true(__a, __b)
            self._set_tvar(__a, __b)
        elif __a is None and __b is None:
            pass
        else:
            raise DefError(__a, __b, __next, args, kwargs)

    def add_to(self, node):
        if self.load is not None:
            edit = "%s" % self.load
            if "CPUTIME" in edit and not CPUTIME:
                pass
            if node in (Suite, Family, Task):
                node.load.add_variable(self.load)
            else:
                node.add_variable(self.load)
            if node.name() == "an" and self.load.name() == "NPOOLS" and self.load.value() == "704":
                raise Exception
            if node.name() == "control" and self.load.name() == "FCLENGTH" and self.load.value() == "18":
                raise Exception()
            if node.name() == "o" and "QUEUE 'emos" in edit:
                raise DefError(node.name(), edit)  # intercept, DEBUG

            if 1:
                labels = {"WSHOST": "infopws",
                          "SCHOST": "infopsc",
                          "HOST": "infophs", }
                for key in labels.keys():
                    info = labels[key]
                    try:
                        msg = ""
                        find = ""
                        if "ECF_JOB_CMD" in edit:
                            find = "%" + key + "%"
                            info = "infopcmd"
                            msg = key
                        elif "ECF_KILL_CMD" in edit:
                            find = "edit %s " % key
                        already = False
                        if type(node) in (Family, Task):
                            for label in node.real.labels:
                                if label.name() == info:
                                    already = True
                        elif type(node) in (ecflow.Family, ecflow.Task):
                            for label in node.labels:
                                if label.name() == info:
                                    already = True
                        if find != "" and find in edit and not already:
                            node.real.add_label(info, msg)
                    except:
                        # raise DefError("duplicated", "edit", edit)
                        # print("#WAR: wont duplicate label:", info, edit)
                        # print(sys.exc_info()[0])
                        pass
        if self.__next__ is not None:
            self.__next__.add_to(node)
        return node

    def add(self, what):
        print("#ERR: add???", "%s" % self.load)
        if type(what) in (Suite, Family, Task):
            raise DefError(what.fullname())
        else:
            raise DefError(type(what))


class Variable(Edit):
    pass


class Variables(Edit):
    pass

repeats = {
    ecflow.RepeatDate: "date",
    ecflow.RepeatInteger: "integer",
    ecflow.RepeatString: "string",
    ecflow.RepeatEnumerated: "enumerated",
    ecflow.RepeatDay: "day",

    # ecflow.ecflow.RepeatDate: "date",
    # ecflow.ecflow.RepeatInteger: "integer",
    # ecflow.ecflow.RepeatString: "string",
    # ecflow.ecflow.RepeatEnumerated: "enumerated",
    # ecflow.ecflow.RepeatDay: "day",
    # ecflow.ecflow.Repeat: "date",  # ???
}


class Repeat(Attribute):
    """ repeat date/int/string/enum """

    def __init__(self, name="YMD", start=20120101, end=21010101,
                 step=1, kind="date"):
        # print("repeat", name, "#", start, "#", end, "#", step, "#", kind)
        if len(name) > 4:
            if (name[:4] == "enum" or
                    name[:4] == "date" or
                    "string " in name or
                    name[:3] == "int" or
                    "day " in name):
                pars = name.split(" ")
                kind = pars[0]
                name = pars[1]
                if "enum" in kind or "string" in kind:
                    start = [i.strip("'").strip('"') for i in pars[2:]]
                elif "int" in kind or "date" in kind:
                    start = pars[2]
                    if len(pars) > 3:
                        end = pars[3]
                    if len(pars) > 4:
                        step = pars[4]

        if kind in ("date", ):
            self.load = ecflow.RepeatDate(
                name, int(start), int(end), int(step))
        elif "int" in kind:
            self.load = ecflow.RepeatInteger(
                name, int(start), int(end), int(step))
        elif kind == "string":
            if len(start) == 0:
                raise DefError("start is 0")
            self.load = ecflow.RepeatString(name, start)
        elif "enum" in kind:
            if len(start) == 0:
                raise DefError("start is 0")
            self.load = ecflow.RepeatEnumerated(name, start)
        elif kind == "day":
            self.load = ecflow.RepeatDay(step)
        else:
            self.load = None

    def add_to(self, node):
        if self.load is not None:
            node.load.add_repeat(self.load)
        return node


class Zombie(Attribute):
    """
 |     ZombieAttr(ZombieType,ChildCmdTypes, ZombieUserActionType, lifetime)
 |        ZombieType            : Must be one of ZombieType.ecf, ZombieType.path, ZombieType.user
 |        ChildCmdType          : A list(ChildCmdType) of Child commands. Can be left empty in
 |                                which case the action affect all child commands
 |        ZombieUserActionType  : One of [ fob, fail, block, remove, adopt ]
 |        int lifetime<optional>: Defines the life time in seconds of the zombie in the server.
 |                                On expiration, zombie is removed automatically
"""

    def __init__(self, arg="ecf:remove:3600"):
        if "zombie " in arg:
            arg = nat(arg, "zombie")
        typ, act, kid, num = arg.split(":")
        types = ecflow.ZombieType.names  # ecf user path
        kids = ecflow.ChildCmdType.names  # complete label init abort event wait
        acts = ecflow.ZombieUserActionType.names  # adopt remove kill fob fail block
        when = []
        if kid == "":
            pass
        elif "," in kid:
            for k in kid.split(","):
                when.append(kids[k])
        else:
            when.append(kids[kid])

        self.load = ecflow.ZombieAttr(types[typ], when, acts[act],
                                      int(num))

    def add_to(self, node):
        """add_zombie"""
        node.load.add_zombie(self.load)
        return node


def If(test=True, then=None, otow=None, *args):
    """ enable Task("t1").add(If(test=(1==1),
                                 then=Edit(ONE=1),
                                 otow=Edit(TWO=2)))
        appreciate that both branches are evaluated, using this If class
        ie there is no 'dead code' as it is with python language 'if' structure

        using If to distinguish od/rd mode request that both users share
        the variables (parameter.py) and ecf.py

        otow: on the other way?
        """
    if len(args) > 1:
        try:
            print("#ERR:", then.name())
            print("#ERR:", otow.name())
        except Exception as exc:
            print("#ERR:", exc.args)
        raise DefError("test",
                       test, "\nthen\n",
                       then, "\nelse",
                       otow, "arg", args)
    if test:
        return then
    return otow


class Root(object):  # from where Suite and Node derive
    """ generic tree node """

    load = None

    def __init__(self, **kwargs):
        # self.load = None  # to be filled with ecFlow item
        # CWN(self)
        for key in kwargs:
            if key in list(attr.keys()):
                self.add(attr[key](kwargs[key]))

    @property
    def real(self):
        return self.load

    def __enter__(self):
        raise DefError("#ERR: this shall be overwritten by kid")

    def __exit__(self, type, value, traceback):
        CWN.pop()

    def __get_attr__(self, attr):
        return getattr(self.load, attr)

    def get_parent(self):
        return self.load.get_parent()

    def __str__(self):
        return self.fullname()

    def __repr__(self):
        return "%s" % self.load

    def is_eq(self, node):
        return super(self, self.__eq__), (self, node)

    def __eq__(self, node):
        if isinstance(self.load, ecflow.Node):
            return "%s == " % self + str(node)
        return self  # False

    def __ne__(self, node):
        if isinstance(self.load, ecflow.Node):
            return "%s != " % self + str(node)
        return False

    def __and__(self, node):
        if self == node:
            return True
        if isinstance(self.load, ecflow.Alias):
            return True
        if isinstance(self.load, ecflow.Node):
            return "%s and " % self + str(node)
        return False

    def __or__(self, node):
        if isinstance(self.load, ecflow.Alias):
            return True
        if isinstance(self.load, ecflow.Node):
            return "%s or " % self + str(node)
        return False

    def get_abs_node_path(self):
        return self.fullname()

    def fullname(self):
        """ simple syntax """
        if isinstance(self.load, ecflow.Node):
            return self.load.get_abs_node_path()
        return str(self)

    def repeat(self, name="YMD", start=20120101, end=20321212, step=1,
               kind="date"):
        """ add repeat attribute"""
        return self.add(Repeat(name, start, end, step, kind))

    def defstatus(self, kind):
        """ add defstatus attribute"""
        return self.add(Defstatus(kind))

    def __add__(self, item):
        self.add(item)

    def append(self, item=None, *args):
        """ get compatible with list """
        return self.add(item, args)

    def add(self, item=None, *args):
        """ add a task, a family or an attribute """
        if DEBUG:
            print(self.fullname(), item, args)

        if item is not None:
            if type(item) in (tuple, list):
                for val in item:
                    self.add(val)
            elif type(item) == str:
                raise DefError(item)
            elif type(item) == int:
                pass
            elif type(item) == dict:
                if item != {}:
                    raise DefError(item)
            else:
                item.add_to(self)

        if len(args) > 0:
            if type(args) in (tuple, list):
                for val in args:
                    self.add(val)
            else:
                raise DefError
            return self

        if not isinstance(self.load, ecflow.Node):
            raise DefError("please dont", type(self))

        return self

    def limit(self, name=None, size=1, inlimit=0):
        """ add limit attribute"""
        return self.add(Limit(name, size, inlimit))

    def inlimit(self, fullpath, token=1):
        """ add inlimit attribute"""
        return self.add(Inlimit(fullpath, tokens))

    # follow pyflow
    shape = None

    def draw_tree(self):
        dot = Dot(fullnames=False)
        self._tree(dot)
        return dot

    def _tree(self, dot):
        try:
            dot.edge(self.load.get_parent(), self)
        except:
            pass
        if type(self.load) in (Alias, ecflow.Alias):
            return
        for n in self.load.nodes:
            if n.name() != '_':
                _tree(dot, n)
            # if n.names[0] != '_': n._tree(dot)

    def draw_graph(self):
        dot = Dot()
        self._graph(dot)
        dot.save("test.gv")
        return dot

    def _graph(self, dot):
        if type(self.load) in (ecflow.Alias, Alias):
            return
        for n in self.load.nodes:
            if n.name() != '_':
                _tree(dot, n)
        # for n in self._nodes.values():
        #  if n.names[0] != '_': n._tree(dot)

    def to_html(self):
        return "%s" % HTMLWrapper("%s" % self)

    def _repr_html_(self):
        return str(self.to_html())


def _tree(dot, node):
    if type(node) == ecflow.Alias:
        return
    if type(node) in (ecflow.Node, ecflow.Family, ecflow.Task,
                      ecflow.Alias):
        dot.edge(node.get_parent(), node)
        for n in node.nodes:
            if n.name() != '_':
                _tree(dot, n)
    elif type(node) in (Node, Family, Task, Alias):
        dot.edge(node.load.parent, node)
        _tree(dot, node.load)
    else:
        raise DefError(type(node))
    # try: dot.edge(node.load.parent, self)
    # except: pass


def get_kind(item):
    if type(item) in (Defs, ecflow.Defs):
        return "definition"
    if type(item) in (Suite, ecflow.Suite):
        return "suite"
    if type(item) in (Family, ecflow.Family):
        return "family"
    if type(item) in (Task, ecflow.Task, ):
        return "task"
    if type(item) in (Alias, ecflow.Alias, ):
        return "alias"
    return "ignore"


global FIS
FIS = "SARP"

STATUSES = {"U": "unknown",
            "Q": "queued",
            "S": "submitted",
            "R": "active",
            "A": "aborted",
            "C": "complete",
            "P": "suspended",
            "H": "halted",
            "D": "shutdown",
            }
ST2C = dict((v, k) for k, v in list(STATUSES.items()))


def fis_filter(status):
    """ yep, filter node out """
    global FIS
    if FIS is None:
        FIS = "SARP"
    # if not FIS: return True
    if "unknown" in status:
        status = "unknown"
    if ST2C[status] in FIS or status in FIS:
        return False
    return True


def to_d3js(node, depth=7):
    kids = []
    FIS = "RASPQCU"
    FIA = None
    if type(depth) == int:
        depth = depth - 1
        if depth < 0:
            return {}
    elif type(depth) == str:
        try:
            depth = int(depth)
        except:
            depth = 5
            print("#WAR: depth reset to 5", depth)
    elif type(depth) == dict:
        if "depth" not in list(depth.keys()):
            depth['depth'] = 5
        depth['depth'] = int(depth['depth']) - 1
        if depth['depth'] < 0:
            return {}
        if "fis" in list(depth.keys()):  # filter states
            if depth["fis"] in ("oper", "ops"):
                FIS = "SARP"
            elif depth["fis"] in ("adm", ):
                FIS = "RASPQCU"
            else:
                FIS = depth['fis']
                print("#MSG: FIS", depth)
        # if "fia" in depth.keys():  # filter attributes
        # if fia != "none": FIA = depth['fia']; print("#MSG: FIA", depth)
    if type(node) in(ecflow.Alias, Alias, ):
        name = node.name()
        status = "%s" % node.get_state()
    elif type(node) in (ecflow.Defs, ):
        name = '/'  # aka definition
        kids = [to_d3js(item, depth) for item in node.suites]
        status = "%s" % node.get_state()
    elif type(node) in (ecflow.Family, ecflow.Suite, ):
        kids = [to_d3js(item, depth) for item in node.nodes]
        name = node.name()
        status = "%s" % node.get_state()
    elif type(node) in (Defs, ):
        name = '/'  # aka definition
        kids = [to_d3js(item, depth) for item in node.suites()]
        status = "%s" % node.get_state()
    elif type(node) in (Family, Suite, ):
        kids = [to_d3js(item, depth) for item in node.nodes()]
        name = node.name()
        status = "%s" % node.get_state()
    elif type(node) in (ecflow.Task, Task):
        name = node.name()
        status = "%s" % node.get_state()
    elif node is None:
        return {'children': [],
                'kind': "none",
                'name': "none",
                '_status': 'unknown',
                'size': 1, }
    else:
        raise Exception(type(node))
    # if fis_filter(status): return {}
    try:
        if ST2C[status] not in FIS:
            return {}
    except:
        pass
    return {'children': kids,
            'kind': get_kind(node),
            'name': '%s' % name,
            '_status': '%s' % status,
            'size': 1 + len(kids), }


def to_pyflow(node, container=None):
    if container is None:
        container = dict()
    if type(node) in (tuple, list):
        for item in node:
            container.update(to_pyflow(item, container))
        return container

    if type(node) in (Alias, ecflow.Alias):
        return
    if node.name() not in list(container.keys()):
        container[node.name()] = dict()
    # steps follow
    upd = {
        'variables': dict(),
        'events': [(item.number(), item.name()) for item in node.events],
        'meters': dict(),
        'labels': dict(),
        'limits': dict(),
        'inlimits': dict(),
    }
    for item in node.variables:
        upd['variables'][item.name()] = item.value()
    for item in node.meters:
        upd['meters'][item.name()] = [item.min(), item.max()]
    for item in node.labels:
        upd['labels'][item.name()] = item.value()
    for item in node.limits:
        upd['limits'][item.name()] = item.limit()
    for item in node.inlimits:
        arg = item.name()
        if item.path_to_node() != "":
            arg = item.path_to_node() + ":" + item.name()
        if item.tokens() > 1:
            arg += " %d" % item.tokens()
        upd['inlimits'][item.name()] = arg
    for key in list(upd.keys()):
        if len(upd[key]) == 0:
            del upd[key]
    # repeat time date days today
    if node.get_autocancel():
        upd['autocancel'] = True
    if '%s' % node.get_defstatus() != 'queued':
        upd['defstatus'] = '%s' % node.get_defstatus()
    if node.get_late() != None:
        upd['late'] = nat('%s' % node.get_late(), "late")
    for item in node.nodes:
        upd.update(to_pyflow(item, container[node.name()]))

    container['%s' % node.name()].update(upd)
    return container


def nat(name, key):
    res = '%s' % str(name)
    return res.replace('%s ' % key, '')


def to_dict(node, container=None):
    kids = dict()
    if type(node) is ecflow.Defs:
        res = {':suites': [], }
        for item in node.suites:
            res[':suites'].append(to_dict(item))
        for item in node.externs:
            if ':externs' not in list(res.keys()):
                res[':externs'] = []
            res[':externs'].append("%s" % item)
        return res

    if type(node) in (ecflow.Alias, Alias):
        print("#WAR: aliases are ignored")
        return dict()

    if type(node) == Suite:
        node = node.real
    if type(node) in (ecflow.Suite, ecflow.Family):
        for item in node.nodes:
            kids[item.name()] = to_dict(item)
    elif type(node) in (ecflow.Task, ecflow.Alias):
        pass
    else:
        raise DefError(type(node))

    temp = {'edits': {item.name(): '%s' % item.value()
                      for item in node.variables},
            'events': ["%d " % item.number() + '%s' % item.name()
                       for item in node.events],
            # 'externs': [],
            'meters': [
                {'%s' % item.name(): {
                    'min': item.min(),
                    'max': item.max(),  # aka threshold(),
                    'thr': item.color_change(), }}
                for item in node.meters],
            'labels': {item.name(): '%s' % item.value()
                       for item in node.labels},
            'limits': {item.name(): item.limit() for item in node.limits},
            'inlimits': [],
            'verifies': ["%s" % item for item in node.verifies],
            'zombies': ["%s" % item for item in node.zombies],
            'dates': [nat(item, 'date') for item in node.dates],
            'days': [nat(item, 'day') for item in node.days],
            'times': [nat(item, 'time') for item in node.times],
            'crons': [nat(item, 'cron') for item in node.crons],
            'todays': [nat(item, 'today') for item in node.todays],
            'trigger': '%s' % node.get_trigger(),
            'complete': '%s' % node.get_complete(),
            'children': kids,
            # 'late': "%s" % node.get_late(),
            # 'clock': '%s' % node.get_clock(),
            }

    for item in node.inlimits:
        arg = item.name()
        if item.path_to_node() != "":
            arg = item.path_to_node() + ":" + item.name()
        if item.tokens() > 1:
            arg += " %d" % item.tokens()
        temp['inlimits'].append(arg)
    out = {':name': '%s' % node.name(),
           ':kind': '%s' % get_kind(node),
           ':status': '%s' % node.get_state(), }
    defstatus = '%s' % node.get_defstatus()
    if defstatus != 'queued':
        out[':defstatus'] = "%s" % defstatus
    if get_kind(node) == "suite":
        out[':clock'] = nat("%s" % node.get_clock(), "clock")
    if node.get_late() is not None:
        out[':late'] = nat("%s" % node.get_late(), "late")
    rep = node.get_repeat()
    if not rep.empty():
        out[':repeat'] = nat("%s" % node.get_repeat(), "repeat")

    # for key in sorted(temp.keys()):
    for key in list(temp.keys()):
        if temp[key] == 'None':
            continue  # WARNING ???
        out[':' + key] = temp[key]

    return out


def to_json(item, pyflow=False):
    import json
    if type(item) in (list, tuple):
        pass
    elif type(item) != dict:
        item = to_dict(item, pyflow)
    return json.dumps(item,
                      # default= lambda o:
                      ensure_ascii=True,
                      skipkeys=True,
                      sort_keys=True,
                      indent=2)


def json_to_defs(treedict, parent=None):
    if treedict is None:
        return
    res = []
    if ":externs" in treedict.keys():
        for item in treedict[":externs"]:
            res.append("extern %s" % item)
            # Extern(nat(item, "extern")))
    if ":suites" in treedict.keys():
        for suite in treedict[":suites"]:
            res.append(from_json(suite))
    for key in treedict.keys():
        if key not in (":externs", ":suites"):
            raise DefError("please use from_json", key)
    return res


def from_json(tree):
    out = []
    res = None

    if type(tree) in (tuple, list):
        if tree == []:
            return  # IGN
        if len(tree) == 2:
            if type(tree[0]) == dict:
                return from_json(tree[0])
            return {str(tree[0]): from_json(tree[1])}
        elif len(tree) == 1:
            return str(tree[0])
        raise DefError("#wwwwww", tree, type(tree))

    elif type(tree) in (str, # unicode
    ):
        print("#IGN", tree)
        return

    elif type(tree) is not dict:
        raise DefError("#wwwwww", tree, type(tree))

    for k in sorted(list(tree.keys())):
        # if type(k) == unicode:            sk = str(k)        else:
        sk = k
        # if type(tree[k]) == unicode:            tree[k] = str(tree[k])
        if sk in (':name', ):
            res = ITEMS[tree[':kind']](str(tree[':name']))

        elif sk in (':status', ':kind'):
            continue

        elif sk in (':children', ":kids", ):
            for kid in sorted(list(tree[k].keys())):
                out.append(from_json(tree[k][kid]))

        elif sk in (":suites", ):
            for item in tree[k]:
                out.append(from_json(item))

        elif sk in (':defstatus', ':trigger', ':complete', ':clock',
                    ':late', ':externs', ':repeat',):
            out.append(ITEMS[k](tree[k]))

        elif sk in (':meters', ):
            for item in tree[k]:
                name = item.keys()[0]
                out.append(ITEMS[k](str(name),
                                    int(item[name]['min']),
                                    int(item[name]['max']),
                                    int(item[name]['thr'])))
        elif sk in (':edits', ':labels', ':limits', ):
            for item in sorted(tree[k].keys()):
                out.append(ITEMS[k](str(item), str(tree[k][item])))
        elif sk in (':events', ':repeat', ':inlimits', ':crons',
                    ':verifies', ':dates',
                    ':times', ':days', ':zombies', ":todays", ):
            for item in tree[k]:
                out.append(ITEMS[k](str(item)))
        elif sk in list(ITEMS.keys()):
            raise DefError
        elif (type(tree[k]) == dict and
              len(tree[k]) == 1 and
              ['children', ] == tree[k].keys()):
                kids = tree[k]['children']
                if type(kids) not in (list, tuple):
                    raise DefError(type(kids))
                for kid in kids:
                    if type(kid) in (str, ):
                        out.append((str(k), str(kid)))
                    elif type(kid) in (dict, ):
                        for elt in list(kid.keys()):
                            out.append((str(k), str(elt)))
                        anot = from_json(kid)
                        if anot is not None:
                            out.append(anot)
                return out
        else:
            raise Exception(k, tree[k], type(tree[k]),
                            len(tree[k]),
                            tree[k].keys())
    if res is None:
        return out
    return res.add(out)


class Node(Root):  # from where Task and Family derive
    """ Node class is shared by family and task """

    def __init__(self, **kwargs):
        super(Node, self).__init__(**kwargs)
        # for key in kwargs:
        #     if key in attr.keys():
        #         self.add(attr[key](kwargs[key]))

    def __enter__(self):
        CWN(self)
        return self

    # def __exit__(self, type, value, traceback): pass

    def kids(self):
        try:
            return [item for item in self.load.nodes]
        except:
            print("#WAR: no kids (nodes)")

    def __iadd__(self, item): self.add(item)

    def __add__(self, item): self.add(item)

    def children(self):
        return self.kids()

    def events(self):
        return [item for item in self.load.events]

    def meters(self):
        return [item for item in self.load.meters]

    def labels(self):
        return [item for item in self.load.labels]

    def name(self):
        return self.load.name()

    def find_variable(self, name):
        if self.load:
            return self.load.find_variable(name)
        return None

    def event(self, name=1, value=""):
        """ add event attribute"""
        return self.add(Event(name, value))

    def meter(self, name, beg, end, thr=None):
        """ add meter attribute"""
        return self.add(Meter(name, beg, end, thr))

    def label(self, name, msg=""):
        """ add label attribute"""
        return self.add(Label(name, msg))

    def edit(self, name, value=""):
        """ add variable attribute"""
        return self.add(Edit(name, value))

    def variable(self, name, value=""):
        """ add variable attribute"""
        return self.edit(name, value)

    def cron(self, time, dom=False, wdays=False, month=False):
        """ wrapper for add_cron """
        return self.add(Cron(time, wdays, dom, month))

    def complete(self, arg=None):
        """ add complete attribute"""
        return self.add(Complete(arg))

    def complete_and(self, arg):
        """ append to existing complete"""
        return self.add(Complete(arg))

    def complete_or(self, arg):
        """ append to existing complete"""
        return self.add(CompleteOr(arg))

    def up(self):
        """ get parent, one level up"""
        return self.load.get_parent()

    def __iadd__(self, other):
        self.add(other)
        return self


class Defs(object):
    """ wrapper for the definition """

    def __init__(self, name=None):
        if name is None:
            self.load = ecflow.Defs()
        else:
            self.load = ecflow.Defs(name)

    @property
    def real(self):
        return self.load

    def kids(self):
        return [item for item in self.load.suites]

    def __get_attr__(self, attr):
        return getattr(self.load, attr)

    # def __get_attr__(self, attr): return getattr(self.load, attr)

    def check_job_creation(self):
        return self.load.check_job_creation()

    def auto_add_externs(self, true):
        self.load.auto_add_externs(true)

    def check(self):
        return self.load.check()

    def add_extern(self, path):
        DEFS.real.add_extern(path)
        return None
        # return self.add(Extern(path))

    def save_as_defs(self, fname):
        self.load.save_as_defs(fname)

    def suites(self):
        return self.kids()

    def add_suite(self, node):
        return self.add(node)

    def __str__(self):
        return "%s" % self.load

    __repr__ = __str__

    def __add__(self, item):
        self.add(item)

    def append(self, item=None, *args):
        """ get compatible with list """
        return self.add(item, args)

    def add(self, item):
        """ add suite """
        if type(item) == Suite:
            self.load.add_suite(item.load)
        elif type(item) == Extern:  # back again
            path = "%s" % item
            if type(path) != str:
                raise DefError(type(path), item)
            if ".Extern" in path:
                if 1:
                    return
                raise DefError(type(path), path, item)
            self.load.add_extern(path)
        elif type(item) in (tuple, list):
            for one in item:
                self.add(one)
        elif item is None:
            pass
        elif type(item) in(str, # unicode
        ):
            if "extern " in item:
                Extern(nat(item, "extern"))
            else:
                raise DefError("ERR:load add, what?", type(item), item)
        else:
            raise DefError("ERR:load add, what?", type(item), item)
        return self

    def find_abs_node(self, name):
        return self.load.find_abs_node(name)

    def find_node(self, name):
        return self.get_abs_node(name)

    def suite(self, name):
        """ add suite providing its name """
        suite = Suite(name)
        self.add(suite)
        return suite

    def __iadd__(self, other): self.add(other)


DEFS = Defs()
if os.getenv("ECFLOW_AUTO_EXTERN"):
    DEFS.auto_add_externs(True)
    print("#MSG: AUTO EXTERN")


class Client(object):
    """ wrapper around client """

    def __init__(self, host="localhost", port="31415"):
        if host is None:
            host = 'localhost'
        if port is None:
            port = ECF_PORT
        self.clnt = ecflow.Client()
        if '@' in host:
            host, port = host.split('@')
        elif ':' in host:
            host, port = host.split(':')
        self.real.set_host_port(host, int(port))
        self.host = host
        self.port = port

    @property
    def real(self):
        return self.clnt

    def __get_attr__(self, attr):
        return getattr(self.real, attr)

    def load(self, defs):
        if type(defs) == Defs:
            self.real.load(defs.load)
        elif type(defs) == ecflow.Defs:
            self.real.load(defs)
        else:
            raise DefError("defs: really?")

    def get_file(self, node, kind="script"):
        return self.real.get_file(node, kind)

    def replace(self, path, defs=None, parent=True, force=False):
        if defs is None:
            self.real.replace(path, DEFS, parent, force)
        elif type(defs) is Defs:
            node = defs.real.find_abs_node(path)
            if node is None:
                print("#ERR, node not found", defs)
            self.real.replace(path, defs.load, parent, force)
        else:
            self.real.replace(path, defs, parent, force)

    def suites(self):
        return self.real.suites()

    def ping(self, host=None, port=None):
        print("#PING", host, port)
        if port is None: port = os.getenv("ECF_PORT", 2499)
        if host is None: host = os.getenv("ECF_HOST", "localhost")
        # if host is None: host = "localhost"        
        self.real.set_host_port(host, port)
        self.real.ping()

    def __str__(self):
        return "ecflow client %s@%s v%s" % (
            self.host, self.port, self.version())


class Suite(Root):
    """ wrapper for a suite """

    def __init__(self, name, **kwargs):
        self.load = ecflow.Suite(name)
        # CWN(self)
        super(Suite, self).__init__(**kwargs)

    def __enter__(self):
        CWN(self)
        return self

    # def __enter__(self): return self   # Root

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def add_family(self, node):
        self.add(node)

    def name(self):
        return self.load.name()

    def family(self, name):
        """ add a family """
        if type(name) == int:
            pass
        elif "%" in name:
            raise DefError(name)
        obsolete()
        fam = Family(str(name))
        self.add(fam)
        return fam

    def task(self, name):
        """ add a task """
        if "%" in name:
            raise DefError(name)
        obsolete()
        tsk = Task(name)
        self.add(tsk)
        return tsk

    def add_to(self, defs):
        if type(defs) == ecflow.Defs:
            defs.add_suite(self.real)
        else:
            defs.add(self)  # aka defs.add_suite(self)

    # def __enter__(self): return self

    # def __exit__(self, *args): pass

count = 0


class Family(Node, Attribute):
    """ wrapper around family """

    def __init__(self, name, **kwargs):
        # if name == "an": raise Exception  # DEBUG
        global count
        if name == "main":
            count += 1
            # if count > 0: raise Exception
        self.load = ecflow.Family(str(name))
        # if "enda0" in name: raise Exception
        if name == "pre1crad_cris_1":
            raise Exception
        super(Family, self).__init__(**kwargs)
        # CWN(self)

    def family(self, name):
        """ add a family """
        if "%" in name:
            raise DefError(name)
        obsolete()
        fam = Family(str(name))
        self.add(fam)
        return fam

    def add_family(self, node):
        self.add(node)

    def task(self, name):
        """ add a task """
        if "%" in name:
            raise DefError(name)
        obsolete()
        tsk = Task(name)
        self.add(tsk)
        return tsk

    def add_to(self, node):
        if DEBUG:
            print("#BDG: add: %s %s" % (node.name(), self.name()))
        parent = self.load.get_parent()
        if parent:
            raise DefError("already attached...",
                           parent.name(),
                           node.name(), self.name())
        if type(node) in (Task,):
            raise DefError("would not do that")
        elif type(node) in (Suite, Family):
            # node.load.add_family(self.load)
            # print(type(node.load), type(self.load))
            node.load.add(self.load)
        elif type(node) in (ecflow.Suite, ecflow.Family, ecflow.Task):
            # print(type(node.load), type(self.load))
            node.add(self.load)
        else:
            node.add_family(self.load)

    def nodes(self):
        return self.kids()

    # def __enter__(self): return self

    # def __exit__(self, *args): pass


class Task(Node, Attribute):
    """ wrapper around task """

    def __init__(self, name, **kwargs):
        # super(Task, self).__init__(name)
        self.load = ecflow.Task(name)
        # if name == "fetchmars": raise Exception
        super(Task, self).__init__(**kwargs)
        # CWN(self)

    # def __setattr__(self, key, val):
    #     # assert key.isupper()
    #     if key.isupper():
    #         key, val = translate(key, val)
    #         print(type(self.load), "%s" % self.load)
    #         self.load.add(ecflow.Variable(key, val))

    def add_to(self, node):
        if type(node) in (Family, Suite):
            load = self.load  # deep copy
            if load.get_parent() != None:
                new = load
                load = new
            node.load.add_task(load)
            return
        node.add_task(self.load)

    def add_family(self, node):
        raise DefError(self.name(), node.name(), self.fullname())

    def add_task(self, node):
        raise DefError(node.name(), self.name())


class Alias(Root, ecflow.Alias):  # from where Suite and Node derive

    def __get_attr__(self, attr):
        return None

    def nodes(self):
        return None


class AttributeList(Attribute):

    def __init__(self, items):
        self.store = items

    def add_to(self, node):
        for item in self.store:
            node.add(item)
        return node

# class Alias(Task):    pass


def display(defs, fname=None):
    """ print defs"""
    if fname is not None:
        with open(fname, 'w') as fop:
            print(defs, file=fop)


class TestEcf(unittest.TestCase):
    """ a test case aka use-case """

    def test_edge(self):
        import json
        s = '{"A": {"children": ["B", {"C": {"children": [{"D": {"children": ["E"]}}, "F"]}}]}}'
        data = json.loads(s)
        edges = from_json(data)

        def rec_form(row, fop):
            if len(row) == 0:
                return
            elif len(row) == 1:
                row = row[0]
            if len(row) == 2:
                print('  {0} -> {1};'.format(*row), file=fop)
            else:
                for item in row:
                    rec_form(item, fop)
        with open('tree.dot', 'w') as fop:
            print('\nstrict digraph tree {', file=fop)
            print("#s\n", s, "\n", edges)
            rec_form(edges, fop)
            print('}', file=fop)
        """
        python ecf.py;  # generate tree.dot
        cat tree.dot | dot -Tpng -otree.png && xdg-open tree.png
        """

    def test_xxx(self):
        """ a test """
        CWN.cdp(False)
        suite = Suite("Suite").add(Clock("real"),
                                   Autocancel(3))
        suite.add(Defstatus("suspended"))

        fam = Family("Family")
        tsk = Task("Task")

        ft2 = Task("Fam")
        ft2.add_to(fam)

        tsk.VAR = "VALUE"  # edit VAR "VALUE"
        tsk.add(Late("-s 00:05 -c 01:00"))

        fam.add(tsk,
                (Task("1"), Task("2")),
                [Task("11"), Task("12")],
                Task("111"), Task("211"),
                Task("t2").add(Trigger(tsk == COMPLETE),
                               Late("-s 00:05 -a 01:00 -c 20:00"),
                               Time("01:00")),
                Task("t21").add(Cron("00:00 23:59 01:00")),
                # each 1st 5th ... by 0900
                Task("d").add(Cron("-d 1,5,10,15,20,25 09:00")),
                Task("t22").add(Today("14:00")),
                Task("t23").add(Date("1.*.*")),
                Task("t24").add(Day("monday")),
                )

        fam.add(
            Task("t3").add(
                If(test=(1 == 1),
                   then=Edit(ADD_ONE=1),
                   otow=Edit(ADD_TWO=1)),

                If(test=(1 == 0),
                   then=Edit(ADD_ONE=0),
                   otow=Edit(ADD_TWO=0)),
                Trigger(tsk != ABORTED),
                Complete(tsk == COMPLETE)))  # longer

        fam.add(
            Task("2t"),
            # Task("t4").add(                Trigger(tsk.name() != COMPLETE)),
            Late("-s 00:05 -c 01:00"),
            Edit(VAR="VALUE"),
            Task("t5").add(Trigger(["t4", "t3", "t2"])),
            Task("t6").add(Trigger("2t" == COMPLETE)),
            Task("t7").add(Trigger("2t eq complete")), )

        tsk.add(Limit("a_limit", 10),
                Inlimit("a_task:a_limit"),
                Meter("step", -1, 100),
                Label("info", "none"),
                Event(num=1),
                Event("a"),
                Defstatus("complete"))

        Extern("/oper/main:YMD")
        tsk.add(Edit({"A": "a", "B": "b"}))  # dict
        tsk.add(Edit(D="d", E="e"))  # list name=value,
        tsk.add(Edit("C", "c"))  # name, value
        suite.add(fam,
                  Task("t321").add(Trigger("Family/t2 eq complete"),
                                   TriggerOr("Family/1 eq complete")),
                  Family("main").add(
                      Repeat("YMD", 20200202, 20320202, 7, "date"),
                      Task("doer").add(Time("04:55"))))

        fam.family("fam").add(Defstatus("complete"))

        DEFS.add(suite)
        another = DEFS.suite("suite")
        another.add(Defstatus("suspended"))
        another.task("tn")
        afam = another.family("family")
        afam.task("t2n")

        display(DEFS, fname="test_ecf.tmp")
        suite.draw_tree()
        suite.draw_graph()
        suite.to_html()

        to_json(suite)
        print("%s" % suite)  # print name
        print(suite)  # print name
        print(repr(suite))  # print content
        print(DEFS)  # print content
        cmd = "xdg-open test.gv.pdf;"
        cmd += "dot -Tps test.gv > test.ps && xdg-open test.ps"
        os.system(cmd)

    def test_defs(self):
        import json
        git = os.getenv("GIT_ECFLOW", './')
        if git[-1] != '/':
            git += '/'
        locs = [git + "ANode/parser/test/data/good_defs",
                git + "CSim/test/data/good_defs"]
        global USE_LATE

        USE_LATE = True

        def process_dir(loc):
            for root, dirs, files in os.walk(loc):
                for file in files:
                    if file.endswith(".def"):
                        defs = ecflow.Defs(os.path.join(root, file))
                        for att in ecflow.AttrType.names:
                            defs.sort_attributes(att, True)
                        tree = to_json(to_dict(defs))
                        print("#name", os.path.join(root, file))
                        # print("#defs", defs, tree, type(tree))
                        data = json.loads(tree)
                        # print("#data", data)
                        edges = json_to_defs(data)
                        # print("#edges", edges)

                        DEFS = Defs().add(edges)
                        import suite_diff
                        for suite in defs.suites:
                            for alter in DEFS.real.suites:
                                if suite.name() == alter.name():
                                    suite_diff.REF = suite_diff.Content(
                                        defs, suite.name(), suite)
                                    suite_diff.RES = suite_diff.Content(
                                        DEFS.real, suite.name(), alter)
                                    suite_diff.walk(suite)
                                    suite_diff.RES = suite_diff.Content(
                                        defs, suite.name(), suite)
                                    suite_diff.REF = suite_diff.Content(
                                        DEFS.real, suite.name(), alter)
                                    suite_diff.walk(alter)
                                    break
                        for att in ecflow.AttrType.names:
                            DEFS.real.sort_attributes(att, True)

                        comp = DEFS.real == defs
                        print("#back", comp)
                        if not comp:
                            print("# defs", defs)
                            print("# DEFS", DEFS)  # , data)

                for dir in dirs:
                    process_dir(os.path.join(root, dir))

        for loc in locs:
            process_dir(loc)

    def test_cdp_aka_pyflow(self):
        CWN.cdp(True)
        with Suite("test",  # trigger="1==1"
                   ) as s1:
            with Family("f1"):
                with Task("t1"):
                    Event(num=0, name="")
                    Event(num=1, name="1")
                    Meter("step", -1, 120)
                    Label("info", "msg")
                with Task("t2"):
                    pass
                with Family("f2"):
                    with Task("t3"):
                        pass

                with Task("t321").add(
                        Trigger("Family/t2 eq complete"),
                        TriggerOr("Family/1 eq complete")):
                    pass

                with Task("t123").add(
                        Trigger("Family/t1 eq complete"),
                        Trigger("Family/1 eq complete")):
                    pass
            if DEBUG:
                print("#DBG: up*3")
            with Family("f22"):
                Task("t01")
                if DEBUG:
                    print("#DBG: up")
                with Task("t11"):
                    pass
        if DEBUG:
            print("#DBG: up*2")
        print(s1)
        print(DEFS)
        CWN.cdp(False)

# from pyflow, COMPAT


def definition_to_html(d):
    result = []
    for n in d.split("\n"):
        result.append(n)
    return "<pre>%s</pre>" % ("\n".join(result),)


class HTMLWrapper(object):

    def __init__(self, defs):
        self._def = defs

    def _repr_html_(self):
        return definition_to_html(self._def)

    __str__ = _repr_html_


def shapes(node):
    if type(node) in (ecflow.Task, Task):
        return "triangle"
    elif type(node) in (ecflow.Family, Family):
        return "rect"
    elif type(node) in (ecflow.Suite, Suite):
        return "invtriangle"
    return None


class Dot(object):
    """ follow tracks from pyflow """

    def __init__(self, fullnames=True):
        try:
            from graphviz import Digraph
        except ImportError:
            raise DefError("pip install --user graphviz # ???")
        self._dot = Digraph()
        self._nodes = {}
        self._fullnames = fullnames

    def edge(self, node1, node2):
        self._dot.edge(self.node(node1), self.node(node2))

    def node(self, node):
        full = node.get_abs_node_path()
        if full not in self._nodes:
            self._nodes[full] = '%s' % node.name()
        self._dot.node(self._nodes[full],
                       # fillcolor="shape",
                       # fontcolor="blue",
                       # fontsize=32,
                       # width=0.5,
                       # style="filled",
                       # fixedsize="shape",
                       # fixedsize="true",
                       # label="xxx",
                       shape=shapes(node))
        return self._nodes[full]

    def save(self, path, view=True):
        if os.path.exists(path):
            os.unlink(path)
        self._dot.render(path, view=view)

    # For jupyter notbeook
    def _repr_svg_(self):
        return self._dot._repr_svg_()


ITEMS = {'suite': Suite,
         'family': Family,
         'task': Task,

         ':state': State,
         ':repeat': Repeat,

         ':event': Event,
         ':events': Event,

         ':externs': Extern,
         ':extern': Extern,

         ':meter': Meter,
         ':meters': Meter,

         ':label': Label,
         ':labels': Label,

         ':edit': Edit,
         ':edits': Edit,

         ':inlimit': Inlimit,
         ':limit': Limit,

         ':inlimits': Inlimit,
         ':limits': Limit,

         ':trigger': Trigger,
         ':complete': Complete,
         ':defstatus': Defstatus,
         # ':kids': Limit,

         ':time': Time,
         ':times': Time,
         ':cron': Cron,
         ':crons': Cron,
         ':date': Date,
         ':dates': Date,
         ':day': Day,
         ':days': Day,
         ':today': Today,
         ':todays': Today,

         ':zombies': Zombie,  # ecflow.ZombieAttr,
         ':zombie': Zombie,  # ecflow.ZombieAttr,

         ':late': Late,
         ':verifies': Verify,
         ':clock': Clock,
         ':autocancel': Autocancel,

         ':suites': Suite,  # dict(),
         ':suite': Suite,  # dict(),
         }


attr = {
    "label": Label,  # N
    "meter": Meter,  # N
    "event": Event,  # N
    "inlimit": Inlimit,  # N
    "trigger": Trigger,  # 1
    "clock": Clock,  # 1 suite
    "autocancel": Autocancel,  # 1
    "script": Script,  # 1
    "verify": Verify,  # 1
    "time": Time,  # N
    "defstatus": Defstatus,  # 1
    "limit": Limit,  # N
    "late": Late,  # 1
    "edit": Edit, "variable": Edit,  # N
    "repeat": Repeat,  # 1
    "zombie": Zombie,  # 1
    "family": Family,  # N
    "task": Task,  # N

    "labels": AttributeList,  # Labels,  # N
    "meters": AttributeList,  # Meters,  # N
    "events": AttributeList,  # Events,  # N
    "inlimits": AttributeList,  # Inlimits,  # N
    "times": AttributeList,  # Times,  # N
    "limits": AttributeList,  # Limits,  # N
    "edits": AttributeList,  # Edits,
    "variables": AttributeList,  # Edits,
    "families": AttributeList,  # Families,  # N
    "tasks": AttributeList,  # Tasks,  # N
}


header = """#!/usr/bin/env python
from __future__ import print_function
import sys
try:
  import ecflow
except:
  sys.path.append("/usr/local/bin")
  sys.path.append("/usr/local/apps/ecflow/current/bin")
from ecf import *
suites = []"""

footer = """
defs = Defs()
for item in suites: defs.add_suite(item)
if 0: print(defs)
port= os.getenv("ECF_PORT", None)
client = Client("localhost", port)
client.load(defs)

"""


def spc(depth=0, toprint=""):
    sys.stdout.write(" " * depth)
    sys.stdout.write(toprint + "\n")


def expression(item, kind="Trigger", depth=0):
    if item:
        spc(depth, kind + "('%s')," % item.get_expression())


def notNone(lower, item, none="None", depth=0):
    item = "%s" % item
    if item != none:
        item = item.replace(lower + " ", "")
        spc(depth, lower.capitalize() + "('%s')," % item)


def fpformat_attr(node, depth=0):
    dct = {ecflow.Label: "label",
           ecflow.Event: "event",
           ecflow.Meter: "meter",

           ecflow.InLimit: "inlimit",
           ecflow.Limit: "limit",

           ecflow.Trigger: "trigger",
           ecflow.Complete: "complete",
           ecflow.Clock: "clock",
           ecflow.Autocancel: "autocancel",

           ecflow.Verify: "verify",
           # ecflow.Script: "script",

           ecflow.Time: "time",
           ecflow.Today: "today",
           ecflow.Cron: "cron",
           ecflow.Date: "date",
           ecflow.Day: "day",
           ecflow.Defstatus: "defstatus",
           # ecflow.Late: "late",
           ecflow.Edit: "edit",
           ecflow.Repeat: "repeat",
           # ecflow.Zombie: "zombie",
           }
    # spc(depth, "[")
    for item in node.labels:
        value = item.value()
        if "'" in value:
            value = value.replace("'", '"')
        spc(depth, "%s('%s', '%s')," % (
            dct[type(item)].capitalize(), item.name(), value))
    for item in node.events:
        spc(depth, "%s('%s')," % (
            dct[type(item)].capitalize(), item.name()))
    for item in node.meters:
        spc(depth, "%s('%s', %d, %d)," % (
            dct[type(item)].capitalize(), item.name(), item.min(), item.max()))
    for item in node.inlimits:
        spc(depth, "%s('%s')," % (dct[type(item)].capitalize(), item.name()))
    one = None
    for item in node.limits:
        if one is None:
            spc(depth, "Limits({'%s': %d," % (
                item.name(), item.limit()))
            one = "done"
        else:
            spc(depth, "'%s': '%s'," % (
                item.name(), item.limit()))
    if one is not None:
        spc(depth, "}),")
    expression(node.get_trigger(), "Trigger", depth)
    expression(node.get_complete(), "Complete", depth)

    one = None
    for item in sorted(node.variables):
        if one is None:
            spc(depth, "Edit(")
            one = "done"
        value = item.value()
        if "'" in value:
            value = value.replace("'", '"')
        name = item.name()
        if name[0] not in string.ascii_lowercase and name[0] not in string.ascii_uppercase:
            print("#WAR: int as a name?", item)
            continue
        # try: i = int(name); print("#WAR: int as a name?", item)
        # except: pass  # OK
        spc(depth + 4, "%s='%s'," % (item.name(), value))
    if one is not None:
        spc(depth, "),")
    if isinstance(node, ecflow.Suite):
        notNone("clock", node.get_clock(), depth=depth)
    notNone("autocancel", node.get_autocancel(), depth=depth)
    notNone("defstatus", node.get_defstatus(), "queued", depth)
    notNone("late", node.get_late(), depth=depth)

    item = node.get_repeat()
    if not item.empty():
        line = "%s" % item  # repeats[type(item)]
        expl = line.split(" ")
        kind = expl[1]
        name = expl[2]
        print("# %s" % item)
        if kind in ("enumerated", "string"):
            spc(depth, "Repeat('%s', [%s], kind='%s')," % (
                expl[2], ",".join(expl[3:]),  kind))
        elif kind in ("day", ):
            spc(depth, "Repeat('%s', %d, kind='day')," % (
                name, item.step()))
        elif kind in ("integer", "date"):
            spc(depth, "Repeat('%s', %d, %d, %d, kind='%s')," % (
                name, item.start(), item.end(), item.step(), kind))
        else:
            raise BaseException(item)
    # spc(depth, "],")


def fpformat(node, depth=0):
    """ functional programming format aka ecf.py """
    if isinstance(node, ecflow.Defs):
        print(header)
        for item in node.suites:
            print("suites.append(Suite('%s').add([" % item.name())
            fpformat_attr(item, depth + 2)
            fpformat(item, depth + 4)
            print("]))")
        print(footer)

    elif isinstance(node, Defs):
        print(header)
        for item in node.suites():
            print("suites.append(Suite('%s').add([" % item.name())
            fpformat_attr(item, depth + 2)
            fpformat(item, depth + 4)
            print("]))")
        print(footer)

    elif isinstance(node, ecflow.Suite) or isinstance(node, ecflow.Family):
        for item in node.nodes:
            if isinstance(item, ecflow.Family):
                spc(depth, "Family('%s').add([" % item.name())
                fpformat_attr(item, depth + 2)
                fpformat(item, depth + 4)
                spc(depth, "]),")
            elif isinstance(item, ecflow.Task):
                spc(depth, "Task('%s').add([" % item.name())
                fpformat_attr(item, depth + 2)
                spc(depth, "]),")
            elif isinstance(item, ecflow.Alias):
                pass
            else:
                raise Exception("FIXME", type(node))

    else:
        print("#ERR:", type(node))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-p", "--path", default=None, help="path")
    parsed = parser.parse_args()

    test = Family("seasonal_monthly_plev").add(
        Defstatus("complete"),
        Edit(GITREPO="c3sdev",
             PRODUCT="seasonal-monthly-pressure-levels",),
        Task("genform",
             date="11.*.*", time="12:00"),
        Task("push2prod",
             date="13.*.*", time="12:00",
             trigger="genform == complete"),
    )

    test = Family("seasonal_monthly_plev").add(
        Defstatus("complete"),
        Edit(GITREPO="c3sdev",
             PRODUCT="seasonal-monthly-pressure-levels",),
        Task("genform").add(
            Date("11.*.*"), Time("12:00")),
        Task("push2prod").add(
            Date("13.*.*"), Time("12:00"),
            Trigger("genform == complete")),
    )
    if parsed.path is not None:
        if ".def" in parsed.path:
            defs = Defs(parsed.path)
            # print(defs)
            fpformat(defs)
        else:
            Exception(parsed.path, "???")
    elif parsed.path:
        hostport, path = parsed.path.split(':')
        client = ecflow.Client(hostport)
        client.sync_local()
        defs = client.get_defs()
        fpformat(defs)
        # print(defs)
    else:
        unittest.main()

"""
python ./ecf.py -p 4tar_ball/lag_ecf.def > lag.def
# SyntaxError: more than 255 arguments

https://git.ecmwf.int/projects/IFS/repos/ifs-suites1/browse/ecflow?at=refs%2Fheads%2Fdevelop
export PYTHONPATH=/usr/local/lib/python3.6/site-packages
cd /media
python3 o.def o /o localhost:3141

export PYTHONPATH=/usr/local/lib/python2.7/site-packages
python2 o.def o /o localhost:3141

"""
