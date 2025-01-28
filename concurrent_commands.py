#!/usr/bin/env python3

""" 
author: drandalltaylor@gmail.com
"""

# This module creates an operating system process for each command provided
# by the the user to the 'run' def and returns the exit code for each of
# those processes.  The processes are run in parallel at the same time
# to reduce the total time taken to run all the commands.  The theoretical 
# time complexity is thereby reduced from O(N) to constant time O(1). 
# The unit test shows 255 individual ping commands each requiring 1 second
# uses a total of a few seconds for all of the ping commands to complete.

from multiprocessing import Process, Queue
import os
import sys
import functools
import time
import errno
import gc

def run(cmds, stop, wait_for_exit=True, time_out=None):
    """
    Creates a process for each command and execute the commands at the same time.
    Arg: cmds: list of strings for os.system call or Python callable objects. 
    Arg: stop: callable returning boolean (unused).
    Returns: List of operating system process exit codes of same length as cmds.
    """

    procs = []   # Process objects created by this module
    queues = []  # Used to transfer return/exit codes back to this run function
    exit_codes = [] # Exit code of the Process objects created by this module
    exec_start_time = []

    def loop_until_queue_allocated():
        while True:
            try:
                q = Queue()
                return q
            except OSError as ex:
                if ex.errno == errno.EMFILE:  # Too many files/pipes/queues open (value 24)
                    # Handle the completed processes to free up Queue resources
                    handle_started_procs()
                    gc.collect()
                else:
                    sys.stderr.write("Error: unexpected error: " + str(ex))
                    assert(False)
    def check_command_timeout(start_time):
        if time.time()-start_time>time_out:
            return True
        return False

    def are_all_handled():
        rc = True
        # As each Process object finishes, it is replaced by None in the proc list
        for proc in procs:
            if proc != None:
                rc = False
                break
        return rc 

    def handle_started_procs():
        # Check to see whether the Process object is handled, if it is replace with None in the lists
        time.sleep(0.1)  # give procs a moment more to finish 
        index = 0
        for proc in procs:
            if proc != None and proc.pid != None:  # pid assigned by operating system 
                if not proc.is_alive():  # if process is no longer running in operating system
                    exit_code_queue = queues[index]
                    if not exit_code_queue.empty():
                        exitcode = exit_code_queue.get_nowait()
                        exit_codes[index] = exitcode
                    else:
                        # Cannot happen theoretically, but can happen during app shutdown from main.py when child processes are terminated.
                        #assert(False)
                        exit_codes[index] = -1
                    procs[index] = None
                    queues[index] = None
                elif time_out is not None:
                    if check_command_timeout(exec_start_time[index]):
                        proc.terminate()
                        time.sleep(1.0)
                        procs[index] = None
                        exit_codes[index] = -1
                        queues[index] = None
            index += 1

    # Create and start all the Process objects - they will run in parallel (at same time)
    for cmd in cmds:
        exit_code_queue = loop_until_queue_allocated() 
        p = Process(target=fun, args=(cmd, exit_code_queue), daemon=True)
        if wait_for_exit:
            procs.append(p)
            queues.append(exit_code_queue)
            exit_codes.append(None)
            exec_start_time.append(time.time())
        p.start()
    if not wait_for_exit:
        return [ 0 ]

    # Wait until the Process objects are handled or terminated 
    while not are_all_handled() or stop():
        handle_started_procs()
        if stop():
            for proc in procs:
                if proc != None and proc.is_alive():
                    try:
                        proc.terminate()
                    except:
                        pass
    # Return a list of exit codes of all the processes created
    assert(len(procs) == len(exit_codes))
    assert(len([x for x in exit_codes if x == None]) == 0)
    return exit_codes

def fun(cmd, exit_code_queue):
    """ Function in a new process to either call os.system or a Python 'callable' object (e.g. def) """

    if type(cmd) == str:
        exitcode = os.system(cmd) # pass the user's string to os.system for execution
        exit_code_queue.put(exitcode)
        sys.exit(exitcode)
    elif hasattr(cmd, '__call__'):
        exitcode = cmd() # call the user's callable object (e.g. Python def)
        exit_code_queue.put(exitcode)
        sys.exit(exitcode)
    else:
        assert(False)



# For unit tests in this file - shows that Python def (and os.system) can be used.
if __name__ == "__main__":
    stop = False

    # Test 1: Parallel Test of command strings to pass to os.system

    quiet = " >/dev/null 2>&1"
    cmds = [
        "ping -c 1 -W 1 127.0.0.1" + quiet,
        "ping -c 1 -W 1 0.0.0.0" + quiet,
    ]
    cmd = "ping -c 1 -W 1 8.8.4."
    for num in range(0, 256):
        cmds.append(cmd + str(num) + quiet)
    current = time.time()
    exit_codes = run(cmds, lambda: stop, wait_for_exit=True)
    end = time.time()
    diff = end - current
    print ("\n")
    print(("Result: os.system parallel pings: Processes={} Duration={} ExitCodes={}".format(len(cmds), diff, exit_codes)))
    print ("\n")

    # Test 2: Parallel Test of Python callable ojects (def's)

    test_def_global = 1 
    def test_def(ip):
        quiet = " >/dev/null 2>&1"
        #quiet = ""
        cmd = "ping -c 1 -W " + str(test_def_global) + " " + ip + quiet
        exit_code = os.system(cmd)
        return exit_code

    cmds = [ ]
    cmd = functools.partial(test_def, "127.0.0.1")
    cmds.append(cmd)
    cmd = functools.partial(test_def, "0.0.0.0")
    cmds.append(cmd)
    cmd = functools.partial(test_def, "8.8.4.4")
    cmds.append(cmd)
    current = time.time()
    exit_codes = run(cmds, lambda: stop)
    end = time.time()
    diff = end - current
    print(("Result: def parallel pings: Processes={} Duration={} ExitCodes={}".format(len(cmds), diff, exit_codes)))
