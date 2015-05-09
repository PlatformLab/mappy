from rpc import RPC

import time
from collections import deque

class Job(object):
    def __init__(self, workList, pool, rpcManager, eventQueue):
        self.status = "RUNNING"
        self.setup = False
        self.setup_request_sent = False
        self.setup_abort_sent = False
        self.tasks_complete = False
        self.committed = False
        self.killed = False
        self.workList = workList
        self.taskList = []
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.activate(self)
    
    def applyRules(self):
        if self.status != "RUNNING":
            # In goal state (SUCCEEDED or FAILED); deactivate.
            self.pool.deactivate(self)
        elif self.killed:
            if not self.all_task_done_or_failed():
                # Job was killed but tasks have not died yet; kill all tasks.
                for task in self.taskList:
                    task.kill()
            elif self.setup:
                # Job was killed after setup occured; 
                if not self.setup_abort_sent:
                    self.eventQueue.append(("JOB_ABORT", self))
            else:
                # Job kill is complete; goal reached.
                self.status = "FAILED"
        elif not self.setup:
            # Job not yet setup; request the job be setup.
            if not self.setup_request_sent:
                self.eventQueue.append(("JOB_SETUP", self))
        elif len(self.workList) != len(self.taskList):
            # Tasks not yet created and scheudled; create and schedule tasks.
            self.taskList = [Task(w, self.pool, self.rpcManager, self.eventQueue) for w in self.workList]
        elif not self.tasks_complete:
            # Tasks not complete last iteration; check and commit if complete.
            self.tasks_complete = True
            for task in self.taskList:
                if task.getStatus() == "KILLED_OR_FAILED":
                    self.killed = True
                    self.tasks_complete = False
                    break
                if task.getStatus() == "RUNNING":
                    self.tasks_complete = False
                    break
            if self.tasks_complete:
                self.eventQueue.append(("JOB_COMMIT", self))
        elif not self.committed:
            # Job not yet committed; nothing to do.
            pass
        else:
            # Job completed tasks and committed; goal reached;
            self.status = "SUCCEEDED"
    
    def handleEvents(self, newEvents):
        self.eventsIn += newEvents

        # Turn events into state changes
        for eventType, value in list(self.eventsIn):
            # JobEventType Events
            if eventType == "JOB_SETUP_COMPLETED":
                if self.status == "RUNNING" and not self.setup and value == self:
                    self.setup = True
            if eventType == "JOB_SETUP_FAILED":
                if self.status == "RUNNING" and not self.setup and value == self:
                    self.killed = True
            if eventType == "JOB_COMMIT_COMPLETED":
                if self.status == "RUNNING" and not self.committed and value == self:
                    self.committed = True
            if eventType == "JOB_COMMIT_FAILED":
                if self.status == "RUNNING" and not self.committed and value == self:
                    self.killed = True
            if eventType == "JOB_KILL":
                self.killed = True
            if eventType == "JOB_ABORT_COMPLETED":
                if self.killed and value == self:
                    self.setup = False
            if eventType == "JOB_UPDATED_NODES":
                for task in self.taskList:
                    task.nodeCrash(value)
            if eventType == "JOB_DIAGNOSTIC_UPDATE":
                # Do some update
                pass
        self.eventsIn.clear()
    
    def pushNewEvents(self, newEvents):
        self.eventsIn += newEvents
        
    def all_task_done_or_failed(self):
        for task in self.taskList:
            if task.getStatus() == "RUNNING":
                return False
        return True
    
    def getStatus(self):
        return self.status
        
    def __str__(self):
        s = ""
        for task in self.taskList:
            s = s + str(task) + "\n"
        return s

class Task(object):
    def __init__(self, work, pool, rpcManager, eventQueue):
        self.status = "RUNNING"
        self.commitLocator = None
        self.killed = False
        self.work = work
        self.taskAttempts = []
        self.time = None
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.activate(self)
    
    def applyRules(self):
        if self.status != "RUNNING":
            # In goal state (SUCCEEDED or KILLED_OR_FAILED); deactivate.
            self.pool.deactivate(self)
        elif self.killed:
            if self.all_task_attempts_done_or_failed():
                # Task killed and all TackAttempts have stopped; goal reached.
                self.status = "KILLED_OR_FAILED"
        elif not self.taskResourcesAvailable():
            # Task preconditions not met (missing resources); fail.
            self.kill()
        elif self.commitLocator == None:
            # Task not complete last iteration; check and add attempt if needed.
            for taskAttempt in list(self.taskAttempts):
                if taskAttempt.getStatus() == "FAILED":
                    self.taskAttempts.remove(taskAttempt)
                elif taskAttempt.getStatus() == "SUCCEEDED":
                    if self.commitLocator == None:
                        self.commitLocator = taskAttempt.container
                else:
                    if self.commitLocator != None:
                        taskAttempt.kill()
            if self.shouldAddAttempt():
                self.taskAttempts.append(TaskAttempt(self.work, self.pool, self.rpcManager, self.eventQueue))
        else:
            # Task completed; goal reached.
            self.status = "SUCCEEDED"
            
    def handleEvents(self, newEvents):
        pass
        
    def getStatus(self):
        return self.status
        
    def all_task_attempts_done_or_failed(self):
        for taskAttempt in self.taskAttempts:
            if taskAttempt.getStatus() not in ("SUCCEEDED", "FAILED"):
                return False
        return True
        
    # Some policy for whether an attempt should be issued.
    # Same affect as if an event T_ADD_SPEC_ATTEMPT was generated
    def shouldAddAttempt(self):
        if len(self.taskAttempts) == 0:
            return True
            
    def kill(self):
        self.killed = True
        for taskAttempt in self.taskAttempts:
            taskAttempt.kill()
        self.pool.activate(self)
        
    def nodeCrash(self, container):
        if self.status != "KILLED_OR_FAILED" and container == self.commitLocator:
            self.commitLocator = None
            self.status = "RUNNING"
        for taskAttempt in self.taskAttempts:
            taskAttempt.nodeCrash(container)
        self.pool.activate(self)
    
    # A check to see if the rescoures like the HDFS stored file are all online.
    def taskResourcesAvailable(self):
        return True 
    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.getStatus(), self.commitLocator)

class TaskAttempt(object):
    def __init__(self, work, pool, rpcManager, eventQueue):
        self.work = work
        self.status = "RUNNING"
        self.container = None
        self.container_requested = False
        self.launch_rpc = None
        self.commit_rpc = None
        self.cleanup_rpc = None
        self.time = None
        self.pool = pool
        self.rpcManager = rpcManager
        self.eventQueue = eventQueue
        self.eventsIn = deque()
        # schedule itself to run
        self.pool.activate(self)
        
    def applyRules(self):
        if self.status != "RUNNING":
            # In goal state (SUCCEEDED or FAILED); deactivate.
            self.pool.deactivate(self)
        elif self.container == None:
            # No container allocated; request container.
            if not self.container_requested:
                self.eventQueue.append(("CONTAINER_REQ", (self, None)))
                self.container_requested = True
        elif self.cleanup_rpc != None:   
            # Cleanup requested (attempt killed); ensure cleanup occurs.
            if self.cleanup_rpc.status == "complete":
                if self.cleanup_rpc.reply == "failed":
                    # Cleanup RPC failed; retry.
                    self.cleanup_rpc = RPC(self.container, None, ("CONTAINER_REMOTE_CLEANUP", self.work))
                    self.rpcManager.send(self.cleanup_rpc)
                else:
                    # Cleanup RPC completed; release container, goal reached.
                    self.eventQueue.append(("CONTAINER_DEALLOCATE", (self, self.container)))
                    self.status = "FAILED"
        elif self.launch_rpc == None:
            # Attempt not launched; launch.
            self.launch_rpc = RPC(self.container, None, ("LAUNCH", self.work))
            self.rpcManager.send(self.launch_rpc)
            self.time = time.time()
        elif self.launch_rpc.status != "complete":
            # Attempt running; nothing to do.
            pass
        elif self.launch_rpc.reply == "failed":
            # Attempt failed; report container failure, goal reached.
            self.eventQueue.append(("CONTAINER_FAILED", (self, self.container)))
            self.status = "FAILED"
        elif self.commit_rpc == None:
            # Attempt complete but not committed; request commit.
            self.commit_rpc = RPC(self.container, None, ("COMMIT", self.work))
            self.rpcManager.send(self.commit_rpc)
        elif self.commit_rpc.status != "complete":
            # Attempt committing; nothing to do.
            pass
        elif self.commit_rpc.reply == "failed":
            # Commit failed; report container failure, goal reached.
            self.eventQueue.append(("CONTAINER_FAILED", (self, self.container)))
            self.status = "FAILED"
        else:
            # Commit succeeded; release container, goal reached.
            self.status = "SUCCEEDED"
            self.eventQueue.append(("CONTAINER_DEALLOCATE", (self, self.container)))

                
    def handleEvents(self, newEvents):
        self.eventsIn += newEvents

        # Turn events into state changes
        for eventType, value in list(self.eventsIn):
            # TaskAttemptEventType Events
            if eventType == "TA_ASSIGNED":
                taskAttempt, container = value
                taskAttempt.assignContainer(container)
            if eventType == "TA_KILL":
                taskAttempt, container = value
                taskAttempt.kill()
        self.eventsIn.clear()
    
    def assignContainer(self, container):
        if self.container == None:
            print "Container Assigned: " + str(container) + " to " + str(self.work)
            self.container = container
    
    def kill(self):
        if self.status == "RUNNING" and self.container != None:
            self.cleanup_rpc = RPC(self.container, None, ("CONTAINER_REMOTE_CLEANUP", self.work))
            self.rpcManager.send(self.cleanup_rpc)
        self.pool.activate(self)
        
    def nodeCrash(self, container):
        if self.container == container:
            self.status = "FAILED"
        self.pool.activate(self)
        
    def getStatus(self):
        return self.status
                    
    def __str__(self):
        return "<{0}: {1} {2}>".format(self.work, self.status, self.container)