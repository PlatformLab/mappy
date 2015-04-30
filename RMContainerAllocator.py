from collections import deque

class RMContainerAllocator(object):
    def __init__(self, sessionManager):
        self.assignedServers = []
        self.assignedTasks = {}
        self.sessionManager = sessionManager
        self.eventsIn = deque()
        self.eventsOut = []
    
    def heartbeat(self):
        if len(self.eventsIn) > 0:
            eventType, value = self.eventsIn.popleft()
            if eventType == "CONTAINER_REQ":
                taskAttempt, container = value
                container = self.containerRequest(taskAttempt)
                if container == None:
                    self.eventsIn.append((eventType, value))
                else:
                    self.eventsOut.append(("TA_ASSIGNED", (taskAttempt, container)))
            if eventType in ("CONTAINER_DEALLOCATE", "CONTAINER_FAILED"):
                taskAttempt, container = value
                self.containerDeallocate(taskAttempt, container)
    
    # returns JOB_UPDATED_NODES, JOB_AM_REBOOT, TA_KILL, JOB_KILL
    def getNewEvents(self):
        events = self.eventsOut
        self.eventsOut = []
        return events
        
    def putNewEvents(self, newEvents):
        self.eventsIn += newEvents
    
    # choose a container based on some objective function
    # handles ContainerAllocatorEvent::CONTAINER_REQ functionalility
    def containerRequest(self, taskAttempt):
        # Use really simple allocation scheme
        if taskAttempt in self.assignedTasks:
            return self.assignedTasks[taskAttempt]

        container = None
        for locator in self.sessionManager.serverList():
            if locator not in self.assignedServers:
                container = locator
                break
        if container != None:
            print "Container Chosen for work " + str(taskAttempt.work)
            self.assignedServers.append(container)
            self.assignedTasks[taskAttempt] = container
        return container
    
    # handles ContainerAllocatorEvent::CONTAINER_DEALLOCATE functionalility
    # handles ContainerAllocatorEvent::CONTAINER_FAILED functionalility
    def containerDeallocate(self, taskAttempt, container):
        if container in self.assignedServers:
            self.assignedServers.remove(container)
        if taskAttempt in self.assignedTasks:
            del self.assignedTasks[taskAttempt]