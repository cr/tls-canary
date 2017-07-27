#!/usr/bin/env python

import logging
from multiprocessing import Lock, Process, Queue
from threading import Thread
from uuid import uuid1


logger = logging.getLogger(__name__)


class Event(object):
    """
    Event object

    An event has an event ID, which receivers can opt to listen to,
    and an optional message. The message must be pickle-able.
    """
    def __init__(self, event_id, message=None):
        """
        Event constructor

        The message object must be pickle-able.

        :param event_id: any hashable object like str or int
        :param message: optional pickle-able message object
        """
        self.id = event_id
        self.message = message

    def __repr__(self):
        """
        Human-readable object representation

        :return: str
        """
        # Messages can be very long, so truncate message representation to 100 characters
        msg_str = repr(self.message)
        if len(msg_str) > 100:
            msg_str = "%s..." % msg_str[:97]
        return "<Event(%s, %s)>" % (repr(self.id), msg_str)


# This thread can run in the main process context only
class GlobalDispatcher(Thread):
    """
    Global event dispatcher thread

    It manager a local registry of receivers and listeners
    and distributes events to receivers registered as listening to
    the respective event ID.

    This thread can only run in the main process context.
    """

    def __init__(self):
        """
        GobalDispatcher thread constructor
        """
        super(GlobalDispatcher, self).__init__()
        self.daemon = True
        self.queue = Queue()
        self.receivers = dict()
        self.receivers_lock = Lock()
        self.listeners = dict()
        self.listeners_lock = Lock()

    # This can be called from the main process context only
    def add_receiver(self, receiver_id):
        """
        Add a receiver ID to the local receiver registry and return
        the new event receiver queue object associated to the ID.

        The method can be called from the main process context only.


        :param receiver_id: hashable object
        :return: multiprocessing queue
        """
        logger.debug("Adding event receiver %s" % repr(receiver_id))
        if receiver_id in self.receivers:
            logger.warning("Receiver %s is already registered" % repr(receiver_id))
            return self.receivers[receiver_id]
        queue = Queue()
        with self.receivers_lock:
            self.receivers[receiver_id] = queue
        return queue

    def remove_receiver(self, receiver_id):
        """
        Unregister receiver ID and remove any listeners associated with its
        event receiver queue.

        The method can be called from the main process context only.

        :param receiver_id: hashable object
        :return: None
        """
        logger.debug("Removing receiver %s" % repr(receiver_id))
        if receiver_id not in self.receivers:
            logger.debug("Unable to remove unknown receiver %s" % repr(receiver_id))
            return
        with self.receivers_lock:
            del self.receivers[receiver_id]
            # Remove any events that the receiver was listening to
            for event_id in self.listeners:
                self.listeners[event_id] = filter(lambda r: r != receiver_id, self.listeners[event_id])

    # This can be called from the main process context only
    def add_listener(self, receiver_id, event_id):
        """
        Register a receiver as listening to an event ID.

        After this, respective events will be dispatched to the event receiver queue
        registered for the given receiver ID.

        The method can be called from the main process context only.

        :param receiver_id: hashable object
        :param event_id: hashable object or list thereof
        :return: None
        """
        logger.debug("Registering receiver %s as listening to %s" % (repr(receiver_id), repr(event_id)))
        if type(event_id) is list or type(event_id) is tuple:
            event_ids = event_id
        else:
            event_ids = [event_id]
        if receiver_id not in self.receivers:
            logger.error("Unable to register unknown receiver %s as listening" % repr(receiver_id))
            return
        with self.listeners_lock:
            for event_id in event_ids:
                if event_id not in self.listeners:
                    self.listeners[event_id] = [receiver_id]
                else:
                    if receiver_id not in self.listeners[event_id]:
                        self.listeners[event_id].append(receiver_id)

    # This can be called from the main process context only
    def remove_listener(self, receiver_id, event_id):
        """
        Remove receiver from the listeners.

        After this, respective events will no longer be dispatched to the receiver's
        queue.

        The method can be called from the main process context only.

        :param receiver_id: hashable object
        :param event_id:  hashable object or list thereof
        :return: None
        """
        logger.debug("Removing %s as listener for %s" % (repr(receiver_id), repr(event_id)))
        if type(event_id) is list or type(event_id) is tuple:
            event_ids = event_id
        else:
            event_ids = [event_id]
        if receiver_id not in self.receivers:
            logger.error("Unable to remove listeners for unknown receiver %s" % repr(receiver_id))
            return
        with self.listeners_lock:
            for event_id in event_ids:
                if event_id in self.listeners:
                    self.listeners[event_id] = filter(lambda r: r != receiver_id, self.listeners[event_id])

    # This must run in the main process context
    def run(self):
        """
        The dispatcher thread's entry point

        :return: None
        """
        while True:
            event = self.queue.get()
            if event.id == "start_listening":
                self.add_listener(event.message["receiver_id"], event.message["event_id"])
            elif event.id == "stop_listening":
                self.remove_listener(event.message["receiver_id"], event.message["event_id"])
            elif event.id == "remove_receiver":
                self.remove_receiver(event.message["receiver_id"])
            else:
                if event.id not in self.listeners:
                    logger.warning("Ignoring event %s because no one is listening", repr(event))
                    continue
                for receiver_id in self.listeners[event.id]:
                    logger.debug("Dispatching event %s to receiver %s" % (repr(event), repr(receiver_id)))
                    receiver_queue = self.receivers[receiver_id]
                    receiver_queue.put(event)

    # This is safe to call from subproccesses
    def dispatch(self, event):
        """
        Dispatch a method to the dispatcher's event queue.

        The method is safe to be called from any process context.

        :param event: messaging.Event
        :return: None
        """
        logger.debug("Dispatching event %s to listeners" % repr(event))
        self.queue.put(event)


# Global event dispatcher instance
# There may only be one.
global_dispatcher = GlobalDispatcher()
global_dispatcher.start()


def is_running():
    """
    Check whether the message broker is active

    :return: bool
    """
    return global_dispatcher.is_alive()


# This can be safely called from any subprocess context
def dispatch(event):
    """
    Dispatch an Event object to the global event dispatcher's queue.
    The dispatcher asynchronously delivers the event to every event listener
    queue that is registered to the event's ID.

    The method is safe to be called from any process context.

    :param event: messaging.Event
    :return: None
    """
    global_dispatcher.dispatch(event)


# This can be called from the main process context only
def create_receiver(receiver_id=None):
    """
    Register an event receiver ID with the global event dispatcher
    and return associated receiver ID and receiver queue object that
    events will be dispatched to.

    If no receiver ID is given, a random UUID will be generated.

    The method can be called from the main process context only.

    :param receiver_id: optional hashable object
    :return: (receiver_id or int, multiprocessing.Queue)
    """
    if receiver_id is None:
        receiver_id = uuid1()
    receiver_queue = global_dispatcher.add_receiver(receiver_id)
    return receiver_id, receiver_queue


# This can be safely called from any subprocess context
def remove_receiver(receiver_id):
    """
    Unregister a receiver ID with the global event dispatcher and have it
    delete any associated event listener registrations.

    The method is safe to be called from any process context.

    :param receiver_id: hashable object
    :return: None
    """
    message = {"receiver_id": receiver_id}
    dispatch(Event("remove_receiver", message))


# This can be safely called from any subprocess context
def start_listening(receiver_id, event_id):
    """
    Register an event receiver's ID for listening to a list of event IDs.
    After this, all events that match the registered IDs will be dispatched
    to the event receiver queue that was registered with the global event
    dispatcher under the given receiver ID.

    The method is safe to be called from any process context.

    :param receiver_id: hashable object
    :param event_id: hashable object or list thereof
    :return: None
    """
    message = {"receiver_id": receiver_id, "event_id": event_id}
    dispatch(Event("start_listening", message))


# This can be safely called from any subprocess context
def stop_listening(receiver_id, event_id):
    """
    Stop listening to to given event ID or IDs

    The method is safe to be called from any process context.

    :param receiver_id: hashable object
    :param event_id: hashable object or list thereof
    :return: None
    """
    message = {"receiver_id": receiver_id, "event_id": event_id}
    dispatch(Event("stop_listening", message))


class MessagingProcess(Process):
    """Wrapper for the Process class that supports IPC messaging"""

    def __init__(self, receiver_id=None, *args, **kwargs):
        """
        MessagingProcess constructor

        It registers a unique event receiver queue with the global event
        dispatcher, but otherwise it works just like multiprocessing.Process.

        Messaging sub-processes must be created from the main process.
        """
        super(MessagingProcess, self).__init__(*args, **kwargs)
        self.__receiver_id, self.__event_queue = create_receiver(receiver_id)

    def events_pending(self):
        """
        Return whether there are pending events in the event receiver queue

        :return: bool
        """
        return not self.__event_queue.empty()

    def receive(self, block=True, timeout=None):
        """
        Get an event from the event receiver queue

        :param block: bool
        :param timeout: float
        :return: messaging.Event
        """
        return self.__event_queue.get(block=block, timeout=timeout)

    def start_listening(self, event_id):
        """
        Register process as listening to the given event ID or IDs.

        After this, you will start seeing events with the respective IDs
        start showing up in the process' event receiver queue.

        See MessagingProcess.receive()

        :param event_id: hashable object or list thereof
        :return: None
        """
        start_listening(self.__receiver_id, event_id)

    def stop_listening(self, event_id):
        """
        Tell the global event dispatcher to stop dispatching specified event IDs
        to the process' event receiver queue.

        :param event_id: hashable object or list thereof
        :return: None
        """
        stop_listening(self.__receiver_id, event_id)

    def stop_events(self):
        """
        Tell the global message dispatcher to purge any references to
        this process' receiver ID and its associated queue from its registry.

        It is imperative that this is called before the process ends. Otherwise
        the global event dispatcher will accumulate dead receivers and listeners
        in its registry that hog memory and slow down the dispatcher.

        :return: None
        """
        remove_receiver(self.__receiver_id)

    @staticmethod
    def dispatch(event):
        """
        Dispatch event to the global event dispatcher's event queue

        :param event: messaging.Event
        :return: None
        """
        dispatch(event)


class MessagingThread(Thread):
    """A messaging-enabled convenience wrapper for threading.Thread"""

    def __init__(self, receiver_id=None, *args, **kwargs):
        """
        MessagingThread constructor

        Must be called from the main process.
        """
        super(MessagingThread, self).__init__(*args, **kwargs)
        self.__receiver_id, self.__event_queue = create_receiver(receiver_id)

    def events_pending(self):
        """
        Return whether there are pending events in the event receiver queue

        :return: bool
        """
        return not self.__event_queue.empty()

    def receive(self, block=True, timeout=None):
        """
        Get an event from the event receiver queue

        :param block: bool
        :param timeout: float
        :return: messaging.Event
        """
        return self.__event_queue.get(block=block, timeout=timeout)

    def start_listening(self, event_id):
        """
        Register thread as listening to the given event ID or IDs.

        After this, you will start seeing events with the respective IDs
        start showing up in the thread' event receiver queue.

        See MessagingThread.receive()

        :param event_id: hashable object or list thereof
        :return: None
        """
        start_listening(self.__receiver_id, event_id)

    def stop_listening(self, event_id):
        """
        Tell the global event dispatcher to stop dispatching specified event IDs
        to the thread' event receiver queue.

        :param event_id: hashable object or list thereof
        :return: None
        """
        stop_listening(self.__receiver_id, event_id)

    def stop_events(self):
        """
        Tell the global message dispatcher to purge any references to
        this thread' receiver ID and its associated queue from its registry.

        It is imperative that this is called before the thread ends. Otherwise
        the global event dispatcher will accumulate dead receivers and listeners
        in its registry that hog memory and slow down the dispatcher.

        :return: None
        """
        remove_receiver(self.__receiver_id)

    @staticmethod
    def dispatch(event):
        """
        Dispatch event to the global event dispatcher's event queue

        :param event: messaging.Event
        :return: None
        """
        dispatch(event)
