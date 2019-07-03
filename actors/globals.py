"""
Defines global objects used across the platform.
"""

# keep_running will be updated by the worker thread listening on the worker channel when a graceful shutdown is
# required.
global keep_running
keep_running = True

# force_quit can be updated to True through a message sent to the worker on the worker channel to force a running
# execution to be halted immediately.
# The worker thread monitoring the actor executions imports this global and checks it on a regular interval.
global force_quit
force_quit = False

