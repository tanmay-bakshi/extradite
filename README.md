# extradite

`extradite` lets you use classes from modules that must never be imported in your main interpreter.

It launches a separate Python interpreter process, imports the target module there, and exposes a proxy class in the root process. Instance and class interactions are forwarded over IPC with pickled payloads.

This project is early-stage and currently focused on correctness of process isolation.
