# Overview: RegoDB

RegoDB is a simple, **in-memory database** server that mimics some functionalities of Redis. It acts like a digital librarian, listening for client requests, understanding a special language called *RESP*, and then efficiently performing operations like *storing* (**SET**), *retrieving* (**GET**), or *managing lists* (**RPUSH**, **LRANGE**) of data, all while keeping everything safely organized in its main memory.


## Visual Overview

```mermaid
flowchart TD
    A0["Data Store
"]
    A1["RESP Protocol Handlers
"]
    A2["Command Dispatcher
"]
    A3["Specific Command Implementations
"]
    A4["Network Listener
"]
    A4 -- "Passes connections to" --> A2
    A2 -- "Parses commands with" --> A1
    A2 -- "Dispatches to" --> A3
    A3 -- "Accesses" --> A0
    A3 -- "Formats responses with" --> A1
```

## Chapters

1. [Network Listener
](01_network_listener_.md)
2. [Command Dispatcher
](02_command_dispatcher_.md)

---
