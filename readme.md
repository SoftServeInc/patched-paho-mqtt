# Python Paho MQTT library patch

## Overview

This patch modifies the Python Paho MQTT library to use the `selectors` module instead of legacy `select` method for handling I/O operations. The motivation for this change is to overcome the maximum file descriptor limit imposed by the `select` method, which restricts the number of simultaneous connections that can be handled when running multiple Paho MQTT clients in the same process.

## Why selectors?

The [selectors](https://docs.python.org/3.13/library/selectors.html) module provides a high-level, platform-independent way to manage I/O events and supports various mechanisms like `epoll`, `kqueue`, or `poll` where available. Unlike `select`, these mechanisms:
- Avoid the hard limit of 1024 file descriptors on many operating systems.
- Provide better scalability, especially for testing purposes when there's a need of handling a large number of connections.
- Offer a small performance boost in scenarios with many idle or low-traffic connections.

## Changes in This Patch

- Replaced the `select` system call with the `selectors` module to manage socket operations.
- Maintained backward compatibility with the existing library interface (Paho version >= 2.0).
- Added an extra parameter to pass a [threading event](https://docs.python.org/3/library/threading.html#threading.Event), enabling the parent thread to trigger an exit from the event loop.

## Usage

To use the patched version:
1. Clone or download the patched library.
2. Replace the existing Paho MQTT library in your project with the patched version.
3. Use the library, no additional configuration is required.
