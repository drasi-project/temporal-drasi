#!/usr/bin/python3
"""Services-free pipe/process fault driver; production workers never use Python."""

import json
import os
import queue
import struct
import sys
import threading

commands = queue.Queue(maxsize=128)
output = threading.Lock()


def exact(size):
    result = bytearray()
    while len(result) < size:
        part = os.read(0, size - len(result))
        if not part:
            os._exit(73)
        result.extend(part)
    return result


def receive():
    while True:
        header = exact(8)
        if header[:4] != b"TDW1":
            os._exit(73)
        size = struct.unpack(">I", header[4:])[0]
        if size == 0 or size > 8 << 20:
            os._exit(73)
        frame = json.loads(exact(size))
        try:
            commands.put_nowait(frame)
        except queue.Full:
            os._exit(73)


def send(frame, command):
    data = json.dumps(
        {"version": 1, "binding": frame["binding"], "command": command}
    ).encode()
    with output:
        packet = b"TDW1" + struct.pack(">I", len(data)) + data
        while packet:
            written = os.write(1, packet)
            packet = packet[written:]


threading.Thread(target=receive, daemon=True).start()
mode = "normal"
while True:
    frame = commands.get()
    command = frame["command"]
    if command == "Stop":
        if mode == "hang":
            threading.Event().wait()
        send(frame, "Stopped")
        if mode == "lie":
            threading.Event().wait()
        os._exit(0)
    if "Request" in command:
        request = command["Request"]
        operation = request["request"]
        if "Construct" in operation:
            mode = operation["Construct"]["source_config"].get("fixture_mode", "normal")
            response = "Constructed"
        elif operation == "Start":
            if mode == "stdout":
                os.write(1, b"private-foreign-output-must-not-be-logged")
                threading.Event().wait()
            response = "Ok"
        elif operation == "Status":
            response = {"Status": "Running"}
        else:
            response = "Failed"
        send(frame, {"Response": {"id": request["id"], "response": response}})
