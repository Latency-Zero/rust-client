"""Isolated real daemon and optional peers for the Rust integration tests."""

import argparse
import asyncio
import importlib
import importlib.machinery
import json
import math
import os
import queue
import sys
import tempfile
import time
import traceback
import types
from functools import partial
from pathlib import Path


def emit(message):
    print(json.dumps(message, allow_nan=False), flush=True)


async def blocking(function, *args, **kwargs):
    future = asyncio.get_running_loop().run_in_executor(None, partial(function, *args, **kwargs))
    return await asyncio.wait_for(future, 6)


class Clock:
    def __init__(self):
        self.now = time.monotonic()
        self.wall = time.time()

    def monotonic(self):
        return self.now

    def time(self):
        return self.wall

    def advance(self, seconds):
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise ValueError("Clock advance must be numeric")
        if not math.isfinite(seconds) or not 0 <= seconds <= 60:
            raise ValueError("Clock advance must be finite and between 0 and 60 seconds")
        self.now += seconds
        self.wall += seconds


class RawWorker:
    def __init__(self, reader, writer, client_id):
        self.reader = reader
        self.writer = writer
        self.client_id = client_id
        self.calls = asyncio.Queue(maxsize=64)
        self.held = {}
        self.task = None

    async def send(self, message):
        self.writer.write((json.dumps(message, allow_nan=False) + "\n").encode("utf-8"))
        await asyncio.wait_for(self.writer.drain(), 3)

    async def read(self):
        line = await self.reader.readline()
        if not line:
            raise EOFError("Raw fixture peer disconnected")
        return json.loads(line)

    @classmethod
    async def connect(cls, port, pool, client_id):
        reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=1024 * 1024 + 1)
        peer = cls(reader, writer, client_id)
        try:
            for sequence, (kind, payload) in enumerate((
                ("hello", {}),
                ("join_pool", {"client_id": client_id, "pool": pool}),
                ("register_process", {"process_name": "echo"}),
            )):
                request_id = "fixture-handshake-%d" % sequence
                await peer.send({"type": kind, "request_id": request_id,
                                 "pool": None, "payload": payload})
                for _ in range(64):
                    message = await asyncio.wait_for(peer.read(), 5)
                    if message.get("request_id") != request_id:
                        continue
                    if message["type"] != "ack":
                        raise AssertionError("Raw fixture handshake failed: %r" % message)
                    break
                else:
                    raise AssertionError("Raw fixture handshake exceeded its read bound")
            peer.task = asyncio.create_task(peer.run())
            return peer
        except BaseException:
            await peer.close()
            raise

    async def run(self):
        while True:
            message = await self.read()
            if message["type"] == "call_app":
                data = message["payload"]["data"]
                # Legacy workers echo only the opaque hop ID and {value,error}.
                reply = {"type": "app_result", "request_id": message["request_id"],
                         "pool": None, "payload": {"value": data.get("value"), "error": None}}
                held = bool(data.get("hold"))
                self.calls.put_nowait({"invocation": message, "reply": reply, "held": held})
                if held:
                    if len(self.held) >= 64:
                        raise AssertionError("Raw fixture held-call bound exceeded")
                    self.held[message["request_id"]] = reply
                else:
                    await self.send(reply)
            elif message["type"] == "error":
                raise AssertionError("Raw fixture received an unexpected error: %r" % message)

    async def close(self):
        try:
            if self.task is not None:
                self.task.cancel()
                try:
                    await asyncio.wait_for(self.task, 3)
                except asyncio.CancelledError:
                    pass
        finally:
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), 3)


class Fixture:
    def __init__(self, server, port, root, clock, pods=False):
        self.server = server
        self.port = port
        self.root = root
        self.clock = clock
        self.pods = pods
        self.raw = None
        self.python = None
        self.python_hooks = queue.Queue(maxsize=32)
        self.python_updates = queue.Queue(maxsize=32)

    def stats(self):
        if self.pods:
            return self.server.get_dashboard_snapshot()
        return {"routes": self.server._route_count,
                "clients": {name: sorted(pool.clients) for name, pool in self.server._pools.items()},
                "processes": {name: sorted(pool.processes) for name, pool in self.server._pools.items()}}

    async def command(self, command):
        operation = command["operation"]
        if operation == "barrier":
            if not self.pods:
                await asyncio.wait_for(self.server._worker_pool.join(), 5)
            return self.stats()
        if operation == "advance_clock":
            if self.clock is None:
                raise ValueError("This daemon was not started with a controlled clock")
            self.clock.advance(command["seconds"])
            await self.server._expire_buffers()
            return self.stats()
        if operation == "start_raw":
            if self.raw is not None:
                raise ValueError("Only one raw fixture worker is allowed")
            port = self.port
            if self.pods:
                port = self.server.children[self.server.pool_owner(command["pool"])].port
            self.raw = await RawWorker.connect(port, command["pool"], command["client_id"])
            return {"client_id": self.raw.client_id}
        if operation == "next_raw_call":
            return await asyncio.wait_for(self.raw.calls.get(), 5)
        if operation == "complete_raw":
            await self.raw.send(self.raw.held.pop(command["request_id"]))
            return {"sent": True}
        if operation == "start_python":
            source = self.root / "python-client" / "latzero"
            if not (source / "server_client.py").is_file():
                return {"skip": "Optional sibling Python daemon SDK checkout is unavailable"}
            # The daemon modules are real, stdlib-only modules. Do not import
            # the unrelated shared-memory package or install its crypto extras.
            name = "_latzero_rust_fixture_sdk"
            package = types.ModuleType(name)
            package.__path__ = [str(source)]
            package.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=True)
            sys.modules[name] = package
            try:
                sdk = importlib.import_module(name + ".server_client").LatZero
            except ModuleNotFoundError as exc:
                return {"skip": "Optional Python SDK dependency is unavailable: %s" % exc.name}
            self.python = await blocking(sdk, "latzero://python-peer", command["pool"],
                                         port=self.port, timeout=5, callback_workers=1)

            def app_echo(value):
                return {"owner": "python-peer", "kind": "app", "value": value}

            def process_echo(value):
                return {"owner": "python-peer", "kind": "process", "value": value}

            self.python.on_event("echo")(app_echo)
            self.python.on("on_app_result", self.python_hooks.put_nowait)
            self.python.on("on_buffer_update", self.python_updates.put_nowait)
            await blocking(self.python.process.register, process_echo, name="echo",
                           min_workers=1, max_workers=1)
            return {"client_id": self.python.client_id}
        if operation == "python_call":
            options = {"value": command["value"], "response_to": command.get("response_to")}
            if command["kind"] == "app":
                return await blocking(self.python.call_app, command["target"], "echo", **options)
            return await blocking(self.python.process.call, command["target"] + ":echo", **options)
        if operation == "python_hook":
            return await blocking(self.python_hooks.get, timeout=5)
        if operation == "python_subscribe":
            await blocking(self.python.subscribe_buffer, command["key"])
            return {"subscribed": True}
        if operation == "python_update":
            return await blocking(self.python_updates.get, timeout=5)
        if operation == "python_set":
            await blocking(self.python.set, command["key"], command["value"])
            return {"set": True}
        if operation == "python_get":
            return {"exists": await blocking(self.python.exists, command["key"]),
                    "value": await blocking(self.python.get, command["key"], "missing")}
        raise ValueError("Unknown Rust daemon fixture command: " + operation)

    async def close(self):
        errors = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 6
        for action in (self.raw.close if self.raw is not None else None,
                       partial(blocking, self.python.disconnect) if self.python is not None else None,
                       self.server.stop):
            if action is None:
                continue
            try:
                await asyncio.wait_for(action(), max(0.001, deadline - loop.time()))
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("Fixture cleanup failed: %r" % errors)


async def run(options, server_class, config_class, server_module):
    temporary = tempfile.TemporaryDirectory(prefix="state-", dir=options.temp_root)
    data_dir = Path(temporary.name)
    fixture = None
    shutdown_id = None
    try:
        clock = Clock() if options.controlled_clock else None
        if clock is not None:
            server_module.time = clock
        config = config_class(port=0, websocket_enabled=False, data_dir=data_dir,
                              min_workers=2, max_workers=4, shutdown_timeout=3)
        if options.pods > 1:
            supervisor = importlib.import_module("latzero_server.pods").PodSupervisor
            server = supervisor(config, options.pods, startup_timeout=15)
        else:
            server = server_class(config)
        fixture = Fixture(server, 0, Path(options.server_root).parent, clock, options.pods > 1)
        await asyncio.wait_for(server.start(), 20 if fixture.pods else 5)
        fixture.port = server.tcp_port if fixture.pods else server._tcp_server.sockets[0].getsockname()[1]
        ready = {"ok": True, "ready": True, "port": fixture.port,
                 "pid": os.getpid(), "data_dir": str(data_dir)}
        if fixture.pods:
            initial_pool = "rust-daemon-interop"
            other_pool = next("integration-other-%d" % index for index in range(1000)
                              if server.pool_owner("integration-other-%d" % index)
                              != server.pool_owner(initial_pool))
            ready.update({"pod_count": options.pods, "initial_pool": initial_pool,
                          "other_pool": other_pool,
                          "initial_owner": server.pool_owner(initial_pool),
                          "other_owner": server.pool_owner(other_pool),
                          "children": [{"pid": child.pid, "index": child.index,
                                        "port": child.port} for child in server.children]})
        emit(ready)
        while True:
            line = await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            command = json.loads(line)
            if command["operation"] == "shutdown":
                shutdown_id = command["id"]
                break
            result = await asyncio.wait_for(fixture.command(command), 8)
            emit({"ok": True, "id": command["id"], "result": result})
    finally:
        try:
            if fixture is not None:
                await fixture.close()
        finally:
            temporary.cleanup()
    if shutdown_id is not None:
        emit({"ok": True, "id": shutdown_id,
              "result": {"stopped": True, "data_removed": not data_dir.exists()}})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-root", required=True)
    parser.add_argument("--temp-root", required=True)
    parser.add_argument("--controlled-clock", action="store_true")
    parser.add_argument("--pods", type=int, choices=(1, 4), default=1)
    options = parser.parse_args()
    if options.pods > 1 and options.controlled_clock:
        parser.error("Controlled fixture clocks are only available for the single daemon")
    sys.path.insert(0, options.server_root)
    try:
        server_module = importlib.import_module("latzero_server.server")
        config_class = importlib.import_module("latzero_server.config").ServerConfig
    except ModuleNotFoundError as exc:
        emit({"ok": True, "skip": "Python daemon dependency is unavailable: %s" % exc.name})
        return
    asyncio.run(run(options, server_module.LatZeroServer, config_class, server_module))


if __name__ == "__main__":
    try:
        main()
    except BaseException as exc:
        traceback.print_exc(file=sys.stderr)
        emit({"ok": False, "error": repr(exc)})
        sys.exit(1)
