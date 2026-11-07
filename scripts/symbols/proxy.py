#!/usr/bin/env python3
"""Minimal TCP cutover proxy for the symbols pilot.

Forwards every connection to the upstream address given by ``--upstream``.
The gray script restarts the proxy to switch between the Go and Rust
service; restarting closes existing connections so gRPC clients reconnect to
the new backend.
"""

import argparse
import asyncio
import contextlib


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        with contextlib.suppress(Exception):
            writer.close()
            await writer.wait_closed()


async def handle(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            upstream_host, upstream_port
        )
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(
        pipe(client_reader, upstream_writer),
        pipe(upstream_reader, client_writer),
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True, help="host:port to listen on")
    parser.add_argument("--upstream", required=True, help="host:port to forward to")
    args = parser.parse_args()

    listen_host, listen_port = args.listen.rsplit(":", 1)
    upstream_host, upstream_port = args.upstream.rsplit(":", 1)
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, upstream_host, int(upstream_port)),
        listen_host,
        int(listen_port),
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
