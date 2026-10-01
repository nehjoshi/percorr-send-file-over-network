from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .driver import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_MAX_BLOB_BYTES,
    DEFAULT_RETRY_DELAY_SECONDS,
    FollowerDriver,
    LeaderDriver,
)
from .follower import Follower
from .leader import (
    DEFAULT_BLOB_TIMEOUT_SECONDS,
    DEFAULT_RESTARTED_TIMEOUT_SECONDS,
    Leader,
    LeaderState,
)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7070


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src",
        description="Send a file from a Leader to a Follower in the face of failures.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--file", required=True)
    common.add_argument("--host", default=DEFAULT_HOST)
    common.add_argument("--port", type=int, default=DEFAULT_PORT)
    common.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help="seconds to wait before reporting a send with no connection as failed",
    )
    common.add_argument("--verbose", action="store_true", help="log events and actions")

    roles = parser.add_subparsers(dest="role", required=True)

    leader = roles.add_parser("leader", parents=[common], help="send --file")
    leader.add_argument(
        "--restarted-timeout",
        type=float,
        default=DEFAULT_RESTARTED_TIMEOUT_SECONDS,
        help="N, seconds to wait for RestartedReply",
    )
    leader.add_argument(
        "--blob-timeout",
        type=float,
        default=DEFAULT_BLOB_TIMEOUT_SECONDS,
        help="M, seconds to wait for BlobReply",
    )
    leader.add_argument(
        "--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT_SECONDS
    )

    follower = roles.add_parser("follower", parents=[common], help="receive into --file")
    follower.add_argument("--max-blob-bytes", type=int, default=DEFAULT_MAX_BLOB_BYTES)

    return parser.parse_args(argv)


async def run_leader(arguments: argparse.Namespace) -> int:
    leader = Leader(
        arguments.file,
        restarted_timeout_seconds=arguments.restarted_timeout,
        blob_timeout_seconds=arguments.blob_timeout,
    )
    driver = LeaderDriver(
        leader,
        arguments.host,
        arguments.port,
        retry_delay_seconds=arguments.retry_delay,
        connect_timeout_seconds=arguments.connect_timeout,
    )
    await driver.run()
    if leader.state is LeaderState.FINAL:
        logging.info("transfer complete: %s", leader.completion_evidence)
        return 0
    logging.error("transfer failed: result %s", leader.failure_result)
    return 1


async def run_follower(arguments: argparse.Namespace) -> int:
    follower = Follower(arguments.file)
    driver = FollowerDriver(
        follower,
        arguments.host,
        arguments.port,
        retry_delay_seconds=arguments.retry_delay,
        max_blob_bytes=arguments.max_blob_bytes,
    )
    await driver.run()
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    logging.basicConfig(
        level=logging.DEBUG if arguments.verbose else logging.INFO,
        format=f"%(asctime)s {arguments.role:<8} %(levelname)-7s %(message)s",
    )
    run = run_leader if arguments.role == "leader" else run_follower
    try:
        return asyncio.run(run(arguments))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
