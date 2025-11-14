"""Unified process cleanup utilities for dev server management.

This module provides centralized functions for killing orphaned build tool processes
(vite, bun, node, esbuild, etc.) to ensure clean server starts and stops.
"""

import logging
from pathlib import Path

# List of process names to explicitly kill (build tools and dev servers)
PROCESS_NAMES_TO_KILL = ["bun", "node", "vite", "esbuild", "vite-node"]


def cleanup_orphaned_processes(
    app_dir: Path,
    ports: list[int] | None = None,
    silent: bool = False,
    logger: logging.Logger | None = None,
) -> int:
    """Kill any orphaned frontend/backend processes.

    This ensures a clean slate when starting servers.
    Explicitly kills vite, bun, node, esbuild, and any other build tool processes.

    Args:
        app_dir: Application directory
        ports: Optional list of ports to check for orphaned processes
        silent: If True, don't log anything (for preventive cleanups)
        logger: Optional logger to use for messages

    Returns:
        Number of processes killed
    """
    try:
        import psutil

        if logger is None:
            logger = logging.getLogger("apx.cleanup")

        app_dir_str = str(app_dir.resolve())
        killed: list[str] = []

        # Find processes using the specified ports
        if ports:
            for proc in psutil.process_iter(["pid", "name", "connections"]):
                try:
                    # Check if process is using any of our ports
                    connections = proc.connections()
                    for conn in connections:
                        if (
                            hasattr(conn, "laddr")
                            and conn.laddr
                            and hasattr(conn.laddr, "port")
                        ):
                            if conn.laddr.port in ports:
                                proc.kill()
                                killed.append(f"{proc.name()} (PID {proc.pid})")
                                break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

        # Explicitly look for all build tool processes in our app directory
        for proc in psutil.process_iter(["pid", "name", "cwd", "cmdline"]):
            try:
                if proc.name() in PROCESS_NAMES_TO_KILL:
                    # Check if process is in our app directory
                    try:
                        cwd = proc.cwd()
                        if cwd and Path(cwd) == app_dir:
                            proc.kill()
                            killed.append(f"{proc.name()} (PID {proc.pid})")
                            continue
                    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                        pass

                    # Also check command line for app directory reference
                    try:
                        cmdline = proc.cmdline()
                        if cmdline:
                            cmdline_str = " ".join(cmdline)
                            if app_dir_str in cmdline_str:
                                proc.kill()
                                killed.append(f"{proc.name()} (PID {proc.pid})")
                    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass

        if killed and not silent:
            logger.info(
                f"Cleaned up {len(killed)} orphaned process(es): {', '.join(killed)}"
            )

        return len(killed)

    except Exception as e:
        if not silent and logger:
            logger.warning(f"Failed to cleanup orphaned processes: {e}")
        return 0


def cleanup_orphaned_processes_double_pass(
    app_dir: Path,
    ports: list[int] | None = None,
    silent: bool = False,
    logger: logging.Logger | None = None,
    delay: float = 0.5,
) -> int:
    """Kill orphaned processes with a double-pass approach.

    Runs cleanup twice with a delay in between to catch slow-responding processes.

    Args:
        app_dir: Application directory
        ports: Optional list of ports to check for orphaned processes
        silent: If True, don't log anything (for preventive cleanups)
        logger: Optional logger to use for messages
        delay: Delay between cleanup passes in seconds

    Returns:
        Total number of processes killed across both passes
    """
    import time

    # First pass
    killed_count = cleanup_orphaned_processes(app_dir, ports, silent, logger)

    # Wait for processes to fully terminate
    time.sleep(delay)

    # Second pass to catch stragglers
    killed_count += cleanup_orphaned_processes(app_dir, ports, silent, logger)

    return killed_count


def cleanup_dev_server_processes(app_dir: Path, silent: bool = False) -> int:
    """Kill any dev server processes (_run_server) for this app directory.

    Args:
        app_dir: Application directory
        silent: If True, don't log anything

    Returns:
        Number of processes killed
    """
    try:
        import psutil

        app_dir_str = str(app_dir.resolve())
        killed: list[str] = []

        for proc in psutil.process_iter(["pid", "name", "cwd", "cmdline"]):
            try:
                cmdline = proc.cmdline()
                if not cmdline:
                    continue

                cmdline_str = " ".join(cmdline)

                # Check if this is a dev server process for our app directory
                # Look for: "apx dev _run_server" with our app directory path
                if (
                    "apx" in cmdline_str
                    and "dev" in cmdline_str
                    and "_run_server" in cmdline_str
                    and app_dir_str in cmdline_str
                ):
                    proc.kill()
                    killed.append(f"dev-server (PID {proc.pid})")

            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass

        if killed and not silent:
            logger = logging.getLogger("apx.cleanup")
            logger.info(f"Cleaned up dev server processes: {', '.join(killed)}")

        return len(killed)

    except Exception as e:
        if not silent:
            logger = logging.getLogger("apx.cleanup")
            logger.warning(f"Failed to cleanup dev server processes: {e}")
        return 0
