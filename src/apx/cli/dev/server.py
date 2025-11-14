"""Centralized FastAPI dev server for managing frontend, backend, and OpenAPI watcher.

Architecture:
- Runs as a Unix domain socket server
- Manages three background tasks: frontend (Node.js/Bun), backend (Python), OpenAPI watcher
- Uses process groups (start_new_session=True) to enable killing all child processes
- Explicit process cleanup on stop/restart to prevent orphaned processes
- Cleanup of orphaned processes on start for bulletproof operation

Key Features:
1. Process Group Killing: Uses os.killpg() to kill entire process trees
2. Graceful Shutdown: SIGTERM first, then SIGKILL after timeout
3. Orphan Cleanup: Scans for and kills orphaned bun/node/vite processes on start
4. State Tracking: Maintains references to subprocess objects for explicit cleanup
5. Port Cleanup: Kills processes holding onto ports before starting new servers
"""

import asyncio
from collections.abc import AsyncGenerator
import logging
import os
import signal
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse

from apx.cli.dev.manager import (
    is_port_available,
    run_backend,
    run_frontend_with_logging,
    run_openapi_with_logging,
)
from apx.cli.dev.models import (
    ActionRequest,
    ActionResponse,
    LogEntry,
    PortsResponse,
    StatusResponse,
)
from apx.cli.dev.logging import (
    LogBuffer,
    LoggerWriter,
    setup_buffered_logging,
)
from apx.cli.dev.process_cleanup import cleanup_orphaned_processes_double_pass


# Global state for background tasks
class ServerState:
    """Global state for the dev server."""

    def __init__(self) -> None:
        from collections import deque

        self.frontend_task: asyncio.Task[None] | None = None
        self.backend_task: asyncio.Task[None] | None = None
        self.openapi_task: asyncio.Task[None] | None = None
        self.frontend_process: asyncio.subprocess.Process | None = None
        self.log_buffer: LogBuffer = deque(maxlen=10000)
        self.app_dir: Path | None = None
        self.frontend_port: int = 5173
        self.backend_port: int = 8000
        self.host: str = "localhost"
        self.obo: bool = True
        self.openapi_enabled: bool = True
        self.max_retries: int = 10


state = ServerState()


# === Process Cleanup ===
# Note: Cleanup functions are now in apx.cli.dev.process_cleanup module


async def kill_process_group(process: asyncio.subprocess.Process, timeout: float = 5.0):
    """Kill a process and all its children using process group.

    Args:
        process: The subprocess to kill
        timeout: How long to wait before force killing
    """
    if process.returncode is not None:
        return  # Already dead

    try:
        # Try graceful shutdown first (SIGTERM)
        if hasattr(os, "killpg"):
            # Kill entire process group
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, signal.SIGTERM)
        else:
            process.terminate()

        # Wait for process to die
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Force kill if still alive (SIGKILL)
            if hasattr(os, "killpg"):
                try:
                    pgid = os.getpgid(process.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # Already dead
            else:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass  # Already dead

            # Final wait
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass  # Give up, OS will clean up eventually
    except ProcessLookupError:
        pass  # Process already dead
    except Exception as e:
        logger = logging.getLogger("apx.server")
        logger.warning(f"Error killing process: {e}")


async def verify_port_released(port: int, timeout: float = 1.0) -> bool:
    """Verify that a port has been released.

    Args:
        port: Port number to check
        timeout: How long to wait for port to be released

    Returns:
        True if port is available, False otherwise
    """
    # Give a small initial delay for the port to be released
    await asyncio.sleep(timeout)
    return is_port_available(port)


async def ensure_port_released(
    process: asyncio.subprocess.Process | None,
    port: int,
    max_attempts: int = 3,
    attempt_delay: float = 1.0,
) -> None:
    """Ensure a port is released by killing the process if needed with retries.

    Args:
        process: The subprocess that might be holding the port
        port: Port number to ensure is released
        max_attempts: Maximum number of attempts to free the port (default: 3)
        attempt_delay: Delay between attempts in seconds (default: 1.0)

    Raises:
        RuntimeError: If port is not freed after max_attempts
    """
    logger = logging.getLogger("apx.server")

    for attempt in range(1, max_attempts + 1):
        # Check if port is released
        if await verify_port_released(port, timeout=attempt_delay):
            if attempt > 1:
                logger.info(
                    f"Port {port} successfully released after {attempt} attempts"
                )
            return

        # Port is still in use
        logger.warning(f"Port {port} still in use (attempt {attempt}/{max_attempts})")

        # Try to kill the process if we have one
        if process and process.returncode is None:
            logger.info(f"Killing process group for port {port}")
            await kill_process_group(process, timeout=3.0)

        # If this was the last attempt, raise an error
        if attempt == max_attempts:
            msg = (
                f"Failed to free port {port} after {max_attempts} attempts. "
                "Please manually kill any processes using this port."
            )
            raise RuntimeError(msg)

        # Wait before next attempt (already waited in verify_port_released)


# === Background Task Runners ===


async def run_frontend_task(app_dir: Path, port: int, max_retries: int):
    """Run frontend as a background task."""
    try:
        await run_frontend_with_logging(app_dir, port, max_retries, state)
    except asyncio.CancelledError:
        # Kill the frontend process on cancellation
        if state.frontend_process:
            await kill_process_group(state.frontend_process)
        raise
    except Exception as e:
        logger = logging.getLogger("apx.frontend")
        logger.error(f"Frontend task failed: {e}")
        if state.frontend_process:
            await kill_process_group(state.frontend_process)


async def run_backend_task(
    app_dir: Path,
    app_module_name: str,
    host: str,
    port: int,
    obo: bool,
    max_retries: int,
):
    """Run backend as a background task."""
    try:
        await run_backend(
            app_dir, app_module_name, host, port, obo, max_retries=max_retries
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger = logging.getLogger("apx.backend")
        logger.error(f"Backend task failed: {e}")


async def run_openapi_task(app_dir: Path, max_retries: int):
    """Run OpenAPI watcher as a background task."""
    try:
        await run_openapi_with_logging(app_dir, max_retries)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger = logging.getLogger("apx.openapi")
        logger.error(f"OpenAPI watcher task failed: {e}")


# === Lifecycle Management ===


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for the FastAPI app."""
    # Setup in-memory logging (no files needed)
    for process_name in ["frontend", "backend", "openapi"]:
        setup_buffered_logging(state.log_buffer, process_name)

    # Redirect stdout/stderr to backend logger BEFORE any tasks start
    # This ensures app imports capture logs correctly
    import sys

    original_stdout = sys.stdout
    original_stderr = sys.stderr

    backend_logger = logging.getLogger("apx.backend")

    sys.stdout = LoggerWriter(backend_logger, logging.INFO, "APP")
    sys.stderr = LoggerWriter(backend_logger, logging.ERROR, "APP")

    try:
        yield
    finally:
        # Restore stdout/stderr
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        # Kill frontend process first (most important for orphans)
        if state.frontend_process and state.frontend_process.returncode is None:
            await kill_process_group(state.frontend_process, timeout=2.0)

        # Cancel all tasks
        tasks_to_cancel: list[asyncio.Task[None]] = []
        if state.frontend_task and not state.frontend_task.done():
            tasks_to_cancel.append(state.frontend_task)
        if state.backend_task and not state.backend_task.done():
            tasks_to_cancel.append(state.backend_task)
        if state.openapi_task and not state.openapi_task.done():
            tasks_to_cancel.append(state.openapi_task)

        for task in tasks_to_cancel:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Aggressive cleanup: kill all vite/bun/node/esbuild processes (double-pass)
        if state.app_dir:
            logger = logging.getLogger("apx.server")
            cleanup_orphaned_processes_double_pass(
                state.app_dir,
                ports=[state.frontend_port, state.backend_port],
                logger=logger,
                delay=0.3,
            )


# === FastAPI App ===


def create_dev_server(app_dir: Path) -> FastAPI:
    """Create the dev server FastAPI app.

    Args:
        app_dir: Application directory

    Returns:
        FastAPI app instance
    """
    app = FastAPI(
        title="APX Dev Server",
        description="Centralized development server for APX projects",
        version="1.0.0",
        lifespan=lifespan,
    )

    state.app_dir = app_dir

    @app.get("/")
    async def root():
        """Root endpoint."""
        return {"message": "APX Dev Server", "status": "running"}

    @app.get("/status", response_model=StatusResponse)
    async def get_status():
        """Get the status of all running processes."""
        return StatusResponse(
            frontend_running=state.frontend_task is not None
            and not state.frontend_task.done(),
            backend_running=state.backend_task is not None
            and not state.backend_task.done(),
            openapi_running=state.openapi_task is not None
            and not state.openapi_task.done(),
            frontend_port=state.frontend_port,
            backend_port=state.backend_port,
        )

    @app.get("/ports", response_model=PortsResponse)
    async def get_ports():
        """Get the frontend and backend ports."""
        return PortsResponse(
            frontend_port=state.frontend_port,
            backend_port=state.backend_port,
            host=state.host,
        )

    @app.post("/actions/start", response_model=ActionResponse)
    async def start_servers(request: ActionRequest) -> ActionResponse:
        """Start all development servers.

        Ensures cleanup before starting to prevent duplicate frontend servers.
        """
        # Check if already running
        if (
            state.frontend_task
            and not state.frontend_task.done()
            or state.backend_task
            and not state.backend_task.done()
            or state.openapi_task
            and not state.openapi_task.done()
        ):
            return ActionResponse(status="error", message="Servers are already running")

        # IMPORTANT: Kill any orphaned processes before starting to prevent duplicates
        # This includes vite/bun/node/esbuild from previous failed sessions
        if state.app_dir:
            logger = logging.getLogger("apx.server")
            logger.info("Cleaning up any orphaned processes before starting servers...")
            cleanup_orphaned_processes_double_pass(
                state.app_dir,
                ports=[request.frontend_port, request.backend_port],
                logger=logger,
                delay=0.5,
            )

        # Store configuration
        state.frontend_port = request.frontend_port
        state.backend_port = request.backend_port
        state.host = request.host
        state.obo = request.obo
        state.openapi_enabled = request.openapi
        state.max_retries = request.max_retries

        # Get app module name
        if state.app_dir:
            from apx.utils import get_project_metadata, ProjectMetadata

            metadata: ProjectMetadata = get_project_metadata()
            app_module_name: str = metadata.app_module
        else:
            return ActionResponse(status="error", message="App directory not set")

        # Start frontend
        if state.app_dir:
            state.frontend_task = asyncio.create_task(
                run_frontend_task(
                    state.app_dir,
                    request.frontend_port,
                    request.max_retries,
                )
            )

        # Start backend
        if state.app_dir:
            state.backend_task = asyncio.create_task(
                run_backend_task(
                    state.app_dir,
                    app_module_name,
                    request.host,
                    request.backend_port,
                    request.obo,
                    request.max_retries,
                )
            )

        # Start OpenAPI watcher
        if request.openapi and state.app_dir:
            state.openapi_task = asyncio.create_task(
                run_openapi_task(state.app_dir, request.max_retries)
            )

        return ActionResponse(status="success", message="Servers started successfully")

    @app.post("/actions/stop", response_model=ActionResponse)
    async def stop_servers() -> ActionResponse:
        """Stop all development servers.

        Explicitly kills all vite, bun, node, esbuild processes to ensure clean shutdown.
        """
        stopped: list[str] = []
        # Cancel tasks
        if state.frontend_task and not state.frontend_task.done():
            state.frontend_task.cancel()
            try:
                await state.frontend_task
            except asyncio.CancelledError:
                pass

            # Kill frontend process explicitly to avoid orphaned processes
            if state.frontend_process and state.frontend_process.returncode is None:
                await kill_process_group(state.frontend_process, timeout=3.0)

            # Ensure port is released with retries (3 attempts over 3 seconds)
            if state.frontend_port:
                try:
                    await ensure_port_released(
                        state.frontend_process,
                        state.frontend_port,
                        max_attempts=3,
                        attempt_delay=0.1,
                    )
                except RuntimeError:
                    # If port release failed after retries, do a final cleanup
                    pass  # Will be handled by the double-pass cleanup below

            state.frontend_process = None
            state.frontend_task = None
            stopped.append("frontend")

        if state.backend_task and not state.backend_task.done():
            state.backend_task.cancel()
            try:
                await state.backend_task
            except asyncio.CancelledError:
                pass
            state.backend_task = None
            stopped.append("backend")

        if state.openapi_task and not state.openapi_task.done():
            state.openapi_task.cancel()
            try:
                await state.openapi_task
            except asyncio.CancelledError:
                pass
            state.openapi_task = None
            stopped.append("openapi")

        if not stopped:
            return ActionResponse(status="error", message="No servers were running")

        # Aggressive double-pass cleanup to kill all vite/bun/node/esbuild processes
        if state.app_dir:
            logger = logging.getLogger("apx.server")
            cleanup_orphaned_processes_double_pass(
                state.app_dir,
                ports=[state.frontend_port, state.backend_port],
                logger=logger,
                delay=0.5,
            )

        os.kill(os.getpid(), signal.SIGTERM)
        return ActionResponse(
            status="success",
            message=f"Stopped servers: {', '.join(stopped)}",
        )

    @app.post("/actions/restart", response_model=ActionResponse)
    async def restart_servers() -> ActionResponse:
        """Restart all development servers."""
        # Stop first
        await stop_servers()

        # Wait for ports to be fully released
        await asyncio.sleep(2)

        # Start with stored configuration
        request = ActionRequest(
            frontend_port=state.frontend_port,
            backend_port=state.backend_port,
            host=state.host,
            obo=state.obo,
            openapi=state.openapi_enabled,
            max_retries=state.max_retries,
        )

        start_response = await start_servers(request)

        # Combine messages
        if start_response.status == "success":
            return ActionResponse(
                status="success", message="Servers restarted successfully"
            )
        else:
            return start_response

    @app.get("/logs")
    async def stream_logs(
        duration: Annotated[
            int | None, Query(description="Show logs from last N seconds")
        ] = None,
        process: Annotated[
            Literal["frontend", "backend", "openapi", "all"] | None,
            Query(description="Filter by process name"),
        ] = "all",
    ) -> StreamingResponse:
        """Stream logs using Server-Sent Events (SSE)."""

        async def event_generator() -> AsyncGenerator[str, None]:
            """Generate SSE events for log streaming."""
            import datetime
            import json

            # Send initial buffered logs
            cutoff_time: datetime.datetime | None = None
            if duration:
                cutoff_time = datetime.datetime.now() - datetime.timedelta(
                    seconds=duration
                )

            # Send existing logs
            buffered_logs: list[LogEntry] = list(state.log_buffer)
            for log in buffered_logs:
                # Filter by process if specified
                if process != "all" and log.process_name != process:
                    continue

                # Filter by time if specified
                if cutoff_time:
                    try:
                        log_time = datetime.datetime.strptime(
                            log.timestamp, "%Y-%m-%d %H:%M:%S"
                        )
                        if log_time < cutoff_time:
                            continue
                    except Exception:
                        pass

                # Format SSE event (use model_dump for JSON serialization)
                yield f"data: {json.dumps(log.model_dump())}\n\n"

            # Send a sentinel event to mark end of buffered logs
            yield "event: buffered_done\ndata: {}\n\n"

            # Stream new logs as they arrive
            last_index = len(state.log_buffer) - 1

            while True:
                await asyncio.sleep(0.1)  # Check every 100ms

                # Check for new logs
                current_index = len(state.log_buffer) - 1
                if current_index > last_index:
                    # Get new logs
                    new_logs: list[LogEntry] = list(state.log_buffer)[last_index + 1 :]
                    for log in new_logs:
                        # Filter by process if specified
                        if process != "all" and log.process_name != process:
                            continue

                        # Format SSE event (use model_dump for JSON serialization)
                        yield f"data: {json.dumps(log.model_dump())}\n\n"

                    last_index = current_index

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return app


def run_dev_server(app_dir: Path, socket_path: Path | str):
    """Run the dev server on a Unix domain socket.

    Args:
        app_dir: Application directory
        socket_path: Path to Unix domain socket
    """
    import os
    import uvicorn

    # Change to app directory so get_project_metadata() works correctly
    os.chdir(app_dir)

    # Ensure socket path is a Path object
    if isinstance(socket_path, str):
        socket_path = Path(socket_path)

    # Remove old socket file if it exists
    if socket_path.exists():
        socket_path.unlink()

    # Ensure parent directory exists
    socket_path.parent.mkdir(parents=True, exist_ok=True)

    app = create_dev_server(app_dir)

    config = uvicorn.Config(
        app=app,
        uds=str(socket_path),
        log_level="info",
    )

    server = uvicorn.Server(config)
    asyncio.run(server.serve())
