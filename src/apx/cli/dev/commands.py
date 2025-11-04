"""Dev commands for the apx CLI."""

import os
import subprocess
from pathlib import Path
from typing import Annotated

from dotenv import load_dotenv
from typer import Argument, Exit, Option, Typer

from databricks.sdk import WorkspaceClient

from apx import __version__ as apx_lib_version
from apx.cli.dev.manager import (
    DevManager,
    validate_databricks_credentials,
    delete_token_from_keyring,
    save_token_id,
)
from apx.cli.dev.logging import suppress_output_and_logs
from apx.cli.version import with_version
from apx.utils import (
    console,
    is_bun_installed,
)


# Create the dev app (subcommand group)
dev_app = Typer(name="dev", help="Manage development servers")


@dev_app.command(
    name="_run_server",
    hidden=True,
    help="Internal: Run dev server in detached mode",
)
def _run_server(
    app_dir: Path = Argument(..., help="App directory"),
    socket_path: Path = Argument(..., help="Socket path"),
    frontend_port: int = Argument(..., help="Frontend port"),
    backend_port: int = Argument(..., help="Backend port"),
    host: str = Argument(..., help="Host for servers"),
    obo: str = Argument(..., help="Enable OBO (true/false)"),
    openapi: str = Argument(..., help="Enable OpenAPI (true/false)"),
    max_retries: int = Argument(10, help="Maximum retry attempts"),
):
    """Internal command to run dev server. Not meant for direct use."""
    from apx.cli.dev.server import run_dev_server

    run_dev_server(app_dir, socket_path)


@dev_app.command(name="start", help="Start development servers in detached mode")
@with_version
def dev_start(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
    frontend_port: Annotated[
        int, Option(help="Port for the frontend development server")
    ] = 5173,
    backend_port: Annotated[int, Option(help="Port for the backend server")] = 8000,
    host: Annotated[
        str, Option(help="Host for dev, frontend, and backend servers")
    ] = "localhost",
    obo: Annotated[
        bool, Option(help="Whether to add On-Behalf-Of header to the backend server")
    ] = True,
    openapi: Annotated[
        bool, Option(help="Whether to start OpenAPI watcher process")
    ] = True,
    max_retries: Annotated[
        int, Option(help="Maximum number of retry attempts for processes")
    ] = 10,
    watch: Annotated[
        bool,
        Option(
            "--watch",
            "-w",
            help="Start servers and tail logs until Ctrl+C, then stop all servers",
        ),
    ] = False,
):
    """Start development servers in detached mode."""
    # Check prerequisites
    if not is_bun_installed():
        console.print(
            "[red]❌ bun is not installed. Please install bun to continue.[/red]"
        )
        raise Exit(code=1)

    if app_dir is None:
        app_dir = Path.cwd()

    # Validate Databricks credentials if OBO is enabled
    if obo:
        console.print("[cyan]🔐 Validating Databricks credentials...[/cyan]")

        dotenv_path = app_dir / ".env"
        if dotenv_path.exists():
            console.print(f"🔍 Loading .env file from {dotenv_path.resolve()}")
            load_dotenv(dotenv_path)

        try:
            with suppress_output_and_logs():
                ws = WorkspaceClient(product="apx/dev", product_version=apx_lib_version)
        except Exception as e:
            console.print(
                f"[red]❌ Failed to initialize Databricks client for OBO token generation: {e}[/red]"
            )
            console.print(
                "[yellow]💡 Make sure you have Databricks credentials configured.[/yellow]"
            )
            raise Exit(code=1)

        if not validate_databricks_credentials(ws):
            # Clear any cached OBO tokens since they were created with invalid credentials
            keyring_id = str(app_dir.resolve())
            console.print(
                "[yellow]⚠️  Invalid Databricks credentials detected. Clearing cached tokens...[/yellow]"
            )
            delete_token_from_keyring(keyring_id)
            save_token_id(app_dir, token_id="")  # Clear the token_id

            # Raise error and don't start the server
            console.print(
                "[red]❌ Failed to authenticate with Databricks. Cannot start server with --obo flag.[/red]"
            )
            console.print(
                "[yellow]💡 Please check your Databricks credentials and try again.[/yellow]"
            )

            # If using a specific profile, show re-authentication command
            profile_name = os.environ.get("DATABRICKS_CONFIG_PROFILE")
            if profile_name:
                console.print()
                console.print(
                    "[cyan]Use Databricks CLI to re-authenticate with identified profile:[/cyan]"
                )
                console.print()
                console.print(
                    f"  [bold]> databricks auth login -p {profile_name}[/bold]"
                )
                console.print()

            raise Exit(code=1)

        console.print("[green]✓[/green] Databricks credentials validated")
        console.print()

    # Use DevManager to start servers
    manager = DevManager(app_dir)
    manager.start(
        frontend_port=frontend_port,
        backend_port=backend_port,
        host=host,
        obo=obo,
        openapi=openapi,
        max_retries=max_retries,
        watch=watch,
    )

    # If watch mode is enabled, stream logs until Ctrl+C
    if watch:
        console.print()
        console.print(
            "[bold cyan]📡 Streaming logs... Press Ctrl+C to stop servers[/bold cyan]"
        )
        console.print()
        # stream_logs catches KeyboardInterrupt internally, so it returns normally
        # After it returns (for any reason), we should stop the servers
        manager.stream_logs(
            duration_seconds=None,
            ui_only=False,
            backend_only=False,
            openapi_only=False,
            app_only=False,
            raw_output=False,
            follow=True,
        )
        console.print()
        console.print("[bold yellow]🛑 Stopping development servers...[/bold yellow]")
        manager.stop()


@dev_app.command(name="status", help="Check the status of development servers")
@with_version
def dev_status(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
):
    """Check the status of development servers."""
    if app_dir is None:
        app_dir = Path.cwd()

    # Use DevManager to check status
    manager = DevManager(app_dir)
    manager.status()


@dev_app.command(name="stop", help="Stop development servers")
@with_version
def dev_stop(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
):
    """Stop development servers."""
    if app_dir is None:
        app_dir = Path.cwd()

    # Use DevManager to stop servers
    manager = DevManager(app_dir)
    manager.stop()


@dev_app.command(name="restart", help="Restart development servers")
def dev_restart(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
):
    """Restart development servers using the dev server API."""
    if app_dir is None:
        app_dir = Path.cwd()

    # Use DevManager to restart servers
    manager = DevManager(app_dir)

    if not manager.is_dev_server_running():
        console.print("[yellow]No development server found.[/yellow]")
        console.print("[dim]Run 'apx dev start' to start the server.[/dim]")
        raise Exit(code=1)

    console.print("[bold yellow]🔄 Restarting development servers...[/bold yellow]")

    manager.stop()
    manager.start()

    console.print(
        "[bold green]✨ Development servers restarted successfully![/bold green]"
    )


@dev_app.command(name="logs", help="Display logs from development servers")
def dev_logs(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
    duration: Annotated[
        int | None,
        Option(
            "--duration",
            "-d",
            help="Show logs from the last N seconds (None = all logs)",
        ),
    ] = None,
    follow: Annotated[
        bool,
        Option(
            "--follow",
            "-f",
            help="Follow log output (like tail -f). Streams new logs continuously.",
        ),
    ] = False,
    ui: Annotated[
        bool,
        Option("--ui", help="Show only frontend/UI logs"),
    ] = False,
    backend: Annotated[
        bool,
        Option("--backend", help="Show only backend logs"),
    ] = False,
    openapi: Annotated[
        bool,
        Option("--openapi", help="Show only OpenAPI logs"),
    ] = False,
    app: Annotated[
        bool,
        Option("--app", help="Show only application logs (from your app code)"),
    ] = False,
    raw: Annotated[
        bool,
        Option("--raw", help="Show raw log output without prefix formatting"),
    ] = False,
):
    """Display logs from development servers. Use -f/--follow to stream continuously."""
    if app_dir is None:
        app_dir = Path.cwd()

    # Use DevManager to stream logs
    manager = DevManager(app_dir)
    manager.stream_logs(
        duration_seconds=duration,
        ui_only=ui,
        backend_only=backend,
        openapi_only=openapi,
        app_only=app,
        raw_output=raw,
        follow=follow,
    )


@dev_app.command(name="check", help="Check the project code for errors")
@with_version
def dev_check(
    app_dir: Annotated[
        Path | None,
        Argument(
            help="The path to the app. If not provided, current working directory will be used"
        ),
    ] = None,
):
    """Check the project code for errors."""
    if app_dir is None:
        app_dir = Path.cwd()

    console.print(
        "[cyan]🔍 Checking project code for error, starting with TypeScript...[/cyan]"
    )
    console.print("[dim]Running 'bun run tsc -b --incremental'[/dim]")

    # run tsc to check for errors
    result = subprocess.run(
        ["bun", "run", "tsc", "-b", "--incremental"],
        cwd=app_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print("[red]❌ TypeScript compilation failed, errors provided below[/]")
        for line in result.stdout.splitlines():
            console.print(f"[red]{line}[/red]")
        raise Exit(code=1)

    console.print("[green]✅ TypeScript compilation succeeded[/green]")
    console.print()

    console.print("[cyan]🔍 Checking Python code for errors...[/cyan]")
    console.print("[dim]Running 'uv run basedpyright --level error'[/dim]")

    # run pyright to check for errors
    result = subprocess.run(
        ["uv", "run", "basedpyright", "--level", "error"],
        cwd=app_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print("[red]❌ Pyright found errors, errors provided below[/]")
        for line in result.stdout.splitlines():
            console.print(f"[red]{line}[/red]")
        raise Exit(code=1)
    else:
        console.print("[green]✅ Pyright found no errors[/green]")


@dev_app.command(name="mcp", help="Start MCP server for development server management")
def dev_mcp():
    """Start MCP server that provides tools for managing development servers.

    The MCP server runs over stdio and provides the following tools:
    - start: Start development servers (frontend, backend, OpenAPI watcher)
    - restart: Restart all development servers
    - stop: Stop all development servers
    - status: Get the status of all development servers
    - get_metadata: Get project metadata from pyproject.toml

    This command should be run from the project root directory.
    """
    from apx.cli.dev.mcp import run_mcp_server

    run_mcp_server()
