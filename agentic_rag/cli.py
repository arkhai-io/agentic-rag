"""CLI commands for agentic-rag."""

import sys


def start_server() -> None:
    """Start the FastAPI server using uvicorn."""
    try:
        import uvicorn
    except ImportError:
        print(
            "Error: FastAPI dependencies not installed. "
            "Install with: pip install .[api]"
        )
        sys.exit(1)

    from agentic_rag.api import app

    # Set number of workers to 1 to ensure a common queue.
    # Assuming GPU is the bottleneck this should still
    # be parallelized efficiently by uvicorn due to async handling.
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        workers=1,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
