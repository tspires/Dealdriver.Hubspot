"""Script to start Celery workers."""

import subprocess
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def start_workers():
    """Start Celery workers for different queues."""
    workers = [
        # Scraping worker (fewer concurrent tasks due to browser usage)
        {
            "name": "scraping_worker",
            "queue": "scraping",
            "concurrency": 2,
            "pool": "prefork"
        },
        # Enrichment worker (AI analysis)
        {
            "name": "enrichment_worker", 
            "queue": "enrichment",
            "concurrency": 4,
            "pool": "prefork"
        },
        # Export worker (file I/O)
        {
            "name": "export_worker",
            "queue": "export",
            "concurrency": 4,
            "pool": "prefork"
        }
    ]
    
    processes = []
    
    for worker in workers:
        cmd = [
            "celery",
            "-A", "celery_app",
            "worker",
            "--loglevel=info",
            f"--hostname={worker['name']}@%h",
            f"--queues={worker['queue']}",
            f"--concurrency={worker['concurrency']}",
            f"--pool={worker['pool']}"
        ]
        
        print(f"Starting {worker['name']} with command: {' '.join(cmd)}")
        process = subprocess.Popen(cmd)
        processes.append(process)
    
    print(f"\nStarted {len(processes)} Celery workers")
    print("Press Ctrl+C to stop all workers")
    
    try:
        # Wait for all processes
        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait()
        print("All workers stopped")


if __name__ == "__main__":
    start_workers()