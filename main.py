"""
Affiliate Bot - Main Entry Point
Automated affiliate marketing bot that finds and posts game deals to Twitter
"""
import sys
import os
import threading
import signal
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import logger
from utils.helpers import ensure_directories
from automation.scheduler import run_scheduler
from routes.auth import auth_router

shutdown_event = threading.Event()

def run_api_server():
    """Run the API server in a separate thread"""
    try:
        import uvicorn
        from fastapi import FastAPI
        from fastapi.middleware.cors import CORSMiddleware
        from routes.items import router as items_router
        from routes.admin import admin_router
        
        app = FastAPI(
            title="Affiliate Bot API",
            description="API for managing affiliate deals and offers",
            version="1.0.0"
        )
        
        # CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Include routers
        app.include_router(items_router, prefix="/api", tags=["offers"])
        app.include_router(auth_router, prefix="/api/auth", tags=["auth"])
        app.include_router(admin_router, prefix="/api/admin", tags=["admin"])
        
        @app.get("/api")
        async def root():
            return {"status": "Hello"}
        
        logger.info("Starting API server on http://0.0.0.0:8000")
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8001,
            log_level="info",
            access_log=False  # Disable access logs to reduce noise
        )
    except ImportError:
        logger.warning("FastAPI/uvicorn not installed. API server will not start.")
        logger.warning("Install with: pip install fastapi uvicorn")
    except Exception as e:
        logger.error(f"Error starting API server: {e}")

def run_scheduler_thread():
    """Run the scheduler in a separate thread"""
    try:
        run_scheduler()
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        shutdown_event.set()

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Shutdown signal received, stopping services...")
    shutdown_event.set()
    sys.exit(0)

def main():
    """
    Main entry point for the affiliate bot
    Starts both the scheduler and API server concurrently
    """
    logger.info("="*60)
    logger.info("Starting Affiliate Bot")
    logger.info("="*60)
    
    # Ensure all required directories exist
    ensure_directories()
    logger.info("Directories verified")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start API server in a separate thread
    api_thread = threading.Thread(target=run_api_server, daemon=True)
    api_thread.start()
    
    # Give API server a moment to start
    time.sleep(1)
    
    # Start scheduler in main thread (runs indefinitely)
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
