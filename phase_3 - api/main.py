import asyncio
import time
import uuid
import sys 
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from asgi_correlation_id import CorrelationIdMiddleware
from fastapi.staticfiles import StaticFiles

from app.logging_setup import setup_logging, logger
from app.services.data_loader import load_and_process_data
from app.store import store
from app.routes import router
from app.database import connect_to_mongo, close_mongo_connection, get_database

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown events.
    """
    setup_logging()
    logger.info("--- Application Starting Up ---")

    try:
        await connect_to_mongo()
        db_instance = get_database()
        store.mark_loading()
        # Start the data loading process with the database instance
        asyncio.create_task(load_and_process_data(db_instance))
    except Exception as e:
        logger.critical(f"Could not connect to the database on startup: {e}", exc_info=True)
        # Exit with a non-zero status code to tell Docker the container failed
        sys.exit(1)

    yield
    await close_mongo_connection()
    logger.info("--- Application Shutting Down ---")

app = FastAPI(
    title="Knowledge Graph API",
    description="An API to query and filter entities from the knowledge graph.",
    version="1.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

# Mount the 'images' directory. This makes all files inside the 'images' folder
# available under the URL path '/static'.
# Assumes your 'images' folder is in the same directory as main.py
app.mount("/static", StaticFiles(directory="images"), name="static")



# MIDDLEWARE CONFIGURATION

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CorrelationIdMiddleware)

@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """
    Global middleware to handle logging and uncaught exceptions.
    """
    start_time = time.time()
    logger.info(
        "Request received",
        extra={"method": request.method, "url": str(request.url)}
    )
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        logger.info(
            "Request completed",
            extra={
                "method": request.method,
                "url": str(request.url),
                "status_code": response.status_code,
                "process_time_ms": f"{process_time:.2f}",
            },
        )
        return response
    except Exception as e:
        correlation_id = getattr(request.state, 'correlation_id', str(uuid.uuid4()))
        logger.critical(
            "Unhandled exception",
            extra={"method": request.method, "url": str(request.url), "error": str(e)},
            exc_info=True,
        )
        return ORJSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "message": "An unexpected error occurred.",
                "correlation_id": correlation_id,
            },
        )

#ROUTER INCLUSION
app.include_router(router)