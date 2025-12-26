import asyncio
import httpx
from app.config import DEFAULT_TIMEOUT, RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR
from app.logging_setup import logger

def get_async_client() -> httpx.AsyncClient:
    """
    Returns a configured httpx.AsyncClient with HTTP/2 support and default timeouts.
    """
    return httpx.AsyncClient(http2=True, timeout=DEFAULT_TIMEOUT)

async def fetch_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    semaphore: asyncio.Semaphore,
    **kwargs
) -> httpx.Response:
    """
    Performs an HTTP request with a semaphore for concurrency control and
    exponential backoff for retries on transient errors.
    """
    await semaphore.acquire()
    try:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                if method.upper() == "GET":
                    response = await client.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = await client.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                return response

            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                if isinstance(e, httpx.HTTPStatusError) and 400 <= e.response.status_code < 500:
                    logger.error(f"Non-retriable HTTP error for {url}: {e}")
                    raise

                if attempt == RETRY_ATTEMPTS - 1:
                    logger.error(f"Final attempt failed for {url}: {e}")
                    raise

                wait_time = RETRY_BACKOFF_FACTOR * (2 ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{RETRY_ATTEMPTS} failed for {url}. "
                    f"Retrying in {wait_time:.2f}s..."
                )
                await asyncio.sleep(wait_time)
        raise RuntimeError("Fetch with retry failed unexpectedly.")
    finally:
        semaphore.release()