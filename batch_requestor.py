import asyncio
import time
import logging
from typing import Optional, Callable, Any, List
from dataclasses import dataclass
from collections import deque

@dataclass
class BatchRequest:
    func: Callable
    args: tuple
    kwargs: dict
    future: asyncio.Future

class BatchRequestor:
    """
    A request batcher that processes requests in batches with cooldown periods.
    
    Attributes:
        batch_size (int): Number of requests to process in a single batch
        cooldown (float): Time to wait after processing a batch
    """
    
    def __init__(self, batch_size: int = 5, cooldown: float = 2.0):
        """
        Initialize the batch requestor.
        
        Args:
            batch_size (int): Number of requests to process in a single batch
            cooldown (float): Time to wait after processing a batch
        """
        self.batch_size = batch_size
        self.cooldown = cooldown
        self.request_queue = deque()
        self.logger = logging.getLogger(__name__)
        self.processing = False
        self.lock = asyncio.Lock()

    async def process_batch(self):
        """Process requests in batches with cooldown periods."""
        if self.processing:
            return
        
        async with self.lock:
            if self.processing:  # Double-check pattern
                return
            self.processing = True
        
        try:
            while self.request_queue:
                batch = []
                # Get up to batch_size requests
                for _ in range(min(self.batch_size, len(self.request_queue))):
                    if self.request_queue:
                        batch.append(self.request_queue.popleft())
                
                if not batch:
                    break
                
                self.logger.info(f"Processing batch of {len(batch)} requests")
                batch_start = time.time()
                
                # Process the batch
                for i, request in enumerate(batch, 1):
                    try:
                        self.logger.debug(f"Processing request {i}/{len(batch)} in current batch")
                        result = await request.func(*request.args, **request.kwargs)
                        request.future.set_result(result)
                    except Exception as e:
                        self.logger.error(f"Request {i}/{len(batch)} failed: {str(e)}")
                        request.future.set_exception(e)
                        # Continue processing other requests in the batch
                
                batch_duration = time.time() - batch_start
                self.logger.info(f"Batch complete in {batch_duration:.2f}s. {len(self.request_queue)} requests remaining")
                
                # Wait for cooldown period before next batch
                if self.request_queue:
                    self.logger.info(f"Waiting {self.cooldown}s before next batch")
                    await asyncio.sleep(self.cooldown)
        except Exception as e:
            self.logger.error(f"Batch processing error: {str(e)}")
            # Mark any remaining requests in the current batch as failed
            for request in batch:
                if not request.future.done():
                    request.future.set_exception(e)
        finally:
            self.processing = False

    async def submit_request(self, func: Callable, *args, **kwargs) -> Any:
        """
        Submit a request to be processed in a batch.
        
        Args:
            func (Callable): The async function to execute
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Any: The result of the function
        """
        future = asyncio.Future()
        request = BatchRequest(func, args, kwargs, future)
        self.request_queue.append(request)
        
        # Start processing if not already running
        asyncio.create_task(self.process_batch())
        return await future

async def with_batching(batch_requestor: BatchRequestor, func: Callable, *args, **kwargs) -> Any:
    """
    Execute a function with request batching.
    
    Args:
        batch_requestor (BatchRequestor): The batch requestor to use
        func (Callable): The function to execute
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Any: The result of the function
    """
    logger = logging.getLogger(__name__)
    task_name = f"{func.__name__}_{id(func)}"
    logger.debug(f"Submitting batched request: {task_name}")
    start_time = time.time()
    
    try:
        result = await batch_requestor.submit_request(func, *args, **kwargs)
        duration = time.time() - start_time
        logger.debug(f"Completed batched request: {task_name} (took {duration:.2f}s)")
        return result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Failed batched request: {task_name} after {duration:.2f}s - {str(e)}")
        raise 