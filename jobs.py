import asyncio
import random
import time

class BaseJob:
    """Abstract base class for all jobs."""
    name = "BaseJob"
    async def execute(self, payload: dict):
        raise NotImplementedError("Subclasses must implement execute()")

class Prowler(BaseJob):
    """AWS security assessment job using Prowler."""
    name = "Prowler"
    async def execute(self, payload):
        time.sleep(random.uniform(0.1, 0.5))  # simulate work
        region = payload.get("region")
        account_id = payload.get("account_id")
        if not region or not account_id:
            raise ValueError("Prowler job requires 'region' and 'account_id' in payload")
        
        # 60% chance of failure
        if random.random() < 0.6:
            failure_reasons = [
                "AWS API rate limit exceeded",
                "Insufficient permissions to access account",
                "Network connectivity issues",
                "Account is not accessible",
                "Prowler scan timeout"
            ]
            return {
                "status": "failed",
                "result": f"Security assessment failed for account {account_id} in {region}: {random.choice(failure_reasons)}"
            }
            
        return {"status": "success", "result": f"Security assessment completed for account {account_id} in {region}"}

class LongRunningJob(BaseJob):
    """A job that simulates a long-running task."""
    name = "LongRunningJob"
    async def execute(self, payload):
        seconds = payload.get("seconds", 0)
        if not isinstance(seconds, (int, float)) or seconds <= 0:
            raise ValueError("LongRunningJob requires positive 'seconds' in payload")
        await asyncio.sleep(seconds)  # actually sleep for the specified duration
        return {"status": "success", "result": f"Completed long running task after {seconds} seconds"}

class Shooker(BaseJob):
    """A job that shakes up a list of numbers."""
    name = "Shooker"
    async def execute(self, payload):
        time.sleep(random.uniform(0.1, 0.5))
        numbers = payload.get("numbers", [])
        if not isinstance(numbers, list):
            raise ValueError("Shooker job requires 'numbers' list in payload")
        # Shake up the numbers by shuffling them
        shuffled = numbers.copy()
        random.shuffle(shuffled)
        return {"status": "success", "result": shuffled}