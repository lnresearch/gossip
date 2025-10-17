#!/usr/bin/env python3
"""
Test dedup service directly.
"""

import asyncio
import logging
import sys
import os

# Add the project to Python path
sys.path.insert(0, os.path.dirname(__file__))

from lnr.config import Config
from lnr.services.dedup import DedupService

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_dedup_service():
    """Test the dedup service directly."""
    try:
        logger.info("Creating dedup service...")
        
        # Override config to use local RabbitMQ
        config = Config()
        logger.info(f"Using RabbitMQ URL: {config.rabbitmq_url}")
        logger.info(f"Using database: {config.database_path}")
        
        # Create and start dedup service
        dedup_service = DedupService(config)
        
        logger.info("Starting dedup service...")
        await dedup_service.start()
        
        logger.info("✅ Dedup service started, running for 10 seconds...")
        await asyncio.sleep(10)
        
        logger.info("Stopping dedup service...")
        await dedup_service.stop()
        
        logger.info("✅ Test completed")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_dedup_service())