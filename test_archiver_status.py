#!/usr/bin/env python3
"""
Test the new archiver status fields.
"""

import asyncio
import sys
import os
from datetime import datetime, timezone

# Add the project to Python path
sys.path.insert(0, os.path.dirname(__file__))

from lnr.config import Config
from lnr.services.archiver import ArchiverService
from lnr.status import status_manager


async def test_archiver_status():
    """Test the new archiver status fields."""
    print("🧪 Testing archiver status fields...")
    
    config = Config()
    archiver = ArchiverService(config)
    
    # Test next flush time calculation
    print(f"\n📅 Archive rotation: {config.archive_rotation}")
    
    # Set a fake last rotation time for testing
    now = datetime.now(timezone.utc)
    archiver.last_rotation_time = now
    
    next_flush = archiver.calculate_next_flush_time()
    if next_flush:
        time_diff = next_flush - now
        print(f"⏰ Next flush time: {next_flush.isoformat()}")
        print(f"⏱️  Time to next flush: {time_diff}")
        print(f"📊 Seconds to flush: {int(time_diff.total_seconds())}")
    else:
        print("❌ Could not calculate next flush time")
    
    # Test byte formatting
    test_sizes = [0, 512, 1536, 1048576, 1073741824, 5368709120]
    print(f"\n💾 Byte formatting tests:")
    for size in test_sizes:
        formatted = archiver.format_bytes(size)
        print(f"   {size:>10} bytes -> {formatted}")
    
    # Test status tracking
    print(f"\n📈 Testing status tracking...")
    
    # Update some fake statistics
    await status_manager.update_archiver_stats(42, 2048)
    
    # Get archiver info
    archiver_info = await status_manager.get_service_info("archiver")
    if archiver_info:
        print(f"   Current archive messages: {archiver_info.current_archive_messages}")
        print(f"   Current archive bytes: {archiver_info.current_archive_bytes}")
        print(f"   Formatted bytes: {archiver.format_bytes(archiver_info.current_archive_bytes)}")
    
    # Test the service instance tracking methods
    archiver.current_archive_message_count = 123
    archiver.current_archive_byte_count = 45678
    
    print(f"\n🔧 Service instance tracking:")
    print(f"   Instance message count: {archiver.current_archive_message_count}")
    print(f"   Instance byte count: {archiver.current_archive_byte_count}")
    print(f"   Formatted: {archiver.format_bytes(archiver.current_archive_byte_count)}")
    
    print(f"\n✅ Archiver status test completed!")


if __name__ == "__main__":
    asyncio.run(test_archiver_status())