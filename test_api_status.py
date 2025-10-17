#!/usr/bin/env python3
"""
Test the API status endpoints with new archiver fields.
"""

import asyncio
import sys
import os
import json
from datetime import datetime, timezone

# Add the project to Python path
sys.path.insert(0, os.path.dirname(__file__))

from lnr.config import Config
from lnr.services.archiver import ArchiverService
from lnr.status import status_manager
from lnr.web import app, services
from fastapi.testclient import TestClient


async def setup_test_data():
    """Set up test data for the archiver."""
    print("🔧 Setting up test data...")
    
    config = Config()
    archiver = ArchiverService(config)
    
    # Set up archiver with test data
    now = datetime.now(timezone.utc)
    archiver.last_rotation_time = now
    archiver.current_archive_message_count = 456
    archiver.current_archive_byte_count = 123456
    
    # Add to global services dict for API access
    services["archiver"] = archiver
    
    # Update status manager with test data
    await status_manager.update_service_status("archiver", "running")
    await status_manager.update_archiver_stats(456, 123456)
    
    print(f"   ✅ Archiver set up with {archiver.current_archive_message_count} messages, {archiver.current_archive_byte_count} bytes")
    return archiver


async def test_api_endpoints():
    """Test the API endpoints with new archiver fields."""
    print("\n🌐 Testing API endpoints...")
    
    # Set up test data first
    archiver = await setup_test_data()
    
    with TestClient(app) as client:
        # Test all services status endpoint
        print("\n📊 Testing /api/status endpoint...")
        response = client.get("/api/status")
        
        if response.status_code == 200:
            data = response.json()
            archiver_data = data.get("archiver", {})
            
            print(f"   Status: {response.status_code}")
            print(f"   Archiver status: {archiver_data.get('status', 'N/A')}")
            print(f"   Message count: {archiver_data.get('current_archive_messages', 'N/A')}")
            print(f"   Byte count: {archiver_data.get('current_archive_bytes', 'N/A')}")
            print(f"   Bytes (human): {archiver_data.get('current_archive_bytes_human', 'N/A')}")
            
            if "next_flush_time" in archiver_data:
                print(f"   Next flush: {archiver_data['next_flush_time']}")
                print(f"   Time to flush: {archiver_data.get('time_to_next_flush_human', 'N/A')}")
            else:
                print("   ⚠️  Next flush time not found in response")
        else:
            print(f"   ❌ Error: {response.status_code}")
            print(f"   Response: {response.text}")
        
        # Test individual archiver status endpoint
        print("\n🎯 Testing /api/status/archiver endpoint...")
        response = client.get("/api/status/archiver")
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"   Status: {response.status_code}")
            print(f"   Service name: {data.get('name', 'N/A')}")
            print(f"   Service status: {data.get('status', 'N/A')}")
            print(f"   Message count: {data.get('current_archive_messages', 'N/A')}")
            print(f"   Byte count: {data.get('current_archive_bytes', 'N/A')}")
            print(f"   Bytes (human): {data.get('current_archive_bytes_human', 'N/A')}")
            
            if "next_flush_time" in data:
                print(f"   Next flush: {data['next_flush_time']}")
                print(f"   Time to flush: {data.get('time_to_next_flush_human', 'N/A')}")
                print(f"   Seconds to flush: {data.get('time_to_next_flush_seconds', 'N/A')}")
            else:
                print("   ⚠️  Next flush time not found in response")
                
            # Pretty print the full response
            print(f"\n📋 Full archiver response:")
            print(json.dumps(data, indent=2))
        else:
            print(f"   ❌ Error: {response.status_code}")
            print(f"   Response: {response.text}")


if __name__ == "__main__":
    asyncio.run(test_api_endpoints())