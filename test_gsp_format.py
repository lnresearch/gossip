#!/usr/bin/env python3
"""
Test the GSP\x01 format implementation.
"""

import sys
import os
import tempfile
from pathlib import Path

# Add the project to Python path
sys.path.insert(0, os.path.dirname(__file__))

from lnr.gsp import GSPWriter, GSPReader, encode_compact_size, decode_compact_size, CompactSizeError


def test_compact_size():
    """Test CompactSize encoding/decoding."""
    print("🧪 Testing CompactSize encoding/decoding...")
    
    test_cases = [
        0, 1, 252,  # 1-byte values
        253, 255, 65535,  # 2-byte values
        65536, 100000, 4294967295,  # 4-byte values
        4294967296, 1000000000000,  # 8-byte values
    ]
    
    for value in test_cases:
        # Test encoding
        encoded = encode_compact_size(value)
        
        # Test decoding
        decoded, bytes_consumed = decode_compact_size(encoded)
        
        print(f"   {value:>12} -> {len(encoded)} bytes -> {decoded} (consumed {bytes_consumed})")
        
        assert decoded == value, f"Decode mismatch: {value} != {decoded}"
        assert bytes_consumed == len(encoded), f"Byte count mismatch: {bytes_consumed} != {len(encoded)}"
    
    print("   ✅ All CompactSize tests passed!")


def test_gsp_format():
    """Test GSP format writing and reading."""
    print("\n📁 Testing GSP format...")
    
    # Create test messages (Lightning Network-like)
    test_messages = [
        b'\x01\x00' + b'A' * 136,  # channel_announcement (138 bytes)
        b'\x01\x01' + b'B' * 150,  # node_announcement (152 bytes)
        b'\x01\x02' + b'C' * 100,  # channel_update (102 bytes)
    ]
    
    # Write GSP file
    with tempfile.NamedTemporaryFile(suffix='.gsp', delete=False) as temp_file:
        temp_path = Path(temp_file.name)
        
        print(f"   Writing to: {temp_path}")
        
        # Write messages
        with open(temp_path, 'wb') as f:
            writer = GSPWriter(f)
            for i, message in enumerate(test_messages):
                writer.write_message(message)
                print(f"   Wrote message {i+1}: {len(message)} bytes")
        
        # Check file size
        file_size = temp_path.stat().st_size
        print(f"   File size: {file_size} bytes")
        
        # Read messages back
        print(f"   Reading back messages...")
        with open(temp_path, 'rb') as f:
            reader = GSPReader(f)
            
            read_messages = []
            for message in reader.read_messages():
                read_messages.append(message)
                print(f"   Read message: {len(message)} bytes")
        
        # Verify messages match
        assert len(read_messages) == len(test_messages), f"Message count mismatch: {len(read_messages)} != {len(test_messages)}"
        
        for i, (original, read_back) in enumerate(zip(test_messages, read_messages)):
            assert original == read_back, f"Message {i} mismatch"
            print(f"   ✅ Message {i+1} matches")
        
        # Clean up
        temp_path.unlink()
        
    print("   ✅ GSP format test passed!")


def test_gsp_file_structure():
    """Test GSP file structure in detail."""
    print("\n🔍 Testing GSP file structure...")
    
    with tempfile.NamedTemporaryFile(suffix='.gsp', delete=False) as temp_file:
        temp_path = Path(temp_file.name)
        
        # Write a single test message
        test_message = b'\x01\x00' + b'X' * 10  # 12 bytes total
        
        with open(temp_path, 'wb') as f:
            writer = GSPWriter(f)
            writer.write_message(test_message)
        
        # Read raw file bytes to verify structure
        raw_bytes = temp_path.read_bytes()
        
        print(f"   Raw file bytes ({len(raw_bytes)} total): {raw_bytes.hex()}")
        
        # Check header
        header = raw_bytes[:4]
        expected_header = b'GSP\x01'
        assert header == expected_header, f"Header mismatch: {header!r} != {expected_header!r}"
        print(f"   ✅ Header: {header!r}")
        
        # Check CompactSize length
        length_encoded = encode_compact_size(len(test_message))
        actual_length_bytes = raw_bytes[4:4+len(length_encoded)]
        assert actual_length_bytes == length_encoded, f"Length encoding mismatch"
        print(f"   ✅ Length: {actual_length_bytes.hex()} (CompactSize for {len(test_message)})")
        
        # Check message data
        message_start = 4 + len(length_encoded)
        actual_message = raw_bytes[message_start:]
        assert actual_message == test_message, f"Message data mismatch"
        print(f"   ✅ Message: {len(actual_message)} bytes")
        
        # Clean up
        temp_path.unlink()
        
    print("   ✅ GSP file structure test passed!")


def test_error_conditions():
    """Test error conditions."""
    print("\n⚠️  Testing error conditions...")
    
    # Test CompactSize errors
    try:
        encode_compact_size(-1)
        assert False, "Should have raised CompactSizeError for negative value"
    except CompactSizeError:
        print("   ✅ CompactSize negative value error caught")
    
    # Test invalid GSP header
    with tempfile.NamedTemporaryFile(suffix='.gsp', delete=False) as temp_file:
        temp_path = Path(temp_file.name)
        
        # Write invalid header
        temp_path.write_bytes(b'INVALID\x01')
        
        try:
            with open(temp_path, 'rb') as f:
                reader = GSPReader(f)
                list(reader.read_messages())  # Try to read
            assert False, "Should have raised ValueError for invalid header"
        except ValueError:
            print("   ✅ Invalid header error caught")
        
        # Clean up
        temp_path.unlink()
    
    print("   ✅ Error condition tests passed!")


if __name__ == "__main__":
    print("🚀 Testing GSP\\x01 format implementation...")
    
    test_compact_size()
    test_gsp_format()
    test_gsp_file_structure()
    test_error_conditions()
    
    print("\n✅ All GSP\\x01 format tests passed!")