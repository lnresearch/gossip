"""
GSP (Gossip Snapshot Protocol) format utilities.

This module implements the GSP\x01 binary format for efficient storage
of Lightning Network gossip messages.
"""

import struct
from typing import BinaryIO, Iterator


# GSP format constants
GSP_HEADER = b'GSP\x01'
GSP_VERSION = 1


class CompactSizeError(Exception):
    """Error in CompactSize encoding/decoding."""
    pass


def encode_compact_size(value: int) -> bytes:
    """
    Encode an integer using CompactSize format (Bitcoin protocol).
    
    Args:
        value: Integer value to encode (must be >= 0)
        
    Returns:
        bytes: CompactSize encoded bytes
        
    Raises:
        CompactSizeError: If value is negative or too large
    """
    if value < 0:
        raise CompactSizeError(f"CompactSize cannot encode negative values: {value}")
    
    if value <= 252:
        # Single byte for values 0-252
        return struct.pack('B', value)
    elif value <= 0xFFFF:
        # 0xFD + 2-byte big-endian for values 253-65535
        return struct.pack('>BH', 0xFD, value)
    elif value <= 0xFFFFFFFF:
        # 0xFE + 4-byte big-endian for values 65536-4294967295
        return struct.pack('>BI', 0xFE, value)
    elif value <= 0xFFFFFFFFFFFFFFFF:
        # 0xFF + 8-byte big-endian for values 4294967296+
        return struct.pack('>BQ', 0xFF, value)
    else:
        raise CompactSizeError(f"Value too large for CompactSize encoding: {value}")


def decode_compact_size(data: bytes, offset: int = 0) -> tuple[int, int]:
    """
    Decode a CompactSize value from bytes.
    
    Args:
        data: Bytes to decode from
        offset: Starting offset in data
        
    Returns:
        tuple: (decoded_value, bytes_consumed)
        
    Raises:
        CompactSizeError: If data is invalid or incomplete
    """
    if offset >= len(data):
        raise CompactSizeError("Not enough data to decode CompactSize")
    
    first_byte = data[offset]
    
    if first_byte <= 252:
        return first_byte, 1
    elif first_byte == 0xFD:
        # 2-byte big-endian value
        if offset + 3 > len(data):
            raise CompactSizeError("Not enough data for 2-byte CompactSize")
        value = struct.unpack('>H', data[offset + 1:offset + 3])[0]
        if value <= 252:
            raise CompactSizeError(f"CompactSize value {value} should use 1-byte encoding")
        return value, 3
    elif first_byte == 0xFE:
        # 4-byte big-endian value
        if offset + 5 > len(data):
            raise CompactSizeError("Not enough data for 4-byte CompactSize")
        value = struct.unpack('>I', data[offset + 1:offset + 5])[0]
        if value <= 0xFFFF:
            raise CompactSizeError(f"CompactSize value {value} should use smaller encoding")
        return value, 5
    elif first_byte == 0xFF:
        # 8-byte big-endian value
        if offset + 9 > len(data):
            raise CompactSizeError("Not enough data for 8-byte CompactSize")
        value = struct.unpack('>Q', data[offset + 1:offset + 9])[0]
        if value <= 0xFFFFFFFF:
            raise CompactSizeError(f"CompactSize value {value} should use smaller encoding")
        return value, 9
    else:
        raise CompactSizeError(f"Invalid CompactSize first byte: {first_byte}")


def read_compact_size(file: BinaryIO) -> int:
    """
    Read a CompactSize value from a binary file.
    
    Args:
        file: Binary file to read from
        
    Returns:
        int: Decoded CompactSize value
        
    Raises:
        CompactSizeError: If file data is invalid or incomplete
        EOFError: If end of file reached unexpectedly
    """
    first_byte_data = file.read(1)
    if not first_byte_data:
        raise EOFError("End of file reached while reading CompactSize")
    
    first_byte = first_byte_data[0]
    
    if first_byte <= 252:
        return first_byte
    elif first_byte == 0xFD:
        data = file.read(2)
        if len(data) != 2:
            raise EOFError("End of file reached while reading 2-byte CompactSize")
        value = struct.unpack('>H', data)[0]
        if value <= 252:
            raise CompactSizeError(f"CompactSize value {value} should use 1-byte encoding")
        return value
    elif first_byte == 0xFE:
        data = file.read(4)
        if len(data) != 4:
            raise EOFError("End of file reached while reading 4-byte CompactSize")
        value = struct.unpack('>I', data)[0]
        if value <= 0xFFFF:
            raise CompactSizeError(f"CompactSize value {value} should use smaller encoding")
        return value
    elif first_byte == 0xFF:
        data = file.read(8)
        if len(data) != 8:
            raise EOFError("End of file reached while reading 8-byte CompactSize")
        value = struct.unpack('>Q', data)[0]
        if value <= 0xFFFFFFFF:
            raise CompactSizeError(f"CompactSize value {value} should use smaller encoding")
        return value
    else:
        raise CompactSizeError(f"Invalid CompactSize first byte: {first_byte}")


class GSPWriter:
    """Writer for GSP format files."""
    
    def __init__(self, file: BinaryIO):
        """
        Initialize GSP writer.
        
        Args:
            file: Binary file opened for writing
        """
        self.file = file
        self.header_written = False
    
    def write_header(self) -> None:
        """Write GSP file header if not already written."""
        if not self.header_written:
            self.file.write(GSP_HEADER)
            self.header_written = True
    
    def ensure_header(self) -> None:
        """Ensure header is written, checking file position."""
        # If we're at the beginning of the file, write the header
        if self.file.tell() == 0:
            self.write_header()
    
    def write_message(self, message_data: bytes) -> None:
        """
        Write a message to the GSP file.
        
        Args:
            message_data: Raw Lightning Network gossip message bytes
        """
        self.ensure_header()
        
        # Encode message length using CompactSize
        length_bytes = encode_compact_size(len(message_data))
        
        # Write length + message data
        self.file.write(length_bytes)
        self.file.write(message_data)
    
    def flush(self) -> None:
        """Flush the underlying file."""
        self.file.flush()


class GSPReader:
    """Reader for GSP format files."""
    
    def __init__(self, file: BinaryIO):
        """
        Initialize GSP reader.
        
        Args:
            file: Binary file opened for reading
        """
        self.file = file
        self.header_verified = False
    
    def verify_header(self) -> None:
        """Verify GSP file header."""
        if not self.header_verified:
            header = self.file.read(4)
            if header != GSP_HEADER:
                raise ValueError(f"Invalid GSP header: expected {GSP_HEADER!r}, got {header!r}")
            self.header_verified = True
    
    def read_message(self) -> bytes | None:
        """
        Read the next message from the GSP file.
        
        Returns:
            bytes: Message data, or None if end of file
            
        Raises:
            ValueError: If file format is invalid
            CompactSizeError: If CompactSize encoding is invalid
        """
        self.verify_header()
        
        try:
            # Read message length
            length = read_compact_size(self.file)
            
            # Read message data
            message_data = self.file.read(length)
            if len(message_data) != length:
                raise EOFError(f"Expected {length} bytes, got {len(message_data)}")
            
            return message_data
            
        except EOFError:
            # End of file reached
            return None
    
    def read_messages(self) -> Iterator[bytes]:
        """
        Iterator that yields all messages in the GSP file.
        
        Yields:
            bytes: Each message data
        """
        while True:
            message = self.read_message()
            if message is None:
                break
            yield message