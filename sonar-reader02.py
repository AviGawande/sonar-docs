import struct
from enum import IntEnum
from typing import BinaryIO, Dict, List, Any
from dataclasses import dataclass

class PageVersion(IntEnum):
    """Enum for valid page versions"""
    SYSTEM_3000 = 3000
    SYSTEM_3000_V4 = 3001
    UUV_3500_LF = 3501
    UUV_3500_HF = 3502
    SYSTEM_5000 = 5000
    SYSTEM_5000_V4 = 5001
    SYSTEM_5000_BATHY = 5002
    SYSTEM_5000_V2_BATHY = 5004

@dataclass
class HeaderField:
    """Represents a header field with its data type and size"""
    name: str
    format: str  # struct format string
    size: int

class SonarDataReader:
    # Header field definitions based on documentation
    HEADER_FIELDS = [
        HeaderField("numberBytes", "<I", 4),
        HeaderField("pageVersion", "<I", 4),
        HeaderField("configuration", "<I", 4),
        HeaderField("pingNumber", "<I", 4),
        HeaderField("numSamples", "<I", 4),
        HeaderField("beamsToDisplay", "<I", 4),
        HeaderField("errorFlags", "<I", 4),
        HeaderField("range", "<I", 4),
        HeaderField("speedFish", "<I", 4),
        HeaderField("speedSound", "<I", 4),
        HeaderField("resMode", "<I", 4),
        HeaderField("txWaveform", "<I", 4),
        HeaderField("respDiv", "<I", 4),
        HeaderField("respFreq", "<I", 4),
        HeaderField("manualSpeedSwitch", "<I", 4),
        HeaderField("despeckleSwitch", "<I", 4),
        HeaderField("speedFilterSwitch", "<I", 4),
        HeaderField("year", "<I", 4),
        HeaderField("month", "<I", 4),
        HeaderField("day", "<I", 4),
        HeaderField("hour", "<I", 4),
        HeaderField("minute", "<I", 4),
        HeaderField("second", "<I", 4),
        HeaderField("hSecond", "<I", 4),
        HeaderField("fixTimeHour", "<I", 4),
        HeaderField("fixTimeMinute", "<I", 4),
        HeaderField("fixTimeSecond", "<f", 4),
        HeaderField("heading", "<f", 4),
        HeaderField("pitch", "<f", 4),
        HeaderField("roll", "<f", 4),
        HeaderField("depth", "<f", 4),
        HeaderField("altitude", "<f", 4),
        HeaderField("temperature", "<f", 4),
        HeaderField("speed", "<f", 4),
        HeaderField("shipHeading", "<f", 4),
        HeaderField("magneticVariation", "<f", 4),
        HeaderField("shipLat", "<d", 8),
        HeaderField("shipLon", "<d", 8),
        HeaderField("fishLat", "<d", 8),
        HeaderField("fishLon", "<d", 8),
    ]

    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_header_field(self, file: BinaryIO, field: HeaderField) -> Any:
        """Read a single header field with proper error handling"""
        try:
            data = file.read(field.size)
            if len(data) != field.size:
                raise EOFError(f"Unexpected EOF while reading {field.name} (expected {field.size} bytes)")
            value = struct.unpack(field.format, data)[0]
            print(f"Read {field.name}: {value}")  # Debug print
            return value
        except struct.error as e:
            raise ValueError(f"Error reading {field.name}: {str(e)}")


    def read_sdf_header(self, file: BinaryIO) -> Dict[str, Any]:
        """Read the SDF header with improved error handling and validation"""
        header = {}
        
        # Read all header fields
        for field in self.HEADER_FIELDS:
            header[field.name] = self.read_header_field(file, field)

        # Validate critical fields
        if header["pageVersion"] not in PageVersion.__members__.values():
            raise ValueError(f"Invalid page version: {header['pageVersion']}")

        # Version specific header extensions
        if header["pageVersion"] >= 3000:  # Version 3 and above
            header["tvgPage"] = self.read_header_field(file, HeaderField("tvgPage", "<I", 4))
            header["headerSize"] = self.read_header_field(file, HeaderField("headerSize", "<I", 4))

        if header["pageVersion"] >= 3001:  # Version 4 and above
            header["sdfExtensionSize"] = self.read_header_field(file, 
                                                              HeaderField("sdfExtensionSize", "<I", 4))

        return header

    def read_channel_data(self, file: BinaryIO, page_version: int, config: int) -> Dict[str, Any]:
        """Read channel data based on page version and configuration"""
        channels = {}
        
        try:
            if page_version in [PageVersion.SYSTEM_3000, PageVersion.SYSTEM_3000_V4]:
                if config & 0x01:  # Low Frequency Port
                    channels["lf_port"] = self._read_channel_samples(file, "H")
                if config & 0x02:  # Low Frequency Starboard
                    channels["lf_starboard"] = self._read_channel_samples(file, "H")
                if config & 0x04:  # High Frequency Port
                    channels["hf_port"] = self._read_channel_samples(file, "H")
                if config & 0x08:  # High Frequency Starboard
                    channels["hf_starboard"] = self._read_channel_samples(file, "H")
                if config & 0x10:  # Sub Bottom Profiler
                    channels["sbp"] = self._read_channel_samples(file, "i")
            
            elif page_version in [PageVersion.SYSTEM_5000, PageVersion.SYSTEM_5000_V4]:
                for beam in range(10):
                    if config & (1 << beam):
                        channels[f"beam_{beam+1}"] = self._read_channel_samples(file, "H")

        except struct.error as e:
            raise ValueError(f"Error reading channel data: {str(e)}")
            
        return channels

    def _read_channel_samples(self, file: BinaryIO, data_type: str) -> List[Any]:
        num_samples = struct.unpack("<H", file.read(2))[0]
        if num_samples < 0:
            raise ValueError(f"Invalid number of samples: {num_samples}")
        print(f"Number of samples: {num_samples}")  # Debug print

        sample_size = struct.calcsize(data_type)
        expected_read_length = sample_size * num_samples
        if expected_read_length < 0:
            raise ValueError(f"Calculated read length is negative: {expected_read_length}")

        data = file.read(expected_read_length)
        if len(data) != expected_read_length:
            raise EOFError(f"Unexpected EOF: Expected {expected_read_length} bytes, got {len(data)} bytes")

        return list(struct.unpack(f"<{num_samples}{data_type}", data))


    def read_sdfx_extension(self, file: BinaryIO, size: int) -> List[Dict[str, Any]]:
        """Read SDFX extension data with improved structure handling"""
        extension_data = []
        bytes_read = 0

        while bytes_read < size:
            record = {}
            # Read record header
            record["recordId"] = self.read_header_field(file, HeaderField("recordId", "<I", 4))
            record["recordSize"] = self.read_header_field(file, HeaderField("recordSize", "<I", 4))
            record["headerVersion"] = self.read_header_field(file, HeaderField("headerVersion", "<I", 4))
            record["recordVersion"] = self.read_header_field(file, HeaderField("recordVersion", "<I", 4))

            # Read record data based on recordId
            data_size = record["recordSize"] - 16  # Subtract header size
            record["data"] = self._read_record_data(file, record["recordId"], data_size)
            
            extension_data.append(record)
            bytes_read += record["recordSize"]

            if record["recordId"] == 0xEEEEEEEE:  # End marker
                break

        return extension_data

    def _read_record_data(self, file: BinaryIO, record_id: int, size: int) -> Dict[str, Any]:
        """Read record data based on record ID"""
        data = {}
        
        # Handle different record types based on documentation
        if record_id == 0x00000001:  # Ship Configuration Info
            data["shipLength"] = struct.unpack("<f", file.read(4))[0]
            data["shipWidth"] = struct.unpack("<f", file.read(4))[0]
            # ... read other fields based on record type
        else:
            # For unknown record types, just read raw bytes
            data["raw"] = file.read(size)
            
        return data

    def read_file(self) -> List[Dict[str, Any]]:
        """Read entire SDF file with improved error handling and validation"""
        pings = []
        
        try:
            with open(self.file_path, "rb") as file:
                while True:
                    # Check for ping marker
                    marker = file.read(4)
                    if not marker or len(marker) != 4:
                        break
                        
                    if struct.unpack("<I", marker)[0] != 0xFFFFFFFF:
                        continue

                    ping = {}
                    ping["header"] = self.read_sdf_header(file)
                    ping["channels"] = self.read_channel_data(
                        file, 
                        ping["header"]["pageVersion"],
                        ping["header"]["configuration"]
                    )

                    if ping["header"].get("sdfExtensionSize", 0) > 0:
                        ping["extension"] = self.read_sdfx_extension(
                            file,
                            ping["header"]["sdfExtensionSize"]
                        )

                    pings.append(ping)

        except (IOError, struct.error) as e:
            raise RuntimeError(f"Error reading SDF file: {str(e)}")

        return pings

if __name__ == "__main__":
    try:
        reader = SonarDataReader("COMET-300_20240201130734.sdf")
        pings = reader.read_file()
        
        print(f"Successfully read {len(pings)} pings")
        for i, ping in enumerate(pings[:5]):  # Print first 5 pings
            print(f"\nPing {i+1}:")
            print(f"Page Version: {ping['header'].get('pageVersion')}")
            print(f"Number of Samples: {ping['header'].get('numSamples')}")
            print(f"Range: {ping['header'].get('range')} meters")
            print(f"Channels present: {list(ping['channels'].keys())}")
            if "extension" in ping:
                print(f"Extension records: {len(ping['extension'])}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
