import struct
from typing import BinaryIO, Dict, List, Any

class SonarDataReader:
    HEADER_FIELDS = [
        ("numberBytes", "<I", 4),
        ("pageVersion", "<I", 4),
        ("configuration", "<I", 4),
        ("pingNumber", "<I", 4),
        ("numSamples", "<I", 4),
        ("errorFlags", "<I", 4),
        ("range", "<I", 4),
        ("speedFish", "<I", 4),
        ("speedSound", "<I", 4),
        ("txWaveform", "<I", 4),
        ("respDiv", "<I", 4),
        ("respFreq", "<I", 4),
        ("manualSpeedSwitch", "<I", 4),
        ("year", "<I", 4),
        ("month", "<I", 4),
        ("day", "<I", 4),
        ("hour", "<I", 4),
        ("minute", "<I", 4),
        ("second", "<I", 4),
        ("hSecond", "<I", 4),
    ]

    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_header_field(self, file: BinaryIO, field: tuple) -> Any:
        name, format_str, size = field
        data = file.read(size)
        return struct.unpack(format_str, data)[0]

    def read_sdf_header(self, file: BinaryIO) -> Dict[str, Any]:
        header = {}
        for field in self.HEADER_FIELDS:
            header[field[0]] = self.read_header_field(file, field)
        return header

    def read_channel_data(self, file: BinaryIO, num_samples: int) -> Dict[str, List[int]]:
        channels = {}
        for channel_name in ["portHf", "stbdHf"]:
            num_bytes = num_samples * 4  
            data = file.read(num_bytes)
            channels[channel_name] = list(struct.unpack(f"<{num_samples}I", data))
        return channels

    def interpret_tx_waveform(self, tx_waveform: int) -> Dict[str, Any]:
        lf_waveform = tx_waveform & 0x0F
        hf_waveform = (tx_waveform >> 8) & 0x0F
        lf_enabled = bool(tx_waveform & 0x80)
        hf_enabled = bool(tx_waveform & 0x8000)
        return {
            "lf_waveform": lf_waveform,
            "hf_waveform": hf_waveform,
            "lf_enabled": lf_enabled,
            "hf_enabled": hf_enabled
        }

    def read_file(self) -> List[Dict[str, Any]]:
        pings = []
        with open(self.file_path, "rb") as file:
            while True:
                marker = file.read(4)
                if not marker or len(marker) != 4:
                    break
                if struct.unpack("<I", marker)[0] != 0xFFFFFFFF:
                    continue

                ping = {}
                ping["header"] = self.read_sdf_header(file)
                
                if ping["header"]["pageVersion"] == 3502:
                    ping["channels"] = self.read_channel_data(file, ping["header"]["numSamples"])
                    ping["tx_waveform"] = self.interpret_tx_waveform(ping["header"]["txWaveform"])
                    pings.append(ping)
                else:
                    print(f"Skipping unsupported page version: {ping['header']['pageVersion']}")

        return pings

if __name__ == "__main__":
    reader = SonarDataReader("COMET-300_20240201130734.sdf")
    pings = reader.read_file()

    print(f"Successfully read {len(pings)} pings\n")
    for i, ping in enumerate(pings[:5]):  # Print first 5 pings
        print(f"--- Ping {i + 1} ---")
        header = ping['header']
        print(f"Page Version: {header['pageVersion']}")
        print(f"Number of Bytes: {header['numberBytes']}")
        print(f"Ping Number: {header['pingNumber']}")
        print(f"Number of Samples: {header['numSamples']}")
        print(f"Range: {header['range']} meters")
        print(f"Speed of Sound: {header['speedSound'] } cm/s")  
        
        tx_waveform = ping['tx_waveform']
        print(f"Transmit Waveform:")
        print(f"  Low Frequency: {tx_waveform['lf_waveform']} (Enabled: {tx_waveform['lf_enabled']})")
        print(f"  High Frequency: {tx_waveform['hf_waveform']} (Enabled: {tx_waveform['hf_enabled']})")
        
        print(f"Date: {header['year']}-{header['month']:02d}-{header['day']:02d}")
        print(f"Time: {header['hour']:02d}:{header['minute']:02d}:{header['second']:02d}.{header['hSecond']:02d}")
        
        print("Channels:")
        for channel_name, channel_data in ping['channels'].items():
            print(f"  - {channel_name}: {len(channel_data)} samples")
            print(f"    First 5 samples: {channel_data[:5]}")
            print(f"    Last 5 samples: {channel_data[-5:]}")
        print("\n")