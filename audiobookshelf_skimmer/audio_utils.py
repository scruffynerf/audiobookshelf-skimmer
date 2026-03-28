import logging

import static_ffmpeg
from pathlib import Path

# Ensure ffmpeg is available
static_ffmpeg.add_paths(weak=True)

logger = logging.getLogger(__name__)

import subprocess

def slice_audio(input_source: str, duration_sec: int = 30, output_file: Path = None, headers: dict = None) -> Path:
    """
    Extracts the first N seconds of an audio source (file or URL) using native subprocess.
    """
    if not output_file:
        tmp_dir = Path.cwd() / "tmp"
        tmp_dir.mkdir(exist_ok=True)
        
        # Parakeet-MLX prefers 16kHz Mono WAV
        suffix = ".wav" 
        if not isinstance(input_source, Path) and "://" in str(input_source):
             stem = "stream_slice"
        else:
             p = Path(input_source)
             stem = p.stem
             
        output_file = tmp_dir / f"snippet_{stem}{suffix}"

    logger.info(f"Slicing first {duration_sec}s of {input_source} to {output_file.name} (16kHz Mono WAV)")
    
    # We use naked subprocess because ffmpegio's pipe handling deadlocks python
    # when ffmpeg fails fast on 404 chunks in HLS streams.
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error", "-nostdin",
        "-probesize", "1000000", "-analyzeduration", "1000000",
        "-fflags", "+nobuffer", "-flags", "+low_delay",
        "-reconnect", "1", "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-timeout", "5000000", "-rw_timeout", "5000000"
    ]
    
    if headers:
        header_str = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
        cmd.extend(["-headers", header_str])
        
    cmd.extend([
        "-i", str(input_source),
        "-t", str(duration_sec),
        "-ar", "16000",
        "-ac", "1",
        str(output_file)
    ])

    try:
        # Hard kill after 60s max to prevent python from EVER hanging indefinitely
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)
        
        # Explicit check that the file was actually created and isn't empty
        if not output_file.exists() or output_file.stat().st_size == 0:
            raise RuntimeError(f"ffmpeg failed to create output file: {output_file}")
            
        return output_file
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timed out completely - hard killed after 60s")
        raise RuntimeError("ffmpeg execution timed out")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to slice audio: {e.stderr}")
        raise RuntimeError(f"ffmpeg failed with code {e.returncode}: {e.stderr}")

