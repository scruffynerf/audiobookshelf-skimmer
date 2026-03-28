import subprocess
import tempfile
from pathlib import Path
import logging
import static_ffmpeg

# Ensure ffmpeg is available (installs a local copy if missing)
static_ffmpeg.add_paths(weak=True)

logger = logging.getLogger(__name__)

def slice_audio(input_source: str, duration_sec: int = 120, output_file: Path = None, headers: dict = None) -> Path:
    """
    Extracts the first N seconds of an audio source (file or URL) using ffmpeg.
    If output_file is not provided, a temporary file is created.
    """
    if not output_file:
        # Use a dedicated tmp directory (ignored by git)
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
    
    # Prepare ffmpeg command
    cmd = ["ffmpeg", "-y", "-v", "error"]
    
    # Add headers if provided (must come before -i)
    if headers:
        header_str = "\r\n".join([f"{k}: {v}" for k, v in headers.items()]) + "\r\n"
        cmd.extend(["-headers", header_str])
        
    cmd.extend([
        "-i", str(input_source),
        "-t", str(duration_sec),
        "-ar", "16000", # 16kHz
        "-ac", "1",     # Mono
        str(output_file)
    ])

    try:
        subprocess.run(cmd, check=True)
        return output_file
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to slice audio: {e}")
        # If copy fails (e.g. codec issues with short duration), try re-encoding to a simple format
        try:
            logger.info("Retrying with re-encoding to wav...")
            output_file_wav = output_file.with_suffix(".wav")
            # Create a new command for re-encoding
            re_cmd = ["ffmpeg", "-y", "-v", "error"]
            if headers:
                re_cmd.extend(["-headers", header_str])
            re_cmd.extend([
                "-i", str(input_source),
                "-t", str(duration_sec),
                str(output_file_wav)
            ])
            subprocess.run(re_cmd, check=True)
            return output_file_wav
        except subprocess.CalledProcessError as e2:
            logger.error(f"Failed to slice audio even with re-encoding: {e2}")
            raise e2
