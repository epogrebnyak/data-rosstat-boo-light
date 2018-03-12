import subprocess
from config import UNPACK_RAR_EXE


def unrar(rar_file_path: str, folder: str):
    job = [UNPACK_RAR_EXE, 'e', rar_file_path, folder, '-y']            
    subprocess.check_call(job)
    

def rar_content(rar_file_path: str):
    """Return single filename stored in RAR archive."""
    job = [UNPACK_RAR_EXE, 'lb', rar_file_path]
    return subprocess.check_output(job).decode("utf-8").strip()
    