"""Directory structure and paths."""

import platform
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
print(PROJECT_ROOT)

IS_WINDOWS = (platform.system() == 'Windows')
if IS_WINDOWS:
    UNPACK_RAR_EXE = str(PROJECT_ROOT / 'bin' / 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'    


