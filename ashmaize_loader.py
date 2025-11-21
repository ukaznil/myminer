"""
Automatic platform detection and loader for ashmaize_py native library.
Supports Windows, Linux, and macOS on both x64 and ARM64 architectures.
"""

import os
import platform
import sys


def get_platform_path():
    """
    Detect the current platform and architecture, return the path to the appropriate binary.

    Returns:
        str: Path to the platform-specific library directory

    Raises:
        RuntimeError: If platform is not supported or binary is missing
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Normalize architecture names
    if machine in ['x86_64', 'amd64', 'x64']:
        arch = 'x64'
    elif machine in ['aarch64', 'arm64', 'armv8']:
        arch = 'arm64'
    else:
        arch = machine

    # Map platform combinations to directory names
    platform_map = {
        ('windows', 'x64'): 'windows-x64',
        ('windows', 'arm64'): 'windows-x64',
        ('linux', 'x64'): 'linux-x64',
        ('linux', 'arm64'): 'linux-arm64',
        ('darwin', 'x64'): 'macos-x64',
        ('darwin', 'arm64'): 'macos-arm64',
        }

    platform_key = (system, arch)

    if platform_key not in platform_map:
        supported = '\n'.join([f"  - {s.capitalize().replace('darwin', 'macos')} {a}" for s, a in platform_map.keys()])
        raise RuntimeError(
            f"Unsupported platform: {system.capitalize()} {arch}\n"
            f"Supported platforms:\n{supported}\n\n"
            f"Please create an issue at https://github.com/djeanql/MidnightMiner/issues "
            f"with your OS version and architecture ({machine}) for support."
            )

    platform_dir = platform_map[platform_key]
    # When in miner/ subdirectory, go up one level to find libs/
    # base_dir = os.path.dirname(os.path.dirname(__file__))
    base_dir = os.path.dirname(__file__)
    lib_path = os.path.join(base_dir, 'libs', platform_dir)

    # Check if the binary exists
    if system == 'windows':
        binary_name = 'ashmaize_py.pyd'
    else:
        binary_name = 'ashmaize_py.so'

    binary_path = os.path.join(lib_path, binary_name)

    if not os.path.exists(binary_path):
        raise RuntimeError(
            f"Binary not found for {system.capitalize()} {arch}\n"
            f"Expected location: {binary_path}\n\n"
            f"The binary may not be available yet for your platform. "
            f"Please create an issue at https://github.com/djeanql/MidnightMiner/issues"
            )

    return lib_path


def load_ashmaize():
    """
    Load the ashmaize_py module for the current platform.

    Returns:
        module: The loaded ashmaize_py module

    Raises:
        RuntimeError: If platform is not supported or binary cannot be loaded
    """
    lib_path = get_platform_path()

    # Add the library path to sys.path temporarily
    if lib_path not in sys.path:
        sys.path.insert(0, lib_path)

    try:
        import ashmaize_py
        return ashmaize_py
    except ImportError as e:
        raise RuntimeError(
            f"Failed to load ashmaize_py from {lib_path}\n"
            f"Error: {e}\n\n"
            f"Please create an issue at https://github.com/midnight-network/MidnightMiner/issues "
            f"with details about your system."
            ) from e


# Convenience: Allow importing ashmaize_py directly from this module
ashmaize_py = None


def init():
    """Initialize and return the ashmaize_py module."""
    global ashmaize_py
    if ashmaize_py is None:
        ashmaize_py = load_ashmaize()
    return ashmaize_py
