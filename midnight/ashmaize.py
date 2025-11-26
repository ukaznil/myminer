from typing import Protocol

"""
GitHub reference implementation:
https://github.com/djeanql/ashmaize-py
"""


class PyAshMaize(Protocol):
    def build_rom(self, key: str, size: int) -> 'PyRom':
        """
        Build a ROM using FullRandom generation.

        Args:
            key: Secret key for ROM generation
            size: ROM size in bytes (default: 1GB)

        Returns:
            PyRom

        """
        ...
    # enddef

    def build_rom_twostep(self, key: str, size: int = 1073741824, pre_size=16777216, mixing_numbers: int = 4) -> 'PyRom':
        """
        Build a ROM using TwoStep generation (faster).

        Args:
            key: Secret key for ROM generation
            size: ROM size in bytes (default: 1GB)
            pre_size: Pre-ROM size (default: 16MB)
            mixing_numbers: Mixing iterations (default: 4)

        Returns:
            PyRom

        """


class PyRom(Protocol):
    def hash(self, preimage: str) -> str:
        """
        Hash a single preimage with default parameters (8 loops, 256 instructions).

        """
        ...

    def hash_with_params(self, preimage: str, nb_loops: int, nb_instrs: int) -> str:
        """
        Hash with custom parameters.

        """
        ...

    def hash_batch(self, preimages: list[str]) -> list[str]:
        """
        Hash multiple preimages efficiently (recommended for bulk operations).

        """
        ...

    def hash_batch_with_params(self, preimages: list[str], nb_loops: int, nb_instrs: int) -> list[str]:
        """
        Batch hash with custom parameters.

        """
        ...
