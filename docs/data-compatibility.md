# Storage Data Compatibility

This document provides guidelines and warnings regarding
the portability of NeoFS storage data between different environments
and operating systems.

## Note on File Systems and Case-Sensitivity

The primary storage subsystem for a NeoFS Storage Node is `fstree`.
It saves objects as files inside a directory tree.

To build this structure efficiently, `fstree` uses **case-sensitive
Base58 encoded string representations** of Object and Container IDs
for both directory and file names.

**Warning: Data Incompatibility Across Operating Systems**

Because Base58 is case-sensitive (e.g., `A` and `a` represent
different values),`fstree` data is highly dependent on the
case-sensitivity policy of the underlying file system.

*   **Case-Sensitive File Systems:** (e.g., `ext4`, `xfs` on Linux,
or specially formatted APFS volumes). These distinguish between
a folder named `2A` and `2a`.
*   **Case-Insensitive File Systems:** (e.g., default `APFS` on macOS,
or `NTFS`/`FAT32` on Windows). These treat `2A` and `2a` as the same
path while preserving the original case.

### The Risk of Data Loss and Corruption

Moving, copying, or restoring an `fstree` data directory from a
Case-Sensitive file system to a default Case-Insensitive file
system can lead to severe issues:

1. **Data Collisions & Overwrites:** Distinct objects with IDs
that differ only in letter casing will be forced into the same
directory or file path, overwriting each other.
2. **Iterator:** Operations like `fstree list` reconstruct
object IDs directly from the directory paths. Because the OS only returns
the preserved case of the *first* created folder, the node will reconstruct
an incorrect Base58 ID and display it to the user.

Direct `Get` operations for a known, valid Object ID might still succeed
because the OS will successfully resolve the case-mismatched path, provided the
file itself wasn't overwritten by a collision.

**Recommendation:** Never migrate or share `fstree` raw data directories
between Linux production servers and default macOS/Windows environments
without ensuring the target volume is formatted as strictly Case-Sensitive.
