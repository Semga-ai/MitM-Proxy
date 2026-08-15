# Proxy based on MitM topology

Operates using the **Minecraft protocol**. Includes **defragmentation**, **full header parsing** (ID, uncompressed length), **packet decompression**, and **state checking and transitions**.
The average processing time for an uncompressed packet is approximately **40 nanoseconds**. Tests were conducted on an **i3-9100F** by calculating the arithmetic mean of the processing times for **5_000** packets.
Time measurements were taken per packet using Instant::now() from minstant. The tests were conducted under combat conditions on a live game server.

**The `target-cpu=native` flag was used.**
**Note that the measurements did not include the time required to parse the full length for defragmentation or the defragmentation process itself.**
