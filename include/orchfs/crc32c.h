#ifndef ORCHFS_CRC32C_H
#define ORCHFS_CRC32C_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#if defined(__x86_64__)
#include <immintrin.h>
#endif

/*
 * CRC32C (Castagnoli, reflected, polynomial 0x82f63b78) over the on-media
 * format. The formatter, the write-ahead log, and recovery tooling must all
 * agree bit-for-bit, so the computation lives in one header shared by every
 * consumer. The software and hardware variants produce identical values, so a
 * volume formatted on one host validates on any other.
 */

static inline uint32_t orchfs_crc32c_software(const void* bytes, size_t length)
{
    const uint8_t* input = (const uint8_t*)bytes;
    uint32_t crc = UINT32_MAX;
    while(length-- != 0)
    {
        crc ^= *input++;
        for(int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (UINT32_C(0x82f63b78) &
                    (uint32_t)-(int32_t)(crc & 1U));
    }
    return ~crc;
}

#if defined(__x86_64__)
__attribute__((target("sse4.2")))
static inline uint32_t orchfs_crc32c_hardware(const void* bytes, size_t length)
{
    const uint8_t* input = (const uint8_t*)bytes;
    uint64_t crc = UINT32_MAX;
    while(length >= sizeof(uint64_t))
    {
        uint64_t word;
        memcpy(&word, input, sizeof(word));
        crc = _mm_crc32_u64(crc, word);
        input += sizeof(word);
        length -= sizeof(word);
    }
    while(length-- != 0)
        crc = _mm_crc32_u8((uint32_t)crc, *input++);
    return ~(uint32_t)crc;
}
#endif

static inline uint32_t orchfs_crc32c(const void* bytes, size_t length)
{
#if defined(__x86_64__)
    if(__builtin_cpu_supports("sse4.2"))
        return orchfs_crc32c_hardware(bytes, length);
#endif
    return orchfs_crc32c_software(bytes, length);
}

#endif
