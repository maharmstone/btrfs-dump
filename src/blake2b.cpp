module;

#include <stdint.h>
#include <stddef.h>
#include <bit>
#include <array>
#include <span>
#include <ranges>
#include <algorithm>

export module blake2b;

using namespace std;

constexpr uint64_t b2b_load_le64(const uint8_t* p) {
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

constexpr void b2b_store_le64(uint8_t* p, uint64_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
    p[4] = (uint8_t)(v >> 32); p[5] = (uint8_t)(v >> 40); p[6] = (uint8_t)(v >> 48); p[7] = (uint8_t)(v >> 56);
}

constexpr uint64_t blake2b_IV[8] = {
    0x6a09e667f3bcc908, 0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
    0x510e527fade682d1, 0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b, 0x5be0cd19137e2179
};

constexpr uint8_t blake2b_sigma[12][16] = {
    { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15},
    {14, 10,  4,  8,  9, 15, 13,  6,  1, 12,  0,  2, 11,  7,  5,  3},
    {11,  8, 12,  0,  5,  2, 15, 13, 10, 14,  3,  6,  7,  1,  9,  4},
    { 7,  9,  3,  1, 13, 12, 11, 14,  2,  6,  5, 10,  4,  0, 15,  8},
    { 9,  0,  5,  7,  2,  4, 10, 15, 14,  1, 11, 12,  6,  8,  3, 13},
    { 2, 12,  6, 10,  0, 11,  8,  3,  4, 13,  7,  5, 15, 14,  1,  9},
    {12,  5,  1, 15, 14, 13,  4, 10,  0,  7,  6,  3,  9,  2,  8, 11},
    {13, 11,  7, 14, 12,  1,  3,  9,  5,  0, 15,  4,  8,  6,  2, 10},
    { 6, 15, 14,  9, 11,  3,  0,  8, 12,  2, 13,  7,  1,  4, 10,  5},
    {10,  2,  8,  4,  7,  6,  1,  5, 15, 11,  9, 14,  3, 12, 13,  0},
    { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15},
    {14, 10,  4,  8,  9, 15, 13,  6,  1, 12,  0,  2, 11,  7,  5,  3}
};

constexpr void blake2b_compress(span<uint64_t, 8> h, span<const uint8_t, 128> block,
                                uint64_t t0, uint64_t t1, bool last) {
    uint64_t m[16], v[16];

    for (unsigned int i = 0; i < 16; i++) {
        m[i] = b2b_load_le64(block.data() + (i * 8));
    }

    for (unsigned int i = 0; i < 8; i++) {
        v[i] = h[i];
    }

    v[8]  = blake2b_IV[0]; v[9]  = blake2b_IV[1];
    v[10] = blake2b_IV[2]; v[11] = blake2b_IV[3];
    v[12] = blake2b_IV[4] ^ t0;
    v[13] = blake2b_IV[5] ^ t1;
    v[14] = last ? ~blake2b_IV[6] : blake2b_IV[6];
    v[15] = blake2b_IV[7];

    for (unsigned int r = 0; r < 12; r++) {
        auto G = [&](unsigned int i, uint64_t& a, uint64_t& b, uint64_t& c, uint64_t& d) {
            a += b + m[blake2b_sigma[r][2 * i]];
            d = rotr(d ^ a, 32);
            c += d;
            b = rotr(b ^ c, 24);
            a += b + m[blake2b_sigma[r][(2 * i) + 1]];
            d = rotr(d ^ a, 16);
            c += d;
            b = rotr(b ^ c, 63);
        };

        G(0, v[0], v[4], v[8],  v[12]);
        G(1, v[1], v[5], v[9],  v[13]);
        G(2, v[2], v[6], v[10], v[14]);
        G(3, v[3], v[7], v[11], v[15]);
        G(4, v[0], v[5], v[10], v[15]);
        G(5, v[1], v[6], v[11], v[12]);
        G(6, v[2], v[7], v[8],  v[13]);
        G(7, v[3], v[4], v[9],  v[14]);
    }

    for (unsigned int i = 0; i < 8; i++) {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

export constexpr array<uint8_t, 32> calc_blake2b_256(span<const uint8_t> data) {
    // Initialize: h = IV xor parameter block
    // Parameter block: digest_length=32, key_length=0, fanout=1, depth=1
    array<uint64_t, 8> h;
    for (unsigned int i = 0; i < 8; i++) {
        h[i] = blake2b_IV[i];
    }
    h[0] ^= 0x01010020;

    uint64_t bytes_compressed = 0;
    auto p = data;
    auto remaining = data.size();

    while (remaining > 128) {
        bytes_compressed += 128;
        blake2b_compress(h, p.first<128>(), bytes_compressed, 0, false);
        p = p.subspan(128);
        remaining -= 128;
    }

    // Last block (padded with zeros)
    uint8_t last_block[128] = {};
    for (size_t i = 0; i < remaining; i++) {
        last_block[i] = p[i];
    }
    bytes_compressed += remaining;
    blake2b_compress(h, last_block, bytes_compressed, 0, true);

    array<uint8_t, 32> ret;

    // Output first 32 bytes in little-endian
    for (unsigned int i = 0; i < 4; i++) {
        b2b_store_le64(ret.data() + (i * 8), h[i]);
    }

    return ret;
}

static constexpr bool test_blake2b_256() {
    constexpr uint8_t input[] = {'a','b','c'};

    array<uint8_t, 32> v{
        0xbd, 0xdd, 0x81, 0x3c, 0x63, 0x42, 0x39, 0x72,
        0x31, 0x71, 0xef, 0x3f, 0xee, 0x98, 0x57, 0x9b,
        0x94, 0x96, 0x4e, 0x3b, 0xb1, 0xcb, 0x3e, 0x42,
        0x72, 0x62, 0xc8, 0xc0, 0x68, 0xd5, 0x23, 0x19
    };

    return ranges::equal(calc_blake2b_256(input), v);
}
static_assert(test_blake2b_256());
