module;

#include <stdint.h>
#include <stddef.h>
#include <bit>
#include <span>

export module xxhash;

using namespace std;

constexpr uint64_t xxh_load_le64(const uint8_t* p) {
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
           ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) |
           ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) |
           ((uint64_t)p[7] << 56);
}

constexpr uint32_t xxh_load_le32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

constexpr uint64_t XXH64_PRIME1 = 0x9e3779b185ebca87;
constexpr uint64_t XXH64_PRIME2 = 0xc2b2ae3d27d4eb4f;
constexpr uint64_t XXH64_PRIME3 = 0x165667b19e3779f9;
constexpr uint64_t XXH64_PRIME4 = 0x85ebca77c2b2ae63;
constexpr uint64_t XXH64_PRIME5 = 0x27d4eb2f165667c5;

constexpr uint64_t xxh64_round(uint64_t acc, uint64_t input) {
    acc += input * XXH64_PRIME2;
    acc = rotl(acc, 31);
    acc *= XXH64_PRIME1;
    return acc;
}

constexpr uint64_t xxh64_merge_round(uint64_t acc, uint64_t val) {
    val = xxh64_round(0, val);
    acc ^= val;
    acc = acc * XXH64_PRIME1 + XXH64_PRIME4;
    return acc;
}

export constexpr uint64_t calc_xxhash64(uint64_t seed, span<const uint8_t> input) {
    auto p = input.data();
    auto len = input.size();
    size_t pos = 0;
    uint64_t h64;

    if (len >= 32) {
        uint64_t v1 = seed + XXH64_PRIME1 + XXH64_PRIME2;
        uint64_t v2 = seed + XXH64_PRIME2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - XXH64_PRIME1;

        do {
            v1 = xxh64_round(v1, xxh_load_le64(p + pos)); pos += 8;
            v2 = xxh64_round(v2, xxh_load_le64(p + pos)); pos += 8;
            v3 = xxh64_round(v3, xxh_load_le64(p + pos)); pos += 8;
            v4 = xxh64_round(v4, xxh_load_le64(p + pos)); pos += 8;
        } while (pos <= len - 32);

        h64 = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18);
        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = seed + XXH64_PRIME5;
    }

    h64 += (uint64_t)len;

    while (pos + 8 <= len) {
        uint64_t k = xxh_load_le64(p + pos);
        k *= XXH64_PRIME2;
        k = rotl(k, 31);
        k *= XXH64_PRIME1;
        h64 ^= k;
        h64 = rotl(h64, 27) * XXH64_PRIME1 + XXH64_PRIME4;
        pos += 8;
    }

    if (pos + 4 <= len) {
        uint64_t k = xxh_load_le32(p + pos);
        h64 ^= k * XXH64_PRIME1;
        h64 = rotl(h64, 23) * XXH64_PRIME2 + XXH64_PRIME3;
        pos += 4;
    }

    while (pos < len) {
        h64 ^= p[pos++] * XXH64_PRIME5;
        h64 = rotl(h64, 11) * XXH64_PRIME1;
    }

    h64 ^= h64 >> 33;
    h64 *= XXH64_PRIME2;
    h64 ^= h64 >> 29;
    h64 *= XXH64_PRIME3;
    h64 ^= h64 >> 32;

    return h64;
}

static constexpr auto test_xxhash64() {
    constexpr uint8_t input[] = {'a','b','c'};
    return calc_xxhash64(0, input);
}

static_assert(test_xxhash64() == 0x44bc2cf5ad770999);
