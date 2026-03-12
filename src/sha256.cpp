module;

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <bit>
#include <span>
#include <array>
#include <ranges>
#include <algorithm>

#if defined(__SHA__) && defined(__SSE4_1__) && defined(__SSSE3__)
#include <immintrin.h>
#elif (defined(__ARM_FEATURE_SHA2) || defined(__ARM_FEATURE_CRYPTO)) && defined(__aarch64__)
#include <arm_neon.h>
#endif

export module sha256;

using namespace std;

constexpr uint32_t K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b,
    0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01,
    0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7,
    0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152,
    0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
    0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08,
    0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f,
    0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
};

constexpr void sha256_process_block(span<uint32_t, 8> h, span<const uint8_t, 64> block) {
    uint32_t W[64];

    for (unsigned int t = 0; t < 16; t++) {
        W[t] = ((uint32_t)block[t*4] << 24) | ((uint32_t)block[t*4+1] << 16) |
               ((uint32_t)block[t*4+2] << 8) | ((uint32_t)block[t*4+3]);
    }

    for (unsigned int t = 16; t < 64; t++) {
        W[t] = (rotr(W[t-2], 17) ^ rotr(W[t-2], 19) ^ (W[t-2] >> 10)) + W[t-7] +
               (rotr(W[t-15], 7) ^ rotr(W[t-15], 18) ^ (W[t-15] >> 3)) + W[t-16];
    }

    uint32_t a = h[0], b = h[1], c = h[2], d = h[3];
    uint32_t e = h[4], f = h[5], g = h[6], hh = h[7];

    for (unsigned int t = 0; t < 64; t++) {
        uint32_t T1 = hh + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) +
                      ((e & f) ^ (~e & g)) + K[t] + W[t];
        uint32_t T2 = (rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) +
                      ((a & b) ^ (a & c) ^ (b & c));
        hh = g; g = f; f = e; e = d + T1;
        d = c; c = b; b = a; a = T1 + T2;
    }

    h[0] += a; h[1] += b; h[2] += c; h[3] += d;
    h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
}

#if defined(__SHA__) && defined(__SSE4_1__) && defined(__SSSE3__)
void sha256_process_block_x86(span<uint32_t, 8> h, span<const uint8_t, 64> block) {
    const __m128i MASK = _mm_set_epi8(12, 13, 14, 15, 8, 9, 10, 11,
                                      4, 5, 6, 7, 0, 1, 2, 3);

    __m128i STATE0, STATE1;
    __m128i MSG, TMP;
    __m128i MSG0, MSG1, MSG2, MSG3;
    __m128i ABEF_SAVE, CDGH_SAVE;

    // Load initial state: h[0..3] = ABCD, h[4..7] = EFGH
    TMP    = _mm_loadu_si128((const __m128i*)h.data());
    STATE1 = _mm_loadu_si128((const __m128i*)(h.data() + 4));

    // Rearrange to SHA-NI layout: STATE0 = ABEF, STATE1 = CDGH
    TMP    = _mm_shuffle_epi32(TMP, 0xB1);    // BADC
    STATE1 = _mm_shuffle_epi32(STATE1, 0x1B);  // HGFE
    STATE0 = _mm_alignr_epi8(TMP, STATE1, 8); // ABEF
    STATE1 = _mm_blend_epi16(STATE1, TMP, 0xF0); // CDGH

    ABEF_SAVE = STATE0;
    CDGH_SAVE = STATE1;

    // Rounds 0-3
    MSG0 = _mm_loadu_si128((const __m128i*)block.data());
    MSG0 = _mm_shuffle_epi8(MSG0, MASK);
    MSG = _mm_add_epi32(MSG0, _mm_set_epi32((int)K[3], (int)K[2], (int)K[1], (int)K[0]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);

    // Rounds 4-7
    MSG1 = _mm_loadu_si128((const __m128i*)(block.data() + 16));
    MSG1 = _mm_shuffle_epi8(MSG1, MASK);
    MSG = _mm_add_epi32(MSG1, _mm_set_epi32((int)K[7], (int)K[6], (int)K[5], (int)K[4]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG0 = _mm_sha256msg1_epu32(MSG0, MSG1);

    // Rounds 8-11
    MSG2 = _mm_loadu_si128((const __m128i*)(block.data() + 32));
    MSG2 = _mm_shuffle_epi8(MSG2, MASK);
    MSG = _mm_add_epi32(MSG2, _mm_set_epi32((int)K[11], (int)K[10], (int)K[9], (int)K[8]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG1 = _mm_sha256msg1_epu32(MSG1, MSG2);

    // Rounds 12-15
    MSG3 = _mm_loadu_si128((const __m128i*)(block.data() + 48));
    MSG3 = _mm_shuffle_epi8(MSG3, MASK);
    MSG = _mm_add_epi32(MSG3, _mm_set_epi32((int)K[15], (int)K[14], (int)K[13], (int)K[12]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG3, MSG2, 4);
    MSG0 = _mm_add_epi32(MSG0, TMP);
    MSG0 = _mm_sha256msg2_epu32(MSG0, MSG3);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG2 = _mm_sha256msg1_epu32(MSG2, MSG3);

    // Rounds 16-19
    MSG = _mm_add_epi32(MSG0, _mm_set_epi32((int)K[19], (int)K[18], (int)K[17], (int)K[16]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG0, MSG3, 4);
    MSG1 = _mm_add_epi32(MSG1, TMP);
    MSG1 = _mm_sha256msg2_epu32(MSG1, MSG0);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG3 = _mm_sha256msg1_epu32(MSG3, MSG0);

    // Rounds 20-23
    MSG = _mm_add_epi32(MSG1, _mm_set_epi32((int)K[23], (int)K[22], (int)K[21], (int)K[20]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG1, MSG0, 4);
    MSG2 = _mm_add_epi32(MSG2, TMP);
    MSG2 = _mm_sha256msg2_epu32(MSG2, MSG1);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG0 = _mm_sha256msg1_epu32(MSG0, MSG1);

    // Rounds 24-27
    MSG = _mm_add_epi32(MSG2, _mm_set_epi32((int)K[27], (int)K[26], (int)K[25], (int)K[24]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG2, MSG1, 4);
    MSG3 = _mm_add_epi32(MSG3, TMP);
    MSG3 = _mm_sha256msg2_epu32(MSG3, MSG2);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG1 = _mm_sha256msg1_epu32(MSG1, MSG2);

    // Rounds 28-31
    MSG = _mm_add_epi32(MSG3, _mm_set_epi32((int)K[31], (int)K[30], (int)K[29], (int)K[28]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG3, MSG2, 4);
    MSG0 = _mm_add_epi32(MSG0, TMP);
    MSG0 = _mm_sha256msg2_epu32(MSG0, MSG3);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG2 = _mm_sha256msg1_epu32(MSG2, MSG3);

    // Rounds 32-35
    MSG = _mm_add_epi32(MSG0, _mm_set_epi32((int)K[35], (int)K[34], (int)K[33], (int)K[32]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG0, MSG3, 4);
    MSG1 = _mm_add_epi32(MSG1, TMP);
    MSG1 = _mm_sha256msg2_epu32(MSG1, MSG0);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG3 = _mm_sha256msg1_epu32(MSG3, MSG0);

    // Rounds 36-39
    MSG = _mm_add_epi32(MSG1, _mm_set_epi32((int)K[39], (int)K[38], (int)K[37], (int)K[36]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG1, MSG0, 4);
    MSG2 = _mm_add_epi32(MSG2, TMP);
    MSG2 = _mm_sha256msg2_epu32(MSG2, MSG1);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG0 = _mm_sha256msg1_epu32(MSG0, MSG1);

    // Rounds 40-43
    MSG = _mm_add_epi32(MSG2, _mm_set_epi32((int)K[43], (int)K[42], (int)K[41], (int)K[40]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG2, MSG1, 4);
    MSG3 = _mm_add_epi32(MSG3, TMP);
    MSG3 = _mm_sha256msg2_epu32(MSG3, MSG2);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG1 = _mm_sha256msg1_epu32(MSG1, MSG2);

    // Rounds 44-47
    MSG = _mm_add_epi32(MSG3, _mm_set_epi32((int)K[47], (int)K[46], (int)K[45], (int)K[44]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG3, MSG2, 4);
    MSG0 = _mm_add_epi32(MSG0, TMP);
    MSG0 = _mm_sha256msg2_epu32(MSG0, MSG3);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG2 = _mm_sha256msg1_epu32(MSG2, MSG3);

    // Rounds 48-51
    MSG = _mm_add_epi32(MSG0, _mm_set_epi32((int)K[51], (int)K[50], (int)K[49], (int)K[48]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG0, MSG3, 4);
    MSG1 = _mm_add_epi32(MSG1, TMP);
    MSG1 = _mm_sha256msg2_epu32(MSG1, MSG0);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);
    MSG3 = _mm_sha256msg1_epu32(MSG3, MSG0);

    // Rounds 52-55
    MSG = _mm_add_epi32(MSG1, _mm_set_epi32((int)K[55], (int)K[54], (int)K[53], (int)K[52]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG1, MSG0, 4);
    MSG2 = _mm_add_epi32(MSG2, TMP);
    MSG2 = _mm_sha256msg2_epu32(MSG2, MSG1);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);

    // Rounds 56-59
    MSG = _mm_add_epi32(MSG2, _mm_set_epi32((int)K[59], (int)K[58], (int)K[57], (int)K[56]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    TMP = _mm_alignr_epi8(MSG2, MSG1, 4);
    MSG3 = _mm_add_epi32(MSG3, TMP);
    MSG3 = _mm_sha256msg2_epu32(MSG3, MSG2);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);

    // Rounds 60-63
    MSG = _mm_add_epi32(MSG3, _mm_set_epi32((int)K[63], (int)K[62], (int)K[61], (int)K[60]));
    STATE1 = _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);
    MSG = _mm_shuffle_epi32(MSG, 0x0E);
    STATE0 = _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);

    // Add saved state
    STATE0 = _mm_add_epi32(STATE0, ABEF_SAVE);
    STATE1 = _mm_add_epi32(STATE1, CDGH_SAVE);

    // Convert back from ABEF/CDGH to ABCD/EFGH
    TMP    = _mm_shuffle_epi32(STATE0, 0x1B);  // FEBA
    STATE1 = _mm_shuffle_epi32(STATE1, 0xB1);  // GHCD
    STATE0 = _mm_blend_epi16(TMP, STATE1, 0xF0); // ABCD
    STATE1 = _mm_alignr_epi8(STATE1, TMP, 8);    // EFGH

    _mm_storeu_si128((__m128i*)h.data(), STATE0);
    _mm_storeu_si128((__m128i*)(h.data() + 4), STATE1);
}
#endif

#if (defined(__ARM_FEATURE_SHA2) || defined(__ARM_FEATURE_CRYPTO)) && defined(__aarch64__)
void sha256_process_block_arm(span<uint32_t, 8> h, span<const uint8_t, 64> block) {
    uint32x4_t STATE0, STATE1;
    uint32x4_t MSG0, MSG1, MSG2, MSG3;
    uint32x4_t TMP0, TMP1;
    uint32x4_t STATE0_SAVE, STATE1_SAVE;

    STATE0 = vld1q_u32(h.data());
    STATE1 = vld1q_u32(h.data() + 4);

    STATE0_SAVE = STATE0;
    STATE1_SAVE = STATE1;

    // Load and byte-swap message words
    MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.data())));
    MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.data() + 16)));
    MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.data() + 32)));
    MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.data() + 48)));

    // Rounds 0-3
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG0 = vsha256su0q_u32(MSG0, MSG1);

    // Rounds 4-7
    TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    // Rounds 8-11
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    // Rounds 12-15
    TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    // Rounds 16-19
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG0 = vsha256su0q_u32(MSG0, MSG1);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    // Rounds 20-23
    TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    // Rounds 24-27
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    // Rounds 28-31
    TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    // Rounds 32-35
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG0 = vsha256su0q_u32(MSG0, MSG1);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    // Rounds 36-39
    TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG1 = vsha256su0q_u32(MSG1, MSG2);
    MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

    // Rounds 40-43
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG2 = vsha256su0q_u32(MSG2, MSG3);
    MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

    // Rounds 44-47
    TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG3 = vsha256su0q_u32(MSG3, MSG0);
    MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

    // Rounds 48-51
    TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);
    MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

    // Rounds 52-55
    TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);

    // Rounds 56-59
    TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);

    // Rounds 60-63
    TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
    TMP1 = STATE0;
    STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
    STATE1 = vsha256h2q_u32(STATE1, TMP1, TMP0);

    // Add saved state
    STATE0 = vaddq_u32(STATE0, STATE0_SAVE);
    STATE1 = vaddq_u32(STATE1, STATE1_SAVE);

    vst1q_u32(h.data(), STATE0);
    vst1q_u32(h.data() + 4, STATE1);
}
#endif

constexpr void sha256_do_block(span<uint32_t, 8> h, span<const uint8_t, 64> block) {
    if consteval {
        sha256_process_block(h, block);
    } else {
#if defined(__SHA__) && defined(__SSE4_1__) && defined(__SSSE3__)
        sha256_process_block_x86(h, block);
#elif (defined(__ARM_FEATURE_SHA2) || defined(__ARM_FEATURE_CRYPTO)) && defined(__aarch64__)
        sha256_process_block_arm(h, block);
#else
        sha256_process_block(h, block);
#endif
    }
}

export constexpr array<uint8_t, 32> calc_sha256(span<const uint8_t> data) {
    array<uint32_t, 8> h = {
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
    };

    auto len = data.size();
    uint64_t bit_len = (uint64_t)len * 8;
    size_t full_blocks = len / 64;

    for (size_t i = 0; i < full_blocks; i++) {
        sha256_do_block(h, data.subspan(i * 64).first<64>());
    }

    // Last block(s) with padding - at most 2 blocks needed
    array<uint8_t, 128> last = {};
    size_t remaining = len % 64;

    for (size_t i = 0; i < remaining; i++) {
        last[i] = data[(full_blocks * 64) + i];
    }
    last[remaining] = 0x80;

    size_t pad_blocks = remaining < 56 ? 1 : 2;
    size_t len_offset = (pad_blocks * 64) - 8;

    for (unsigned int i = 0; i < 8; i++) {
        last[len_offset + i] = (uint8_t)(bit_len >> (56 - (i * 8)));
    }

    sha256_do_block(h, span(last).first<64>());

    if (pad_blocks == 2)
        sha256_do_block(h, span(last).subspan(64).first<64>());

    array<uint8_t, 32> ret;

    for (unsigned int i = 0; i < 8; i++) {
        ret[i * 4] = (uint8_t)(h[i] >> 24);
        ret[(i * 4) + 1] = (uint8_t)(h[i] >> 16);
        ret[(i * 4) + 2] = (uint8_t)(h[i] >> 8);
        ret[(i * 4) + 3] = (uint8_t)h[i];
    }

    return ret;
}

static constexpr bool test_sha256() {
    constexpr uint8_t input[] = {'a','b','c'};

    array<uint8_t, 32> v{
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea,
        0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
        0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
        0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad
    };

    return ranges::equal(calc_sha256(input), v);
}
static_assert(test_sha256());
