// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Stopwatch.h>

#include <iomanip>
#include <iostream>
#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>
#include <unordered_map>
#include <vector>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <Core/Types.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>

using Key = UInt64;
using Value = UInt64;


/// Various hash functions to test

namespace Hashes
{
struct IdentityHash
{
    size_t operator()(Key x) const { return x; }
};

/// Actually this is even worse than IdentityHash.
struct SimpleMultiplyHash
{
    size_t operator()(Key x) const { return x * 0xff51afd7ed558ccdULL; }
};

struct MultiplyAndMixHash
{
    size_t operator()(Key x) const
    {
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        return x;
    }
};

struct MixMultiplyMixHash
{
    size_t operator()(Key x) const
    {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        return x;
    }
};

struct MurMurMixHash
{
    size_t operator()(Key x) const
    {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }
};

/// Pretty bad, only for illustration purposes.
struct MixAllBitsHash
{
    size_t operator()(Key x) const
    {
        x ^= x >> 1;
        x ^= x >> 2;
        x ^= x >> 4;
        x ^= x >> 8;
        x ^= x >> 16;
        x ^= x >> 32;
        return x;
    }
};

struct IntHash32
{
    size_t operator()(Key x) const
    {
        x = (~x) + (x << 18);
        x = x ^ ((x >> 31) | (x << 33));
        x = x * 21;
        x = x ^ ((x >> 11) | (x << 53));
        x = x + (x << 6);
        x = x ^ ((x >> 22) | (x << 42));

        return x;
    }
};

struct ArcadiaNumericHash
{
    size_t operator()(Key x) const
    {
        x += ~(x << 32);
        x ^= (x >> 22);
        x += ~(x << 13);
        x ^= (x >> 8);
        x += (x << 3);
        x ^= (x >> 15);
        x += ~(x << 27);
        x ^= (x >> 31);

        return x;
    }
};

struct MurMurButDifferentHash
{
    size_t operator()(Key x) const
    {
        x ^= x >> 23;
        x *= 0x2127599bf4325c37ULL;
        x ^= x >> 47;
        x *= 0xb492b66fbe98f273ULL;
        x ^= x >> 33;
        return x;
    }
};

struct TwoRoundsTwoVarsHash
{
    size_t operator()(Key x) const
    {
        UInt64 a = x;
        UInt64 b = x;

        a ^= a >> 23;
        b ^= b >> 15;

        a *= 0x2127599bf4325c37ULL;
        b *= 0xb492b66fbe98f273ULL;

        a ^= a >> 47;
        b ^= b >> 33;

        return a ^ b;
    }
};

struct TwoRoundsLessOpsHash
{
    size_t operator()(Key x) const
    {
        x *= 0xb492b66fbe98f273ULL;
        x ^= x >> 23;
        x *= 0x2127599bf4325c37ULL;
        x ^= x >> 47;

        return x;
    }
};

#if __SSE4_2__
#include <nmmintrin.h>
#endif

struct CRC32Hash
{
    size_t operator()(Key x) const
    {
#if __SSE4_2__
        return _mm_crc32_u64(-1ULL, x);
#else
        /// On other platforms we do not have CRC32. NOTE This can be confusing.
        return intHash64(x);
#endif
    }
};

struct CityHash
{
    size_t operator()(Key x) const
    {
        return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&x), sizeof(x));
    }
};

struct SipHash
{
    size_t operator()(Key x) const
    {
        ::SipHash hash;
        hash.update(x);
        return hash.get64();
    }
};

struct MulShiftHash
{
    size_t operator()(Key x) const
    {
        static UInt64 random[2][256]
            = {{
                   0xb9979dc11a7ab921, 0x069177f0cca1cb81, 0xa77b7458984cdca6, 0x20cdddcd60ebf956, 0x54a7e16ccc85c618,
                   0x5a7b32512add86d0, 0xd4024d92207929e0, 0x927506e044a87bcb, 0x0ee2fafa66d27f8d, 0x34e5d597062edc13,
                   0x9d9e5971e4ef679f, 0x1373fa521c455462, 0xae20d7a035922d67, 0x2acceeaa1c38dea1, 0xa8d7318d831b2291,
                   0x237a092ca188a58d, 0xf3aab9c253e94340, 0x59c067c9247ad798, 0x9a2a3b0fc175faea, 0xb4ff42232b62408a,
                   0xc33f6f6e14b2ad59, 0xd8354b02517bb45e, 0x6cd3324afa8e1fa8, 0x491f3efd10beb8ce, 0xec5511355b29b707,
                   0x11a0e525b3f8a1d9, 0xaecb034c53727bb3, 0xaad2f1a0f7b1bb15, 0xdc318ef665bf079b, 0xecaed1b2e1808d2d,
                   0x38ccecd71d8548f5, 0x4a29ee777e1e8984, 0x58d683d104a9a9ac, 0x1c86c3ae03b0d968, 0x6ae480de1f434851,
                   0xdbb254502c123636, 0x7c8514ca7feb2780, 0x209ce2dacac250c4, 0xec256290581d4242, 0xba54af4e004cd657,
                   0xef09683b8da8cfd4, 0xef4b60e1cc22c00f, 0x8bb9849ecb4f0f4e, 0x64186f57b4e7e197, 0xf86d9122f253282c,
                   0xe545dcc26d8232fc, 0x7a8eeab5b064887e, 0xd77895d6c7baf87b, 0x07bf805fc0fcef52, 0x362d110a780e166b,
                   0x1b0a76b85b924a27, 0x49904133b1fb35c4, 0x76328bab42b9f5b8, 0xcfd15d94b28db6d0, 0x8908beb1031eb7b3,
                   0xbe05c1d38c789639, 0x9a38f06e2a4a5e2e, 0x9c49fa7b05cbd5e7, 0xf31aa060547fc13b, 0xafcfd8714b2a4082,
                   0x71a117f24d3d8cbe, 0x201a2c30737c1243, 0x9915450b0b26957e, 0xbe5865d67bd08830, 0x1efd55f709543d56,
                   0x69305e7e4c08948e, 0xd19d4fe1a47b1598, 0x36e4246123e542b7, 0x4cf0640cc2b66fdf, 0xf4301d669c78d4f9,
                   0xe1e7025ea57b3a3e, 0x6d59685c09510c07, 0xd90b29fbe9680c7a, 0x5575a24ba532391e, 0xdebab7c32a11d297,
                   0x09010bd6a6b295c8, 0x5b745d475a2cc5f8, 0x1a17df8fa5d2fb0e, 0x9eb9c9a1c56caa5d, 0x8759a2d4b80f3104,
                   0x1a6732736d219e48, 0xb9a934bedbbb1b64, 0xe88fc304f4f41aa2, 0xb650530683f9aecf, 0xc6de4393e1754ec5,
                   0x455dd664a10d1975, 0x295dffd0c8accda1, 0x4b0e49b50e1c3cd1, 0xf9b16d40c7bcb99d, 0x73f924a20a6b9bba,
                   0x59d4473fddb90104, 0x16a1b347c260d1b0, 0x0b6c06c9546458b7, 0x808f3c2d9e112207, 0xd142ca3cdf2a92c8,
                   0x36b4ea106906393d, 0x3ad7d15985207a3f, 0x5b3236e568021603, 0x95ff5ce56c0c9e4f, 0x35b8973c8634cac8,
                   0xd09591efe1cea59e, 0x9a852d896596d1c5, 0x5b8d231f680d8003, 0xaf56403d17f64962, 0x27f9e41e098024b4,
                   0x276adf3aacd76289, 0x0f828c068e8e4552, 0x45b28f9c40591b8c, 0xf543929acc2fd3a1, 0x7f5c232270f8675c,
                   0x96bf54ca56992a6b, 0x831ceab94de9a77f, 0x884d660886d85f97, 0xa441f1401555b4cd, 0x2f7a37e0df322ca7,
                   0xcfd621bcaf7e8968, 0x2f4c4f54612f78bf, 0x81ad69be79a99942, 0x452169980a69690a, 0xfe7ecd6fdfffc7db,
                   0x6750d09c96c0723e, 0xa96e8b7c1f87d0b7, 0x9355142350227cfb, 0xd04b2c03a5fd752c, 0xaa884bf5cbaef0c9,
                   0x47439b6a210da3c7, 0x08366f260246b77d, 0xada9a09bde98ac47, 0xf285854404e11a2e, 0x1f6b5f0db9344910,
                   0x43635afd969b49a1, 0xf6e336e4883a399c, 0xb3dc4cd8d0589e84, 0xeda5c2c0fc7742f4, 0x10b3b8fb622b125b,
                   0x9f109b5ab6eeed6e, 0x69277c89abd4cd78, 0xea221cf76ab7fc86, 0xfa0ca125e8daf891, 0xdac858a3c5e11c00,
                   0xafab19a748a947ae, 0xeb2b69a5a4da6489, 0x601eb5a12ac4565b, 0xf782900ee4cd7365, 0xa1dc2fd083f4546c,
                   0xf8723698955f8c1a, 0x1ef4282aebf3d945, 0x653da3bf71e624bd, 0xbe04b29a9baf7d5a, 0xb18f73a34543666f,
                   0x90c901c604dce6e2, 0x3208dbe059534b49, 0xacb11eee833467d5, 0x3d07083a9d5be0d6, 0x6bd915dd0fef2557,
                   0xabee35defb430211, 0xb3180e083638dc85, 0x2be0361e7febfce0, 0x4d97cf883fabdc41, 0xf1be01ddae8c4048,
                   0xb7d0b4f0332d2992, 0x72141ded3dd6f55a, 0x9123d943f30926d9, 0x7eeca6bc6be7313c, 0x95ab321d7ee5c72c,
                   0x21421b21f9d81288, 0x19233206a680c160, 0xf09a9d339c9d311c, 0x05133ec82d1ecd3b, 0x71f02fcd10beb60a,
                   0x5e61ad5eeb1ceeee, 0x087c9fbd97613ab5, 0xfe250587d99f3438, 0x5e871afeec0fa630, 0x4f88441aa4eedd4d,
                   0x1a42dd66f7452619, 0xf58e8d8f81b957ef, 0x66986b54ecf07347, 0x2382340318cfb238, 0x7d1c4f93c49b788c,
                   0xd8d9fa627f06932a, 0x6aa7c64c4873e3d5, 0x27efff555b10bb4e, 0x784a1565db94187f, 0x4271a9c1fcd0772f,
                   0xd234d11377eb40b0, 0xcaef5e6aa3977628, 0x64009936900c3fb0, 0xf0ae881430c514c5, 0x0cd32d01c5798aa4,
                   0xc45b39cce35de6e4, 0x1307b9f4d832f787, 0xceeee5e813135402, 0xc930a0867296e8a3, 0xe4b52a4e4f3d9783,
                   0xe4e7934e0d97202f, 0x51d1e9340b2459ce, 0x99cc0f90dcfe55b9, 0xf4f27b19cdedfe55, 0x9debe48d7c4cb666,
                   0xccd8bda6061dd33a, 0x3d1b011a2f870b00, 0xbb4bf9865127ebc3, 0xa149ab768fb8f044, 0x3119a981452666bd,
                   0x65db8aa27ea6d215, 0x1c6d1bd4e5f373ce, 0xa17f5382b2217d61, 0x9599f49e8a4c3636, 0x745bea7bbfdb01c0,
                   0x3827fcecc93556d5, 0x23467c41001df3a1, 0x6b055e5393e6df45, 0x845fe338233e4e8f, 0x2924d70bee302e1e,
                   0x00ff993181bdc604, 0x491f3a3eb8502037, 0x5d76d4307fb75616, 0xd02fc60a95d3e5ef, 0x807b31765ef80cfd,
                   0x94d6208ae964ca56, 0x01985a747cedaf30, 0xc93ba934dd080643, 0xff2171253b5f3f29, 0x661b5bf4c5c90793,
                   0x3e16f8961257ea4d, 0x6234adf4978717be, 0x0fb6ebda63b582d7, 0xe93085e5a1bfd52a, 0x356873f8edf17b1b,
                   0x6bd859a750a2cd58, 0x413c4f77cf56e540, 0x416f62eca618e666, 0x3fdbb2157558f5fc, 0x3446b31c6c14a00c,
                   0xba534ff625129ce8, 0x2579bbcd151f4a9c, 0xb13327991b337d1d, 0xe548cfe328d2e52a, 0x8685e6bd4abe8bc5,
                   0xc912ce5f3fe32b12, 0x544f4c648136ef2a, 0xac9dc00a55c71ae7, 0xb9c2048993ef7255, 0xdbfb82775439009b,
                   0x7124703dd90168cd, 0xfbf35a0d9e650617, 0x2c757dcd616aea51, 0x2c465d4aceb02eab, 0x28cb9d0f78dc8657,
                   0x87cfaeaa76051bc8, 0x0aeaf0c08f5e04a1, 0x7c60c6ac4e1bcdaa, 0x5904b7dcc3ac2e1f, 0x17625e6c1972f71c,
                   0x64dc0d8c46a96c16,
               },
               {
                   0xb2b37152266d808f, 0xd1688ce5907fa9bb, 0xb4fbde50315ebb85, 0x94e1d1a40be0f814, 0xe5ef4ab98cebd353,
                   0xf908528b3ec218e6, 0xacc740ba335dceda, 0x19f10e26e4c68a48, 0x840a9672a2f5a2b5, 0xc0cf3b2996e6d1e9,
                   0x548a34dffc0570c1, 0xdc031240feff5c48, 0x92297ff0b800820e, 0x0c11352081d2a81e, 0x8fd8d29c79d55a69,
                   0x66488d4729e78c30, 0x941d465e2582b6fb, 0xe76b9689bba28cc5, 0x4103bec4b7c08c40, 0x4e4b4b76d188000c,
                   0x844db5402e959fc8, 0x2e134ee324e2f088, 0x625fa933c1fa4d1a, 0x1d4f0121515d0ce0, 0xb1d0d19a00d95196,
                   0xc38fe9c8032d86ac, 0xd6d71c1464cf5365, 0xef8d00bb18b455e8, 0x7536cd4fc7f65501, 0x97b4b02c30117dce,
                   0x7411e5084e20b6b9, 0x988167ff559bec81, 0xf91372f1a162a022, 0xc638ef90a723246c, 0x6761b912a7d129a9,
                   0x56927af0806a2486, 0xae697de7c569d46c, 0xf0587bfdf442de4e, 0x02b83e162ef75c67, 0x712f8e20cd8d62cd,
                   0x443a2b5a16f137ca, 0x5faca36e7dc9b149, 0x3fefa267d527a8d5, 0xa8bbd36ee9c73ceb, 0x451a8aa3972efdbd,
                   0x0b5a7ddaf5d22b7f, 0x37a58d60fb0033f5, 0xd57f6057b724127a, 0xa36b778ce2959583, 0xfd843a9d8800b0b4,
                   0x057f720e9582b96e, 0x4304286582ffe050, 0xa3fe58e3d150bad8, 0x3b1cc25468a8cf65, 0x5a343b883d439ecd,
                   0xefebddaf9fccf271, 0xdee57a385349bdc0, 0x685936c14e3c96ad, 0x54efe66dcb28d0fa, 0x3b3a5bbb4873165e,
                   0x6a10806224cc7c73, 0x8b81d349a6653e23, 0xd3c77a6c5254a1c1, 0xe426ed8badd6ae84, 0x159ed3abb8289790,
                   0xc34a0abddf1c3868, 0x7e0ffe203c7b27d3, 0x26a12e8a39321471, 0x058ef4beca30a376, 0xa329f59e34edcd3c,
                   0x72b0a270dcf9a332, 0x166ceb4a84f8b7e6, 0xa280d2c77091a912, 0xec16fa92ca834210, 0x87cb7bde305e2e8c,
                   0x637fbc31719a722e, 0xf1d6b723b62c74b9, 0xa79f73228d226e72, 0xdde9e7d9b6111fc5, 0x7ceb45bc84699f40,
                   0xa91bfc1705e0d732, 0x306af3eed572452d, 0x4a9433f2005ed515, 0x9f00376f44600cec, 0xefd838b045fb86ef,
                   0x65098de0679c0513, 0x51ccc18e00e21e0e, 0xd462c4e789677c32, 0x95a177c568ffc03e, 0xb12376a070d7b2d7,
                   0x664ae720214ec675, 0xcf15fe99a80aebd0, 0xa7d007e79eec58ab, 0x2c6b911aa9872e3d, 0x77f746e811360633,
                   0xd0830a895f8f557b, 0xfbefdc19fccb70e2, 0x02e4ca8f22349a2f, 0xcc8e04e0283f929c, 0xeb743ce2f3f2a830,
                   0x966611978470f470, 0xd46e0a5f306e76d0, 0xad2c8ed5bcd22c46, 0xf1eba8f4d933b891, 0x058a7875bb427a22,
                   0x45107f3da4aa3e16, 0xe81284e4b7d9db22, 0xe004099ab8178db8, 0x53e36cc21df27075, 0xd9a17d119adf78de,
                   0xe95c27a78e7824e7, 0xb584e128467f80dc, 0xd44ff1193314853c, 0xf0124a034d852e19, 0xc9077af6da8a9ce9,
                   0x57a082ea0d3f7acd, 0x14cd16087a7bf2a7, 0xeb64143d63df7307, 0xe15943fecae64d35, 0x978bb411cfc5f25c,
                   0x76a96f8f617871b7, 0xd7d9eddcbfae7bd1, 0x61bcfec611e3ed1c, 0xf2af9d3e527ecf80, 0x8182c0a8ee728f30,
                   0x7ca964bcddcbe009, 0x99c70ed92ad37d47, 0x52ad14b9a60bebdc, 0x651a92a09a0f6cd9, 0xf081fe7ada03ec1f,
                   0xc2e558216dc44e49, 0x28d1e01b15889c1b, 0xd24d7e86573816dd, 0x965aa3b1404a1b4b, 0x39776cd5c65d62da,
                   0x5a1afb7ffaff7228, 0x993d7e2bccb5b123, 0x8af2f371498b991f, 0xec8f424fd6b122f9, 0xf7a7a05f114f446d,
                   0xf1729f142472c92c, 0x1e47d5d4dbd7d8d2, 0x3c79a550342c0bb7, 0x70d8ff005cc97454, 0x565f5c2a091373b5,
                   0xb2f07256c7f2de3b, 0xcc0e305a4fa50bba, 0x90d7259bcd4d4dfd, 0x57a21147c7b8d4b4, 0xb4e904c414344400,
                   0x1b9843cef6b7de4e, 0xc55ed544783d7077, 0x951fd36278c4af42, 0x5596691ec2a6c112, 0xa96366b16e85bae6,
                   0xdce8b9304db2cf16, 0x4196a4a77dad5d51, 0xc035d6a8486f503c, 0xda814111a183c18b, 0x850d476c24afcdeb,
                   0x8b1fb1911d79a756, 0x3fbb083dc9ff1546, 0xd9ea3048315a4840, 0x1bbaf883b34fb0e8, 0xc7812b9babc4f1aa,
                   0xeee79e6e139bfd6d, 0x0f61f802cad137ab, 0xe6702e761b466507, 0x00fea66009d13ddd, 0x27865b24fdc1543a,
                   0xe53c0cadcbd4ba6d, 0x081dead3238fe2fe, 0xc4ad52df5a17e06b, 0xdf196982de8820b4, 0x93056ac04ef6a5d0,
                   0xe3b0e29b5b394f0b, 0xd22124f2f63464f3, 0xcfdc91988a08c919, 0x142e82bacd825129, 0xa295a43e93e4e702,
                   0x1b344f1ae78d7438, 0x1ad39d6fce7b2de1, 0xf38a7608b3c52ae3, 0x0411864df1f01a7b, 0x8da709efa240a816,
                   0x4db801db3b0405a4, 0x82e53d875886c89c, 0x1f8a6580748a29c4, 0xb5f3c5da7b0c656d, 0xe6308e06103bb9da,
                   0x1c044a02583c3b23, 0x7824fafc7744ded1, 0x120ae3a85640de45, 0xbe7841a70dbe7f85, 0x13b1bedabd2227e0,
                   0x543f4d8601666ca0, 0x140acc8931524f45, 0x07d8d511f4deaf51, 0xe4218d90e617fb4c, 0xbff96e733749a010,
                   0x6cb6990dbdfed3a6, 0xe651708ed78c4008, 0x2d86b38cd518d5fe, 0x41316f458fcf5a20, 0x74cf6939366c2013,
                   0x298a2a81db72ba3a, 0xbdeb78b42a71be11, 0x7bf37745b51a4e93, 0x71d074c050fd7f32, 0xd7ae1af287ef3c83,
                   0x6545c0921918a9c4, 0x81b80a0be366c29b, 0xb5f96c0c829ff255, 0xedc1e2598be87178, 0xd930d6eafb62c208,
                   0x1b4ae0fd3bc45e3f, 0xae4eba5234af88a3, 0xef250aa0f82c9251, 0xf842992589d21959, 0x76d99f8cc618151d,
                   0xc0d053337ad3d0da, 0x87569479dbbdcc16, 0xc0a31211852f8ac5, 0xf09022e229d40060, 0x2a4acf18d2d9a943,
                   0x95b9dd630a124a30, 0xe3d441a7c183a2df, 0xb70592e18a0d31d5, 0x45dfe88bf06ec04f, 0x9a8982b8dabc5814,
                   0x9853c63295eb60a1, 0xcc6ab672b45261c5, 0x1d59b5ef15f998e5, 0xab052dc26ca65d9b, 0xc29094316db6ad02,
                   0x90e2463bc2a67bd5, 0x5d9658394413a531, 0xbbae171ee3e4997a, 0x7baf87e1759cbfee, 0x269b8ee8cb2d9c69,
                   0x98d272695f943d3c, 0xc2aa69caf54a47ae, 0xfb6d9caf908685a0, 0x36aab7dfb5ef3444, 0x9a28ca1db0e037e0,
                   0xd54c3b5005923402, 0x124addbcdcfa5bc1, 0x277b48aaa7bec0d9, 0x76d3a563d86fa26e, 0x0545a2263944b662,
                   0xdfd9e108d234c7a9, 0xa7f406b6d42d6ec8, 0x9becf8e91d0daf3e, 0xc53f653fc3f42bd7, 0x66a70e50c0535454,
                   0x2ff4d545d6a21306,
               }};

        x ^= x >> 33;
        x *= random[0][x & 0xFF];
        x ^= x >> 33;
        x *= random[1][x & 0xFF];
        x ^= x >> 33;

        return x;
    }
};

struct TabulationHash
{
    size_t operator()(Key x) const
    {
        static UInt64 random[8][256] = {
            {
                0x4f3cecaf24409b1b, 0x6ad9f166d91c6613, 0x54bf75358aea8524, 0x3fbc1de24a079a6e, 0x57ea94a0259aec73,
                0xf9174938aeb467ca, 0x6f5deec36e40d25e, 0x534addc1aa10643b, 0x94734451bc5c5ec5, 0x4d72432932c6c7b7,
                0x56ddb2f1203575ef, 0x8f7f217ead5654b0, 0xd7c0eac16d4aa24e, 0x84e4265047714b2d, 0x449769b97c43e1e1,
                0xfed98b3f4c5b7698, 0x48bb913b09ea35c1, 0xe69cfcd6052df551, 0x483636e39bab623d, 0xc108de147d29545a,
                0xeafb7485ba1f8e40, 0xd4a3891e24ab7233, 0x941f1975d079ba30, 0x8776cd48b75bbf4c, 0x42c5f14b72ec0eef,
                0x19a18efdff3a84b5, 0xec4078cb31112625, 0x3063155b2e1cace2, 0x4d0fd702d00c53d5, 0xc80ad41b4e104360,
                0x67e9d8d12617d417, 0x1a6de5d7f6a3958b, 0x9520893617a19775, 0x8842a4072f85e7b1, 0xa066c6eec0f4c288,
                0x91c753eb152561fb, 0x1eacdc853dbdc4d3, 0x38e8e61d8cc61e0e, 0x22117333fc2eb16e, 0xed909eb368ecc36b,
                0x2b67fc646f0b73d2, 0x7c28b15e21c0a93a, 0x13f8de9d1bad4d5f, 0xd96a893cf9da4125, 0xd3bbc92ad05fb53e,
                0xd13aaad8d8075799, 0x18f003d700064040, 0xbb47fd3c38570068, 0xcd3db144d1a1f6d4, 0xebb33f814155f734,
                0x740c6c7f4d91ac30, 0x9e3ae55cefe1f46e, 0x678c8b1c10da8b96, 0x37510cf678751024, 0x4e9e97713eb900d6,
                0xb11271b9b1617fe7, 0xf2b35c453dfcfc22, 0xf5c2a8307ec8d153, 0x14089b8b1462447b, 0x5a350397786bb472,
                0xfff7cd246e11a821, 0xe5649ebd197aa820, 0x5b7b9b407888c0f8, 0x617e4610c8e466fa, 0x928f11ee454fbeb6,
                0x72e6a8006953074e, 0xb695a3dadef3ae60, 0xa26906a7e0140bbd, 0xf856fd404e987a6f, 0x95cddade446d74c7,
                0xf7cd44918454ca8d, 0x34b17066e9a0f88d, 0x3481e736e1fd2fbb, 0xdf09f9dab79514ba, 0xf9352d79440c48ae,
                0xae71591dcae8b4b2, 0x2f1bfdb07031c1ee, 0x50f106bd520bb92d, 0xbc39d67db555d325, 0x0768dcf245299385,
                0x2d0f0564d3ee2403, 0x0b4075b353804510, 0x3722374ac591e9d2, 0x561d767a57b72214, 0xd6e1775b179939ea,
                0xedaf88090284c61e, 0xc897695662d2c7d2, 0xe2bdcab72b125c05, 0x37c0d44b70a6e565, 0x3c5be744f87454d9,
                0xd8d969301f77195a, 0x3445efae8fea16b9, 0x338b465dd8e11413, 0x4bcde36d984527ac, 0x0b803b6976171f05,
                0x51a7386146462803, 0xc2897c1a69b6f1c1, 0x5ab092594684a5b4, 0xd3f4afafab7fc3ee, 0x744c31a06c5c0460,
                0x49f01a31eefe4021, 0xaa62ba4831a87a0c, 0x61052036b02cb121, 0xc4bb5582ed8d7e4a, 0x25b6b517dcf0d7fa,
                0x703b1897ff50a997, 0x83e5b7ef256dead8, 0x8da8e0ea74709e3e, 0xd5e2be449fd500a8, 0x0c386f813d21c684,
                0x4bfee7a5bc728b8e, 0x5f0aca2baba81c31, 0x7ddfcfe8a6e897de, 0x90b69d33b9af5fbd, 0xfddedc985978876b,
                0xc8f93bbdde12f7f2, 0x45d7a1087f4739f0, 0x75ea324184f1e5b8, 0x97b78ad1ec2ef95f, 0x192e5bf5ab92521b,
                0x5175eefdc3fadec7, 0xa23a36c97bac48b2, 0x6e1c21a7c232880e, 0xf8010ca06ce802ea, 0x6e9822f71a8123cb,
                0xdcb21616d2941bc5, 0x6f2929e3943e0777, 0x5a6ae6bc66eb2f1d, 0xf20a3c93c0fa6172, 0xcc1f91002d11567d,
                0xa558d8dd4fec490b, 0xbe0ca1be73f5d533, 0x66dd4427dc66bbe1, 0x6a27341a10cd985f, 0x8d12380bcbd9bec3,
                0xfefaaf20d5b9139f, 0x8641d578cdc4e199, 0x5f21e78ab4d25c23, 0xef52fdd1026d6f8a, 0x842f108fa0fcb69d,
                0x9bc3723415c2724b, 0xc9615a1d92ab9500, 0x7c0850b85e7535cd, 0x8e93357abaf1ed24, 0xdf6fb36799938890,
                0xe2082b8202e65187, 0x11170a177d855ab0, 0x751564db24effa8d, 0xa74b7769d222649f, 0x12a999982fd65d64,
                0xc380a14a49b93b32, 0x5b9e932a4f264ee3, 0x1ca4323b708a0aec, 0x28fd5d5a327c91d9, 0x31289307419c9f1b,
                0x20bda201fd63649d, 0xd1d1c8c8163836d6, 0x9cdae51d6993d0a3, 0x3a6fbe9579c1da3a, 0x27072cbc6d3a4825,
                0x94b7107387607e61, 0xb1b0b605f39f115c, 0xb2557d474dc4e952, 0x8794c509cf6e26b1, 0xc1a8398cb2e98c9c,
                0x92d0f486b7ac4afd, 0x9082064213d7c39b, 0x7e200cefde5ed88d, 0x904b5a00dd597b29, 0xe84c40f0bf571c92,
                0x196b4a47fe185361, 0x2fb3e66b0fa8c185, 0x581a7f90e8646a69, 0x4b74f5f56f087180, 0x2e94fe98e0b1cc0a,
                0x198f7bfbc692eda7, 0xa4d0b7908707016e, 0x0c90ee281eff932d, 0x879a1cf4c24d189a, 0x2d66cea5314bd480,
                0x23346e49480e7e83, 0x82d63e12f56ea02c, 0x4ce802d35f8f46e2, 0xd143fe931608d5f5, 0x66c466071c6718a2,
                0x59a5ab0a39dd53b2, 0x4805529d2a394dff, 0x631151511fde3379, 0xab98f8154c24669d, 0xb3a05b6b742dcdf2,
                0x9d9827d3e071e26f, 0x9c157833c16216fa, 0xa9c8584ed28c8023, 0x94ca7cddf52bbc28, 0xc34d111216c15159,
                0x68abebbb05b62206, 0x4786f8c9094da769, 0xb9c218515d329fe9, 0xd997a2ad91f01905, 0x81aaa629f3b2bce0,
                0xefef8a896238229a, 0x6af252f60dd72940, 0x4492c36d5f165eac, 0x9dd50a2ef4d5f9a1, 0xdfbf94a3550e6ecd,
                0x9288b91b1caf05e7, 0x9c0ad10f9c67d06e, 0xdd4acdcb36db48c5, 0x7570a07af93f329d, 0xa31f26e2fffef103,
                0x2886476327948381, 0xbddd48e33441c988, 0xc887b91661bf6e9b, 0xfc21127e01445e2f, 0xf0a8e3af7c953713,
                0x47da9d0482ee366b, 0xc3d6ca5719939732, 0xedeadea909a6fa76, 0x26fcdad40d586205, 0xa1dd97b354af5ed4,
                0xcc8aa75425740654, 0x4a0c5113156178f9, 0x981abd8fcf766b13, 0x99f31176a6cb712a, 0x49ac51e92982f34e,
                0xdfeaa1da9373b16f, 0xa31666230da09aa3, 0xdc1eaceaf1afa2e2, 0x4326313df2d59166, 0x8836b6746f79236a,
                0xe166e915f72d2540, 0x09cf7b4b78b2637e, 0x55369c7b01071949, 0x86ff67741a6c3cb6, 0x76503dcd4383b8cd,
                0x36731b7b7cf8df2e, 0x51a2d6aad260273b, 0xf6d7cbb0db4253cb, 0x1230054a3bd28926, 0x4eb266aae155fc98,
                0x9a2a7f60a9dac0e9, 0x79290b7b9035b3d4, 0x3a424cdbbe2aaa45, 0xcc46944623341c59, 0x7c185b60bade4199,
                0x6ae892b88237cebc, 0xdd8ec8066e75ae58, 0x69a8aa059ff444a0, 0x987e27eae07462d5, 0x539f69c3d236e3c3,
                0xc6bcae3c97941ed6, 0x29e853acc29b3eb5, 0x1faeb880153ba613, 0xd35bb34905b04517, 0xabcc7468231c83ca,
                0x286d214a37fb65b3,
            },
            {
                0x55baa482d6094d30, 0xa775277f382073a4, 0x73bae2387e32c5f8, 0x4a6416aaca027e0c, 0x0ad38404a8ca17e5,
                0x37ca2344345f1a5d, 0x9c910f03937fc178, 0xd99efaccd8d0f15a, 0x557c9d7538915f7e, 0x500ee4fb62acc9e5,
                0x3f1eb011057383c9, 0xaa4fe82c1926b108, 0x84e24186cfadd3fb, 0x0862cef2d38de5f0, 0xf03099ce7d642f66,
                0x143b22300078711e, 0x04d44b5b3444abb0, 0x2c46d930d09accf6, 0xc5c8cfe5da8a7e49, 0x9c0dfe1f0c791972,
                0xe287ed0f98ffb25c, 0xe91eb5daf941701b, 0xf4989aba25810a10, 0xa23853aed0869787, 0xc008036099dbab0e,
                0x752dd9e217b5c4fd, 0x763c0b933129e776, 0x7b474798a5a16875, 0x31c3cf01e8ac88ae, 0xa3075c68c217fbbd,
                0x12b53ba28ba2ef53, 0x62d619c3d366944e, 0x8d1aa326f3f96bb6, 0xe4ba4927c89184c3, 0x863b2374081f1e4f,
                0xa2f5b1f264772e64, 0x7d8085a2941f5b09, 0xc19447f7e00838ca, 0x1fc50c73bd36adff, 0xc10e2d3ea18d12a2,
                0xfb9d34ae20b0977a, 0x809ae69b8558a6d5, 0xf643d21e8543a777, 0x1223ae6c2809d3c5, 0x5ff0047e9b20a9d6,
                0x814623fd6a99203e, 0xc912bd1a4b7ba3e1, 0xf4683a10792dce3a, 0x6157694e70c50676, 0xae3916927aae4a7c,
                0x0973002f06279ed7, 0x0f4cd1c254c5e26e, 0x7d7ba6bb160200c3, 0x3c3bd06bd3fa92d9, 0x1e559440993b113a,
                0xfca7ed369bf393eb, 0x6caa82117c2f3ca6, 0x4d848224ec4a0dfd, 0xfde9e96a3023bfc4, 0x3bf9ed37ebcad940,
                0xcbf6587df4678baa, 0x98d4968ce7ec8779, 0x2644f85c1e0fbe76, 0xc984308f877374a2, 0xd53e563c2f8511a5,
                0xd6dd5155704369f7, 0x853bdf051a2a6a2d, 0x21d9a29d02e502b4, 0x65150c59e806b5a9, 0xa1943a083f94e255,
                0xdd5422dee2ebf9d2, 0x7a418ce99f1795b8, 0x98b0e22488a10741, 0xcabcf61f9966df2d, 0x58e14076e68fca44,
                0x314d7ddf395cccdf, 0xcc403b46e733f565, 0xf15b35f8d71d8a1b, 0xf5f1b4f972ab688c, 0x1b3b1dea29c68904,
                0x2c96354f3f1da731, 0x00f045692ffcf9f2, 0x03ace2077bc44510, 0x7da8d3d81d6d03f5, 0x5cd83bc6f5d9f8ad,
                0x7c04887a3721286e, 0x1bd151e4125406cc, 0x888c7ad9ebd8ba2f, 0x143f637fe24a0da0, 0x6b3779a345052dc8,
                0xbef717c4f597384e, 0xcbaffc196a0aebed, 0xc7c4c469a8d9c20a, 0x4e260ce8512295c8, 0xe0c9b47f50817abe,
                0x0dd940d3900eaabe, 0x91d566526665dcf7, 0x470d9079c9c2fc39, 0x388867f7a6d48949, 0xd0c3ba55686d3d6c,
                0x9880ba4fa1a0765d, 0x31178f63e3c86654, 0x1ad2daa404e3ce35, 0xcb816899daf1d758, 0x943662411dd67f38,
                0xff9592c29d8c8150, 0x123d9c7fe4bf5702, 0xae43ea51c75b1793, 0x9d3e98a5ae2bc556, 0x601f8a748a24ab05,
                0x3b090e60d6837c99, 0x6236755d48edd559, 0xefff911ef6b222b5, 0xdd567e4aafa31faa, 0x98bcb00b24a47474,
                0x7cb94e6af25671c5, 0x5e821e019dde465e, 0x7defecfae68e913b, 0x915811752db3d4e7, 0x8be3d8cb181a5e8f,
                0x95915979542f5ef1, 0x49708c505e34e6e5, 0xda4c55a2f6d1dc53, 0x2474d9abbfdc58c0, 0xc83b01a3120290ea,
                0xd35cfa7833fb3e6a, 0x26dceb5adf4d2e72, 0xb4a95e8d3921a995, 0x05f9eea1de04e25c, 0xe1b3f728f77a0ea7,
                0xd0e72bcdb5a43289, 0x532a242c481e5582, 0xa1af8ca39bd360b1, 0x92f5d69ceab3c9a3, 0x4d3effdcb8889428,
                0x1a41253f9e794758, 0x8ddaf31b28db2834, 0x0e183ef313da9b9e, 0xd9b3f3a53daeee98, 0x39d555f28e821b18,
                0xc5209431aed2ea3a, 0x70836f760f213e2c, 0xc8eb3a1a5b250610, 0xee225c27d0fae90c, 0xa3d1bfdb2f236eec,
                0xe4ea1d3c6ab29fb1, 0x3e4113121d92c559, 0xd6f76fcb55b71cc9, 0x9b2c2b439c547467, 0xd4c0eaa7c33240f8,
                0xbfaf66aa8c19832f, 0x9d3b1f5a4b810871, 0x01635ec6f0d79ff2, 0x60ac5b58598a106c, 0x487dd435e33b6846,
                0x60bffd49a79cc594, 0x7b94ee39d7958a63, 0x146e9a412ad1259f, 0x7f78dd55015459f7, 0xf9b4611ed19dcc7b,
                0x02099fed0debcb39, 0xfa2c53462f353c9f, 0x4df9be5b185dac4e, 0xdff08e1bb6197444, 0x68b8eef25269c9d1,
                0x29375f899bfdf95c, 0x717425fb6cf4f3e7, 0xd05924d6516744ea, 0x7767e15c3f0ae746, 0xef09e001fbd96e10,
                0x3ede191e2efd92b4, 0x2870f7b6584ed8db, 0x5b865889a51ac221, 0xc48bec3f1fb51b24, 0x221b7a0a04040567,
                0xea1db24529ff5064, 0x1258c51e195e4518, 0x84789f760d2611c0, 0xff729a0328c4f026, 0xf600dfa2d51a3a17,
                0xf90dc307cb46a03a, 0x9a7fdb17130b19d8, 0xbc52f4ea57b7260b, 0x40a6a8b285f3ab9e, 0xeb3cffe125e5ec4d,
                0x7403e102ed00cedb, 0x73f6ef7ce2d178b7, 0xcffa6b38980c2e97, 0xd0e796c209ee19d2, 0x592bf15bd1da7c94,
                0x39d26577581c4aac, 0x5396af489dd6bc53, 0x93258b2aa667f2e1, 0x848a94686c6f7296, 0x03e62afec9dee865,
                0x07f2fd48c7715faa, 0xc15419583397837f, 0x672d8bce0e659a4e, 0x30773e75d88a6236, 0x87e89b73fd6b2338,
                0x44b36eeaa3cf1782, 0x463715a899473e12, 0xacbbbf9ef34f1993, 0xe69201e06e0366bd, 0x4bb133053a2711e8,
                0x3d8a3aa545b44de0, 0x76c4052be97df9b4, 0x764d1f80780c0dc3, 0x2f857803d304866b, 0x6e4bcb1fe6dba6f0,
                0x203bda623fffa785, 0x4d4ebd02bbfd90f0, 0x4bb68df8e27003ad, 0xa8f2c6e2fcdd7a3c, 0xd671f8629f1e6a49,
                0x8c5bc906af678f51, 0xc15daa98abab5323, 0x5e452b96ea600fd3, 0x8639b76f2b8701d0, 0x38d0efba497d2ed2,
                0xe58138b534ec6b00, 0xc2ae5081bea98827, 0x94804a7d23f42a72, 0x2aeb670b22f6b6bd, 0xf82c45c2f9056dcf,
                0x90688fa9103a686d, 0x033d4d30d160e812, 0x882a3ece84fec7b2, 0x3e6a772e2759e4da, 0x5e8c67943a040727,
                0x32d55a6250431795, 0x7fc3e1139a64263d, 0xe43b8cc338035f25, 0x2dc5cf5c635e6b83, 0xa28357ae64955e46,
                0xdd5be08f32c7243d, 0xe444c100eab4a18a, 0x879f71ed16556a1f, 0x4946e311697b872c, 0x3b03ec5646dc6737,
                0x50dfa7300f6d9cc4, 0x8e7f48c57f42e905, 0x3649232984345f09, 0x8f8455e58304e686, 0x1ddecfecdd046180,
                0x3bc6a932474b2a09, 0x350c7f543f5ccce4, 0x657e3a0eb2152346, 0xf71988c12b0bcd16, 0xa3e5210ed52837b5,
                0x1cc15b50de742d38, 0x6d951d94d09d05e0, 0x0b62b88308358306, 0x7d8d89a552fc7b2c, 0x2ca923216a4670b5,
                0x9b9ed6833377a5c8,
            },
            {
                0xbb80ca3eebdae7b4, 0xf5fc0d7464541a53, 0x9a2c38654912cf7a, 0x021a17fd20db7bfd, 0xbc058265765c6a1d,
                0x8b50b42412f8dd1e, 0x61dac7b7556a005f, 0x0df84fd583671469, 0x58165564a89bf07c, 0xff19c3a56cfe3267,
                0x858ab38bb08616d3, 0x645f43110df8c740, 0x30348b0389c8d7c9, 0xf7d9a363581d8eaa, 0x671246f994cd2296,
                0xa806a2677b012b0a, 0xa3c10c4870db951c, 0x56034551fe978bc9, 0xb06ee3c60f36cb7e, 0x632d0d010f3dc94d,
                0x75d975398748a7c2, 0x6b90477488a987b5, 0x9fe5b16871658519, 0xf04888c6a874558a, 0xb138a4d0d84d3cab,
                0xb11a09eff6ff47cc, 0x4eede1a796b9f4b0, 0xbb484f86d18aaecf, 0x2a30f47cfb79fa2c, 0x81ae01d95ac19546,
                0x994abf9277a54039, 0x8ccdf00781ccd895, 0x7a0f04ea61661d2e, 0x89c3a67131ccc7d7, 0x3c45517546a7ba31,
                0x22fd6f368fb15fa9, 0x9de7fbe2b23b406c, 0x9c781d12f148508a, 0xc3a45b7d9f3b9a66, 0xdc9e5ffdfc3dd94f,
                0x2e7571001e32d2c1, 0x50e37ff45132e76c, 0x470f8344ec798a14, 0x4935da7cd2ab3c1d, 0xbae60625dbb8865a,
                0x17c9776beba8739b, 0x43639b7937f12895, 0x6be16198b6f962c3, 0x4d91d6ef47a9a3db, 0x2dbdead3f22822f2,
                0x3c81b7f0cfaaba7f, 0xef3a37046d27f6f3, 0x31d68e3a049ac326, 0xa8c44e710a99aa40, 0x93be20d0a0572ffe,
                0xa38e5189e299539d, 0x3bee2001ad51ba22, 0x46edbadd327d48d7, 0xba8bfd9e4105d214, 0xb60dae7c54d82759,
                0x7d93ed61474ac119, 0xfe57a9875ef3388c, 0xf27d7420dbc63872, 0x5f026dd089f44ca7, 0x04ccb7959ff3d70e,
                0xf1c48fb27f2396ce, 0x9be866e8a8598eda, 0x6ac8d846c3e15a03, 0xf67e5ccd61c63b36, 0xdf30033839e8d09b,
                0xfc1af6737d5bd799, 0xd670eba2d160fb3a, 0xe2e38d82c480922d, 0xd9c110c0b7454bb7, 0xcdd114994b88c8b4,
                0x99a7cf833cec3774, 0x66f2e51c71fa8474, 0x5ab5e8bc8b4b961c, 0x8079e6a7a2c87c6b, 0x346759d7e0e87e26,
                0x3afb97b58da05ee7, 0x29c3b79b60a81743, 0x673a121ba778462b, 0x58bb6651c3676e58, 0x21ca8f3b3717e87a,
                0x48eb7f1e72e2d6fb, 0xf1fc8baf3eacf652, 0x6b60026b8bc860ef, 0x8d63de031fa90638, 0x1e5e8d6237d49d97,
                0x38136b31c0b6215d, 0xe4577bee2ba3ac08, 0x82f23249ef3f3240, 0xfa7d83521c2cd147, 0x0b0c9b7d0a3e865e,
                0xf91c445ceb6d8de8, 0x20c4aad83cc1588a, 0xfa18db09e7aa57b0, 0x9462bde17785da6b, 0x58de5861f9e1e0f0,
                0xe1ea5249d05bc6b9, 0x64122980de6a88dd, 0x2dbf98533c8dda27, 0xf4a3f494cbeb0826, 0x0da98b56ec88bb70,
                0x0f3023729baf8a54, 0x2de585c4fbd9726b, 0x2e75bbaea4864189, 0xd882334004de7a85, 0xb2902adfd01a3826,
                0x97c3aa5b950b920f, 0x46842894ea9be2b1, 0x21eca237fe98a7ad, 0x3b1b381c09d5fde3, 0xcd80bbf3dc0a4f51,
                0x81a22e80d737c423, 0xdfa4b9c3efa2690c, 0xb6640950f3512262, 0x8ab28a948a71fd7f, 0x1dcc9fa50918eae2,
                0x2fcddc6cb9c448d8, 0x2af05f8c76f15e5b, 0xcd239ecc0d4ab1c2, 0x842916ee4c7bc90c, 0xa0e0067d3a2ca83e,
                0x9c057b0a000c604e, 0xd66eaba1537d6547, 0x0fe6b8cfc9d6b2db, 0x7bca27e7f4463505, 0x795772f9d573c8de,
                0xe7bd9d26f2c5a659, 0xc647ade7c3e886e1, 0x555b27d9e409c407, 0xbc92740673659089, 0x9937bc604b77b522,
                0xecce084bccc0c015, 0xf040a0322db51be2, 0x48774313fa139741, 0xb0ef318171f2b231, 0xe1a8325d453addcf,
                0xcff3ab63cf4e5300, 0x53d93408ab26653e, 0xb62ec45defb84ad3, 0xefa0678ac0ae466f, 0x3b77863961b7dd21,
                0x9494470fdb1023d1, 0x7fc8a6630e395ffc, 0x34b2823c1fed2f01, 0x52bac1ec78029fc6, 0xb5b3dc553d87f40c,
                0x8123455bf05c58d6, 0x99b48302e79f7585, 0x13599a21e6e5dbc6, 0xfa9ff5b4d2450b6e, 0xfeeff34ef2cec325,
                0xd06b665f50c684a8, 0xd79c8e679afe4e42, 0x396e4fa2fd3d0219, 0x2470437d8c870bc1, 0xc409103bdac24398,
                0x716685e54881c123, 0xa682b9ee57ceaba4, 0x5b072e58695c9d2b, 0x10bc4ff5df48b7f4, 0xf34e0ebbedb97531,
                0x67b217d3fef4a126, 0xdebd2bfbb4d93189, 0xac02afd9ed15866d, 0x58b5a2be17103744, 0x52d898777aa0eb3f,
                0x5ffc2287b414c08d, 0xcc4b3bd481c388b9, 0x9178a0523ce86cf3, 0x4052d981085790f6, 0x9dcc9bf08a7c81d1,
                0x63c090d225c1ca65, 0x10f5fd1d5c89636f, 0x86fbfb7d0d9fe33d, 0xbf0ce7f29378ada3, 0x285b12f15fca3646,
                0x48f4bb45a0ac2d95, 0xd03da469351ed891, 0x6445027b3770e1df, 0x619121108e91a7e5, 0xae25db3c091700f8,
                0xa420a3820bce4cb3, 0xac925ff9d73c8aeb, 0x938a9d2d71e5de1b, 0xb117d661029ee7c3, 0xd26c8298488fe9a5,
                0xaaa6efe17a616f72, 0x0d762246d5447e63, 0x8ca2e2259cb0807f, 0xd4ee85540367fde5, 0xf48c06b4576464ca,
                0x8939ce11f242d079, 0x61f661397ee42812, 0xb9c933b237d256eb, 0xef6462b82accd495, 0xc841b19f709ee355,
                0x40a83507a842a821, 0x81ebbef507433d5d, 0xdbf8e966e48b8425, 0x6fe079d5474e7efe, 0x95164bf3a6925ba4,
                0xc606b1562c361bb6, 0x5e4478e57790321a, 0x72681d0c09b894d9, 0xa76b0fba11901df2, 0x31440946e0469413,
                0xc007a32eb76038c7, 0xab2153b29a4f0afc, 0x1f260a34d801d133, 0xd478c19f40b0bc11, 0xbd507cd60448b636,
                0x8bda5189b7e59cf5, 0xffe796e29958ea74, 0xc4497c21ed10d1f6, 0x0969bce43ac75030, 0xcfc56de77cb6a984,
                0xc562800010f354eb, 0xd5412ac41f5e7a68, 0x3adeb5267c1c4c5c, 0xe4f9374056ed7da6, 0xc0145376822450fa,
                0xb49b18b4d61d9c87, 0xffef71bf89364602, 0x3eae710f3dc5a3a7, 0xa3d8d45b2acbeee2, 0xe0629b6961ad0ae1,
                0x7cbdd9e4aed684c3, 0xad7465e6350cf367, 0x4bb75782cd44f10e, 0x0b779338049be08b, 0x9f4d62c6a7f950e5,
                0x7b77447b20b2c5c9, 0x1c202fb8a66a3b97, 0x6a42dbcbda5bd100, 0x30092bd58ff3f2b5, 0x4932990285689a17,
                0x4eb3bf0664008056, 0x7349d0f71c402501, 0x1ac5c7890be1648f, 0x804e58480e1d9f03, 0x7745715195b33cc1,
                0x7acaab2ae047de17, 0xbef967dd731932e8, 0xf39b36adf14f5f39, 0x54de75433d72679d, 0xa024427954257afc,
                0xdc1f2602b79f60f6, 0x6945f75ca9b85afd, 0x79be4d5842f6e76c, 0x8587b10172ed4972, 0xfd94c25d96f769fb,
                0x58fc9b5d7e109c27,
            },
            {
                0xa88a7951f6af7c26, 0x854223da3e20c283, 0x2b2e5c4b70cfae4c, 0x63d43562ba946eec, 0x3a222d1e4bd67ede,
                0xecd349cc5df57d11, 0xd8813655f95d202b, 0x8da9c54f840b6f23, 0xe19c356b4c7b0e4b, 0x33f801964d3e714d,
                0x35ec2635f57d0e59, 0x2c9ad2755c85c608, 0xcd9005585557a085, 0xaf5a46b900bc2133, 0x9586a1d04597e820,
                0x092aa13847e86510, 0x255145c434b7e9ed, 0x3be538ef4cd81013, 0x75c1a51b0c0a12af, 0x97c6bc4285339205,
                0x71527e929440d1a8, 0xd89efb27b6632cc3, 0xe37e45bcf5418690, 0x9c19764bdb37ef75, 0xed923b87b16de861,
                0x285c43a3fae7fd2c, 0xbda2e10ae12bf7ac, 0xf1d3e73d1c39c17f, 0x5f476ec09a2b58c9, 0x9ed9986bc0418584,
                0x428210cdf58a1cb6, 0x99445397fabf0c25, 0xca1937c189d93e58, 0x16fd348ffd5d396a, 0x818f411019500b44,
                0x8a950a743d53ad01, 0xccc6eea71c3bdea5, 0xd4c55e5e176b9c4a, 0x3220310c80c91a0c, 0xbc91c8af3025a7fd,
                0xa52ac42dce9de1d7, 0xf1d40470602653c2, 0xa27709831970c604, 0xd9f77ac5ffa4546f, 0x33e08cda8037240f,
                0x96291c5479dbb9c8, 0xcf93a6ec7a12e2aa, 0x3c414a99ace70849, 0x122e90df962e9ee4, 0xf29546a83900111b,
                0xec5663205fbfcf62, 0xe54ba5b1804c55eb, 0x29a082609d1b4607, 0x159d3fcdb03a178f, 0x432d87c0f1810558,
                0xd671f5b26b5d3965, 0x54ce81d17eb43632, 0xf82d6c60b41a7546, 0x1164d69b71895a9f, 0xe83730e7cf0d4fd5,
                0x1ec8d8088556425a, 0x9fe70630541a2576, 0xd0b459959e7aa521, 0xb5f8fd1ceb0d643d, 0x3f264f978c5656e9,
                0xf43dcafceba77ce3, 0x75cac4b48a08888c, 0x921d45dd4616f967, 0xc213d9e1ced5f9f1, 0x19663c87a2146214,
                0x7c27f197147eff89, 0x7dc892acb09d55a8, 0x9e5229b1d98ceae3, 0x4edf950bd9d0bc8c, 0x4bae80af91b8f299,
                0xd4033b0de084e625, 0x087bc700c8b74054, 0x28e1a48622969996, 0x7f7cd7de54d8b186, 0x88c3d303be96ab69,
                0x9b45d7933cf40caa, 0x6df9872317ad2f89, 0x024a9ec77017d4e5, 0x165ba6c512af3185, 0xcf84b4c2e74de3f1,
                0xbc697622ec5647ae, 0x706da3e4a61fe96f, 0xd9d3f9db778740f1, 0xf9c0e5eca2f2bcff, 0x2d524da481d9272e,
                0x16bba40a6cce9ba5, 0x750e05ec675e93da, 0xdc93851adb541514, 0x9f1c7da1e981d778, 0x190070f1a5cf108f,
                0x2c888733a270fae7, 0x39456e2ae70645ab, 0xd4c9d08cdcb1f71c, 0xc8196c614ca3e3c4, 0xd2b3cf7103fae0e9,
                0x8836fd84df92bcc0, 0x6d50b0d36d72027b, 0xd11a6103e091d19b, 0x31b7b9817144abc1, 0x7f7052bea785c0f1,
                0xe427d056c016e098, 0xde52125ca177b382, 0x97e2625bd4b40471, 0xcaac8129ff6244ea, 0xe2d1883196dc692a,
                0x5b2b56d6cc7f1e5b, 0x6fcf8aa25135af49, 0xe6101fb2e7485b12, 0x43f699984ef3c842, 0x9063be433e05bb12,
                0x3dc329422371eaa0, 0x599eb4840a097253, 0x89352561c10c16d8, 0x635bb8b0d0f2b10d, 0xd782844495ae23cb,
                0xc3600266328f4a5d, 0x4abcc70311e5544a, 0xd7255c63db4e07f3, 0x39f379dcb004d6de, 0x82b2de870ad576b6,
                0xf77baccb37f7a560, 0xe69c1c3b10f96c4e, 0xce668e8e4b8bdfb7, 0x5861502a49b34f17, 0x478844ec4a8d5d66,
                0x29c8670c21feb8b1, 0x07b62ef8da3d3f98, 0x03198d5e29bb4811, 0x1d7cc509bc687944, 0x0f4bf147856e231b,
                0x9bbf2c791c6f62d6, 0x97ea02cecb758f59, 0x519c16e96124ca95, 0x4590342c9e56d7e9, 0x6c3162803d692c2f,
                0x77d690a788f2c008, 0x5b26afe0c406bcb0, 0xd2edea6ed5cbdea3, 0x23312eaddd21053e, 0x25b89ec08450bfb1,
                0xdc6c41322241964c, 0x84b621796cddbc43, 0x7aa80d38b629966a, 0xfaddb1d7897daebf, 0x52c9251f7c37b125,
                0x660c122e18c6a660, 0xed69ac30d66ce018, 0x417d46843cf3717a, 0x2eb3c3e35b5d50a2, 0x5236d099920e9c43,
                0x82e41f78bb052814, 0xa6c70570b1dc8f20, 0x7c6c07e98c8192d2, 0xcfc74b2d5b86c5dc, 0xf1c4bebb6ea53836,
                0xeee8de556fe348d8, 0xbeb43fa9f9ad4d1b, 0xd7bb5de4e32801ca, 0x331badb0d8fb1447, 0xcc484b71eb42660e,
                0x08bb5bccd9b6cb79, 0x061a30335f58167a, 0x30ae0e1ee7d10f15, 0x0575c89117a048ac, 0x4cb57a8db1042199,
                0xeb535a2899c5d90d, 0x6ccb63317da9b90e, 0xc2e045cc5a36ab33, 0xd72daa0318985a3d, 0xa83e48709485bcd3,
                0x88bff618e9ce00e9, 0xd130f20674e94346, 0x9c604d925b7c78e9, 0x57f9630b7996c471, 0xf4167ed7bf51929d,
                0xb47df98e06143ad1, 0xa91de782989a17a1, 0xfa450c2a983a516c, 0xd9f1ffc1142a974e, 0xb12fa960ef7acb7a,
                0x9d1145c686991175, 0xc843bf8185df5cf5, 0x999f7f2a410af422, 0xdc14ac5eec660cf6, 0xa7305a02180595a1,
                0xb026a76f4950433d, 0xaf56dbdf42def30a, 0xc9f77d35c81844d5, 0x3e3dbe0b889f855e, 0xb7c6251ff5793431,
                0x29970f12adff286d, 0x0f4256a46c9509a6, 0x861398aa76312757, 0x80117a92346c98e8, 0xcb941de6d36d9a8d,
                0xf96763bbf815159d, 0x9dee36d76883f2fa, 0xce293626a8588322, 0xf2cd528f36deb9fd, 0xee498833047760ff,
                0x1735ef925ff3d8c9, 0x900e8a7581ba1243, 0xee66bb7219040439, 0x6916ba9919ee8980, 0xa5bf86f79c70dbe2,
                0x6a4d218937106d41, 0x75b536d8a0e5934f, 0x15bfc0732c3a95d8, 0x60d202befa86f934, 0x8fce130f3a4721ea,
                0x1dd04bc6bcddfe7b, 0x6253416ae2d529ab, 0x7942ac94c61a1103, 0xe30727ae935ad303, 0x198c70f055a0e11b,
                0xc02d61abd4e871f8, 0x2d7fa01655e098c7, 0x9a18702441c63e27, 0x89f16573478cacbe, 0xf61b4d689888d5ba,
                0x05854fce331ed312, 0x25adcf465754d6c7, 0x70e46a973b538cb5, 0xa567e24814e5eadf, 0x91819099ee2f7f3e,
                0xa3fd1834aedf96f0, 0xa66c001124586041, 0x1de1d70453fd3ffd, 0xf08f8c9a3e638f4d, 0x5e7d8f4f27e06fde,
                0x753ef15506e33866, 0x70f4901b22561095, 0xbf657c8be01ac8f1, 0xe3de164e00ab04ea, 0x88fa9dacc90e5e8e,
                0x807af1339e75e9b1, 0xc2051e7c2d63f73f, 0x2aec04cbe0b6122a, 0x1f75fa1370b3c282, 0x8408940d0d46a3ec,
                0xa7f7bf86e49f72df, 0x41ee278d9e440b0c, 0xff2158d4506077ec, 0x577a7ead4560808c, 0x7528a71b079c5f6d,
                0x5508b7bf4ee8a355, 0x36cb40725f579ddf, 0x6f3628b1c380efdc, 0x53603471e5bde573, 0xb64003bd645a150b,
                0x057df07f11dfd440,
            },
            {
                0x9a77c7345cda77e2, 0x1ca85a2de1696bd9, 0xa5d9f1910f778ee8, 0x0f0568887c6835c4, 0x24d690187d3653b4,
                0x16910eb48bbdf5d0, 0x91ad593d66ec3534, 0x9c84391fd555b7ea, 0xb7fbb2192c63fe68, 0x7cfed94f758f1072,
                0x18dcb3b59a47ebc5, 0xa55f240d78072f6e, 0xe9c6c21cca2f8688, 0xe19eac5f19e46ac1, 0xb2682eda034d9cac,
                0xfe3158c2b0cccc30, 0xd4893a7d8dfcc06f, 0x45758e32476450ce, 0x566595a64c4095db, 0x1c4d3bdf1b7f3f6b,
                0x5a4a224a87582225, 0x67b4874cfa50d87f, 0x4afa51637244ed08, 0x972730da8b35e3d8, 0xc69ccdcf7676fa8f,
                0x38a300f1a694a5a0, 0x58e578aba836edd9, 0x66ed06828b3cec53, 0x90fb57d1165837e0, 0xf8a28b42413bf900,
                0xef361c212eee9d10, 0x584b1ffa73cc8cfa, 0xcae9e0a934eb7d6d, 0x54167f5166af683e, 0x0c86f4cc8b19543c,
                0x6e895e81dead892f, 0x4d3712e2ce5aade8, 0xc1f214e71386fc5f, 0x5a919420900ce0b1, 0xe0d695ae0e37816f,
                0xf531bb6a312b2844, 0x2ed6482bf280f375, 0x6f2cc06de0ed8798, 0x67ac3ea472c443cd, 0xf78f10414e69031f,
                0x64a6fe0d3752f133, 0xfc579f78ad9c5fa9, 0xff7a4136a5887609, 0x83e6fa63dd05fcfc, 0x68bfbd0be612a1c0,
                0xca9425599a7540e9, 0x95726b8f12208486, 0x79044fd8de167316, 0xd7a9dd6654df09a0, 0x7c5e2c454fdd3efe,
                0x987a944a6698f80a, 0x14180a87e1e47dc1, 0x6e20e89e26bde6a3, 0x4729dc95fa0662f9, 0xa821bba5ce926e50,
                0x25fda875eb4a26fc, 0x420e8ab43d767572, 0xbd72b87e93afe4e0, 0xc613165495fb12dd, 0x6c911c526cebf7b3,
                0xd5375557a5e06a20, 0x7827cec995f9a783, 0xaf5bc58a7a6256f9, 0xa804f99b2986c91a, 0x090093da8f2d134d,
                0x23a07269e2b3bdc0, 0xa0d112abebc21ead, 0xea60ceb3baede59b, 0x8edcb89e1449ebd5, 0x027227159f41556a,
                0x818a8f349b46f3ca, 0x53c2fe4592c6d0b7, 0xad4c079cdf7f0f8f, 0x29ca4e889b080e1c, 0x0b24ef9197d62236,
                0x8407220fcb49ff43, 0xf1ec64ff67d2a53d, 0x3257f1ab7b151e38, 0x473dafcfc804d842, 0x4840a949d38ee2e3,
                0xd545611202ed1527, 0x2629c7420b29415c, 0x98dfbf9765f495f8, 0xa89e3299b7ff0dd2, 0x0acdbaea06493258,
                0xd9fc11c216a06144, 0xa742f8c6441ceaaf, 0xa9bb953a7f594f36, 0xa5d25771ab29777c, 0x07467df7ae8651b1,
                0x761c1e03fcea8625, 0x4fc0ceb9103e332e, 0x194f9eb3126b3469, 0xaaf201319cba0588, 0xdd357625e56d8d4f,
                0x4c59e1e05c9c6e3c, 0x5de60410b28490cd, 0xf7b5fca38dbe10e1, 0x3cc4669d484a0e58, 0xaf5e4a1497b92520,
                0xaba9ae98f8eb01ed, 0x23dcb749536e6d96, 0x176cdcb765585dfd, 0x9fdcc03eb6cc8aee, 0x7ef90df8d70ac032,
                0x869ea534767effef, 0x3192464580f307c9, 0x0f364ffd58da0220, 0x5241ceea28126c87, 0x93a33f579362d687,
                0x0ba283ed8c936244, 0xdfeaef961188fc6a, 0xc5fd5d7d19be65aa, 0x6cf7691fe8b97941, 0xb098cfdce8f83ce3,
                0x2d2efd8e86efc5fb, 0xc847895f32509250, 0x7d5a439ca9805332, 0x6c73f8404c1d1b78, 0x63537939023b3534,
                0x8f8c41a3790edbba, 0xbc07c56e04f83c0a, 0xd7776d8ceaa173b2, 0xb0958a6df751ea82, 0x72a49d76e3fdaaaa,
                0x9a8755ed0d97c66a, 0xcc0212498aa9cf50, 0x49e15b32e397894c, 0xe7c9c8e6bbd3da89, 0x10320696fd4d50ca,
                0x2e8e5e50b61234de, 0x65c4cba41a7c3d6c, 0x611009b13d54d04f, 0xf8a3147d1f5f8703, 0xa5e55f0497be92f0,
                0x40972e0f332a88ce, 0x993e6b327b8b1b75, 0x3106bd73d7354afb, 0x38d21d37d5a32391, 0x23f2e73b6b532c54,
                0xaa59ee2a4c0c0ae4, 0xa872464eebe07296, 0xe17597ea6d64ca1e, 0x7e81473b38c3a278, 0xfe779897a3a0817c,
                0x6d39781d430c4199, 0x32f38244bdf00c87, 0x9170f5844adc6a17, 0xe7702538eab72626, 0xfc59dcf970709ec3,
                0x4f5a5b877dffbb7e, 0x522bb999b577b0ae, 0x2f88f8601b6f03ec, 0x9359cf049c509223, 0xfbc59ca041039433,
                0x0a130f79dc2c18dd, 0x2828b4b5c02cacac, 0xd323880c43c9088f, 0xa9d0fe1a5b95c24c, 0x4709d409107ce76b,
                0xe3a966ae902a7db8, 0xe2d8b08e6ce7a27b, 0x87342131b457b8a2, 0xec10987c7b4e5237, 0x0686be69892b3d8b,
                0xa10e34f060579ac4, 0x25b4f4d8893ef395, 0xfdf80694a47c1318, 0x4a7aef81eb0f0ccc, 0x4cdd7f1f2e26765c,
                0xcdcd19799d02f601, 0x2a73302b9a783cec, 0x41c90bcd5a4048a5, 0x903cbaa13552c646, 0xf21e5360d91f720c,
                0x61eb5b8837518aea, 0xc329fc22f184568c, 0x8c7bae6e28c5a811, 0x487b2e663c332db1, 0xcfe6e9d0990c269c,
                0x2d470bff7ee5ba04, 0xdf7a75e3b806df45, 0xaec2713615ccfc13, 0xe9141f72150dda5f, 0xa43273d143e08a43,
                0xe99cfbfa3c6c6ad5, 0x9c79062ab5592dcf, 0x4df4a40c0f04e044, 0x52dfa998e2dcd87a, 0x747314f727da3567,
                0x5514e3fa427ea9cc, 0x4da2a99d17b80a97, 0x1d399dae1e0500f7, 0x48c22762a4b945c2, 0x90cc863a18f4d095,
                0xfbc1d454dc9bd7a9, 0x5f20c7f4035e7272, 0x44169fc6292aa860, 0x8bfa97163371ffad, 0x0e4293b569cb4d08,
                0xb859b978360af10a, 0x07f6a9aab3b807c8, 0x010aaf41927d14ee, 0xd79589856a744970, 0x6baf36d0195c5065,
                0xb810cc0627c17bda, 0x53a7dac8761bfde4, 0x4f754280adde73a9, 0xc4220c627d177648, 0xa5b40676ec727b56,
                0x188beac8841560f9, 0x5efc63712cb61fcc, 0xe08979b6232ed50c, 0xcc3cb06cd0d7bebd, 0x5d3aba8695f9b103,
                0x8b585f9d522fa83a, 0xaf7abb7baf0a4de8, 0xc1485c2ad8193e3a, 0x920d19b1f34ff0c5, 0xda68cfd7d17036ae,
                0xa4bf1ca38d152d89, 0xb7f8901716ce010b, 0xa07214f75395d901, 0x752d62b076fd1fd2, 0x997e7d2bbc6fb233,
                0x32c2642bf5bba727, 0xb7565ca330a2f23a, 0xcf30d62aa921e747, 0xc35e14e2fe9d2f9b, 0xf041eb4dce789e7f,
                0x1ede4e7bb4ef9106, 0x4c7037fd27336b12, 0xb3cc7c62a252d53f, 0x4eaa8a8db5134ba5, 0x386609a59b8caad9,
                0x24572e5c57a6a540, 0x952f057e75b2d9ea, 0xc451a53b0896ad06, 0xced825ff76147b56, 0x157888dbbff3c714,
                0xfe327864760f6a02, 0xe14812fbf2c9f4bd, 0x2681e1b1ee53bae9, 0x9cd66fd424ab5257, 0x2d75400927ec4c76,
                0x99f40c24efe79cce, 0x76cdc64580b62737, 0xf10725bd0cfe4778, 0x77fb36ae946625df, 0x77ef10083f683fd0,
                0x05eeb4a1ef54449e,
            },
            {
                0x2a56217db4e684e9, 0xefd9be7c8ed05934, 0xd5bffad1bd8f125c, 0x03fb5363e1edfb14, 0x1cc6cc64c4d0ae1f,
                0x0a42031ee4292ddc, 0xf301f9059c9f18df, 0x1af5471bd5244a32, 0x7c7ea2e05b9caf0d, 0xfd14b6b8f328dbbc,
                0x3c8434c59f2ac51c, 0xb2c4539718146cf3, 0x7c57e402dfa9d30b, 0xe32ea9139bbaa8fb, 0xec7fe2183e1047e1,
                0x8eb981ad72a62dd6, 0x433a2771754ec560, 0x5b4c387fe5449a87, 0x25090e65b33c7fa5, 0xfc479b37a179ebb8,
                0xd4bdfa34464c1ef3, 0x5e561efcb0eed040, 0x39a48202df8f4c7f, 0x598f1a20ab01585e, 0xaa2d4a27ec04a943,
                0xf8c313551fe9b622, 0x4ae1ceea76c17a7c, 0x6fe875a68dcc86e2, 0x447ad01c13e542c1, 0xe400f20550810995,
                0x96d49f014cd98fca, 0x5ed385299c7b4dfe, 0xf76b155e53afbfeb, 0x5e04d2ee1866a1d3, 0x7f2947d0cc9fbdaf,
                0x32cd48c6924917b2, 0xeda920681759dac8, 0xf128a276a5875d34, 0x7b61d4508840d642, 0x56fcc9b9ec0c5b19,
                0x286ee601e0677a6b, 0xc2d38be64e18d90e, 0x97b0f73b230f6c6b, 0xe51c164458c48dab, 0x5e8c90d219b3f05a,
                0xd4f9042b2a886a9c, 0x4973f70976bec492, 0x8d125fc3576e1f4a, 0x7d04238d15b03dbc, 0x1ad3e0fb8cbf01ac,
                0x552889b6f89b226f, 0xd3be2ccfe71bad98, 0x194c18a44225502b, 0x3c1342293f7d8758, 0x3c55a71842d822e7,
                0x429ef302612a85f5, 0x8778fef06d83323c, 0xd4a6dccde826dc6f, 0xd08961f8bcd0aae7, 0x8f7bddf23e6e5943,
                0x336b311a93c6c5e1, 0x0ca66bd597df1ea0, 0x305b2bd1379da391, 0x44ba8703ddaa9264, 0x19df8550c98bb110,
                0x654e939254612508, 0x86c299ab6ab25a0e, 0x7b0890ae521e8533, 0xbc43e5ab2fe50a30, 0xd92ea375251cc568,
                0x148e8f2917940c46, 0x6cd9dc6ff544fe7e, 0x8b1e28ab642e561f, 0x660776023bc2cf22, 0x34c2dfa5b20f690e,
                0x8696dcd5d31e8443, 0xcc4fab161a9769f9, 0x1666e5268191f4b8, 0x190f9860f9715459, 0x9b96062d8ace3903,
                0xf9769277ef1e35cc, 0x84da71550dd5e12e, 0x28e9114a9d82f629, 0xf0123e1a100c2b32, 0xca7b2c16287ff884,
                0x9e033549734d526c, 0x1047bdb6d289b437, 0x7d5e90dbb8dbfdb8, 0xe7f5db555bea587a, 0xe0813e5683b7c932,
                0x7f0cf9dec9364053, 0x4b51e815d18ba7b0, 0x5442aae2b67f3f75, 0x3e016e48f53685a7, 0x418b19bf6b7dae9a,
                0xa5cd71e10d2b07bf, 0x77910d9f7ea5986f, 0xbca898a323103afb, 0x9a1a705c170bc1c6, 0xd3af2645987bf970,
                0x01e992f0723bf52f, 0x587600e55581ac01, 0x67d16733b2effc05, 0x4d9b139f2edbb511, 0x7254a7b70ccbd629,
                0xeb34ca5036cbdca7, 0xef01c6006ea8deb2, 0x52825250d2fbae30, 0x783058f42afbe1d5, 0x5c8777aa43452402,
                0xfadb9609e3097035, 0x26a60ccf9c9a6bcc, 0x9c0f764a411520a8, 0xa452a6ef30600db1, 0x07ff2a1a7dd635b7,
                0x7421a0fccb49019a, 0xcd366f636226d1a9, 0xc3789659bc65500b, 0x7eadf2fc78431397, 0x39ac21bfaa8fb1c1,
                0x1bd1044a1056e136, 0xd8d03bf161c45d8b, 0xa511107ae70ef91f, 0x4578577bf3401ad7, 0x7c747dd3185ac6f9,
                0xae0f70b1e3d09eaf, 0x34315f4df64ec30f, 0x18b14e9991d7e75d, 0x2fb793a55cc3d6a1, 0xdeec732db958e157,
                0x14822e15ce378be5, 0xa7890eb4399904ff, 0x03635ac694453df4, 0x3db6ebf58882a657, 0xb48e4695b2ba4450,
                0xa3737c721d865244, 0x02d14362965a4abf, 0xff08ef4806d880aa, 0x75325bbce2624d55, 0x25c08941a17f15a1,
                0x5a9bda82aedf1631, 0x86a6454b0ad89de0, 0x3431e81e69f3272f, 0xedd40f8308cf1760, 0xe02e37d2a28bf538,
                0xe9a937eef9393b02, 0x1624ce92539df4e8, 0xabd1b62b1f30ee74, 0xa9cbcba7fa08005f, 0x0cb2ef53a66f43e9,
                0x9e4380060bca9250, 0x8d0e1a93db1ce300, 0x38c355289c9e2599, 0x505e684e514832c3, 0x4a2e806b8d273941,
                0x9cbeddcbffbe7c68, 0xdc5679abed54d763, 0x415002250f8db2f2, 0x7068cc805ee3fcc2, 0x331e549068e34679,
                0x2cf7b17f7c1c1429, 0xf56b5b8e160e6f39, 0xa1a52ce0472d370a, 0xa019766ba1516e03, 0x50d65e1a44c81bc7,
                0x2b821396f26b4f8b, 0xca68a52ad5b4d3ee, 0x5b8c38f8751ed2a1, 0x5d7f679a79227e10, 0x554296b41e0465aa,
                0x9c93098b0649659b, 0x6e4b0c5f0b38c0f0, 0xb98a0ed0d12d0683, 0x3df67a148a059c8b, 0xc540796f70d1da65,
                0x30e979d9a14cf535, 0x2a01c7abbdddcd67, 0x56b9828e1afab21a, 0x8ba914926bf7b789, 0x73ce0e23ec8277aa,
                0x88946c2ed16236bb, 0x78b3f48c210ff713, 0x353f05a0cd499072, 0x4ca2b54469718a85, 0x7d49332ae3aa820c,
                0xf91dc24e6b26d0a9, 0xa44f11be64887621, 0x350a66d62dcdc0ef, 0x0aef379c4bc78b26, 0x8db49dc3b772f90c,
                0x1a6fb16f766f8569, 0xa5e6512c63882797, 0xd20a287e698666e4, 0x67368df4deee5bff, 0x4712556de85489d5,
                0x9e7b838a28c71245, 0xe66f3915761df924, 0x451318769b9f2e7b, 0x7a3fdc9968ed3682, 0x6983aae7ce79736e,
                0x38d4d5317189ddef, 0x599cf389921caf79, 0xc79bdbb20f0a655d, 0x4f8b4bcce704f85a, 0xc348cd9a28c98ff6,
                0x6e320be1b5a41d45, 0x8fef58da19bc2f6a, 0x9194b77af87e49e0, 0xc67e195b0c368ea6, 0xad14b1accc6c322f,
                0x668645f2798545a7, 0xf6e30036843fcc30, 0x886410567bbc5d8f, 0x818d5b833ca5f60c, 0xafb2568c92411155,
                0x7d14aaf154c7e21e, 0x3bc86219a3d8bbc7, 0xc10ebbc3b69e9412, 0xf7b5f7ff0c119182, 0x03bc3b8a327daa8f,
                0xcba179fe94ffbabf, 0x59ab5d718f499bac, 0xae52c530c3d2ccd4, 0x9542aaa796d65f7b, 0x82676e2203703ed6,
                0xd42ec1caa36c91a6, 0xf998d9cb425d1bea, 0x2ea9111019bfde4c, 0x47c73357f1537f18, 0xbecb28ccb3d5ad74,
                0x2ee54b2939756235, 0x926750d275c4ca4a, 0xb0876ebdf17df92f, 0x7e46ab036486d51f, 0xf82281750609d41b,
                0xb4bd4ce265f7acce, 0x964c14b31bc4ff70, 0x6d164994e041a402, 0xadfdaeb0d3f5ae7a, 0xce2165e87f4d6f83,
                0x6847c6a461ecab8f, 0x81d0538a06450fa7, 0xa421f73ef5306975, 0xd5be7ce8e0cd329e, 0x7dba1cc002b8b151,
                0xec22e7c23e5f55c8, 0x9c9d1fb4ec0e97e5, 0x49f970017912e3a0, 0xaed66e59557a741a, 0x0d0d16ee28d6c13d,
                0x66af528b4dd156ba, 0xe14636d09a64c4c7, 0xd9aefe8a0b2b52bc, 0x52d78a74baf8f8db, 0x195318f6527513e3,
                0xa3506be0fd193599,
            },
            {
                0x693c954182b4741c, 0x68f2b504c79f50f4, 0x9ea2d3fcd31219be, 0xdeb379709610abaf, 0x252c789c065bc767,
                0xd53f8b1079a53490, 0xf9a295c7237f72d4, 0x16b079440a1ee6e5, 0x95b90c538974ec37, 0x9f7e700f6115fc58,
                0x226cae7b140ad1d5, 0x079694f207e3eeb5, 0xb5d9ca4428146a90, 0x602e940309e3812b, 0x9c997f99ee478ab5,
                0x57ce949fa068ed62, 0x2bceddb522fb218e, 0xf6cda5f368302b3b, 0xbc3415845cb3f0ec, 0x6b6b6c1124c65f20,
                0xa70c0cf3a07fddf2, 0xa82d3063e783ecee, 0xe45f2bcb8cc21ecf, 0x31a06bb1c2c4c87b, 0x10cb087ac4218670,
                0xcee2cfc8aab38285, 0x32dc35d2c9e66a9c, 0x54d190bb546a7407, 0xab83af6298bfbd45, 0x3299f79be4d69bd5,
                0x4232803bab2cbc3c, 0x365fc10f694dadc7, 0xab6461f35ca74fd7, 0x4d79db0109543f2d, 0xc20a65a40c1a7c75,
                0xc480935ea03366f2, 0x69051ec1905745de, 0xc434caaf41d67fb4, 0x43600d4b75af4651, 0x26a0263f6670d3bf,
                0x22a56bf53b14e42f, 0x27903ae759cd1bf0, 0x4d93a0ffe7823e59, 0x51ba7d675e503778, 0x31d1591bc39e1ee6,
                0x47abcfb03f7fbe6a, 0xc374032ffbb2529d, 0xc947a9a6d22c8a77, 0x4cbdb8657757deb6, 0x3ddac2d6494105e6,
                0x6a71d630068b6937, 0xe91d52f7c517c4a0, 0xca5b5b34e3d39a30, 0xe85aebe2a54f3b0c, 0xa6c6e4855b9bc019,
                0x180684b73cf3bf1c, 0x899c91feca383c61, 0x2a86d2bbc21d072c, 0xe360fc3e5090918e, 0x7fe7617c889df4cd,
                0xf623d4c6855984bc, 0x853e73111dd8b7a1, 0x44019810ef8f4e5d, 0xa3a3da1cb9b0d365, 0x623d252bfad4920e,
                0xd0a7e63d2c3c1716, 0x0ba0707098019a41, 0x5e53c2e2c983c23e, 0x450b52be3d49d69e, 0x9ab447f195a55292,
                0x102d78a093d7f1df, 0x3f7c94cf01499b19, 0x33e76d65080e3507, 0x8359da72871602eb, 0x5ef38d12d198cd49,
                0x086093b0a719e03e, 0x79d43fc327975daa, 0x40603f81e5322103, 0xa3d0c309f83d6668, 0x7bd49ef8dab4bf13,
                0xe8c42b598fbc95d0, 0x40e3f510fd4ee26f, 0xfc2336b4a41d7551, 0x4f05c2ce8cfdec8e, 0xac2751b38bea1d1c,
                0xe144cb59ecf184ef, 0x2d9b520a145c8c16, 0x5406f27989bc96f6, 0x23852a7bae978b47, 0xd524b42073f547c0,
                0x576b9e7e58ceb7bc, 0xd9fa306c26bc5f59, 0x98fb2ccfdb313b82, 0xdd9af8268276b52e, 0x0ceb16a78c1976ce,
                0x36af4961f8495a94, 0x73806309402da06b, 0x95039f7c76076df2, 0x6f7e82fae9851c79, 0x109333447490d749,
                0x1f7b22431e0def0f, 0x7dc4611f05707f17, 0x5813730f34eefad7, 0xdb1fcb04572ae4a2, 0x407d37ae2379c94d,
                0x4954946ac085f469, 0xf458acbf19e6c1dc, 0x2be788825f25be5c, 0xb0a0c5df6f9288f4, 0x6ea1bbd73d1ae82e,
                0x6d5e97db179b28b9, 0x75316e71e46cd0e8, 0x45e01681319d4af7, 0x6040c6b875672aac, 0x7410ff82c09dcd91,
                0x7d0089bcb5a48794, 0xa523a1c12fa0802c, 0xc706705b526a1e20, 0xd9f04815c0a6456c, 0x5f4636135077785e,
                0x739dc5f1763648ba, 0x7904fcb8dd9bc1f4, 0x1a0ada96d0e5c541, 0x536030bc09131cb2, 0x5ca7a3fd6cd28982,
                0x5a547f3cd3550cfe, 0x2ba4bb1c218f771f, 0x0ca35e7f151cf0c9, 0x7df42783cbb3c2a1, 0x6e595392af3971a3,
                0x58cec5b3c93fd809, 0xe0542bc9c4bf2efc, 0x0ed113ff0880017c, 0xcb83af5016ca1a5c, 0xe6bcdf6f4858fca3,
                0xc62a665e28b71345, 0x7a54581d47c2c867, 0x4caad9fc4b10e1f5, 0x40f2f726071d98d5, 0xc2359aa4ed087f16,
                0x9b9aec929bc2c626, 0xd8fd364849d25a77, 0x8c0c8dcb6f8737cc, 0x94a189e5e3927e4f, 0x27f8743006e6c34e,
                0xd5a7ee0aa548f683, 0x063e2b21d7d094e9, 0xd4213f38104a5537, 0x71b6218a8230c5ba, 0x10b229265323b4d2,
                0x22234374a2734482, 0x569bb9d4b9a86b85, 0xaab0651b418d3b46, 0x8571f8d5871bbf1a, 0x48f130a098213091,
                0xdf03437620ff0b3b, 0x3cecf68a577637e7, 0x6a696cfe69915e8b, 0x8dbeb76a195c4bce, 0x035186a897f2a454,
                0xfc2c666ace71b74d, 0xd8eb7d118a1f8f80, 0x83047174cd5de1e4, 0x23ba618cb482fa3d, 0x26898e552ea4231b,
                0xc04fe13b89da5b34, 0x0cfe71a23468a6ac, 0x58cafe5f25162c0b, 0xdcd2af6d1599e02f, 0x7935c18e94667b84,
                0x11816440f6daf103, 0xdb45ed1052ac7f5a, 0x7f800876907b2cf2, 0x47ad23232c79a575, 0x31c14a166b1ecfa1,
                0x2016835b15bc7dcb, 0xe36e6f0dc3163bf7, 0x8ac4fd23c1434c1e, 0xea8658a15b3dc7fe, 0x9e097194ae34823a,
                0x4c512342a1df82f3, 0xbc466d229b632dda, 0x6f58bf666deb6ac1, 0x1cf0ce404ac53c42, 0x0d7b776466bf4144,
                0xd11ec613cb7fd08e, 0xb41380a877ab9a37, 0x0c3f31d6ca1f15da, 0x5c4367d3b7153bb0, 0x008f82043cdb673b,
                0xcbc376c830250dfa, 0xd69657607aa62bf1, 0x76c76e0eb1d2e2a9, 0xbda67585c841a49a, 0x935a6956f82ea2d4,
                0xf2ca4947064cdf9a, 0x394ae4a2cd6c5e31, 0xffc5b5acf2dd77fd, 0xd2c1e4b30eb836e1, 0xa26050157ce432b6,
                0xb4118e6f65668938, 0x4ba5e3cedc2f33b9, 0x5320d4d82a7f3a52, 0x6500f53bfe4e40d4, 0x74b48ff4c216ff96,
                0x24c60a293dc8ea12, 0x8be1ac3d6efc61ae, 0x467684a7fdae0f6f, 0x8fe8c9a92a7f547b, 0x7824e120587a07b1,
                0x2fcec2a5b63c3bf4, 0x1b73d74a5e67ba18, 0xb4581385dc6d258e, 0xf45e65a2291cca42, 0xce06c8be6f931f25,
                0x83256e283e8982f7, 0xb9ae8d52823e0688, 0x6dbd2c9b6581a333, 0x84a84ee825e9ebab, 0x512149c44e3a5fa5,
                0x4080391afd375a5e, 0x1b56667fcc708c58, 0x758f5051d80f4990, 0x3c7252fd2f439ab6, 0x22c150a13f828513,
                0xfb4e2a09669176d2, 0x3762920064169341, 0xc78b23529c6bd59d, 0x76a6d00007ce11f8, 0x1ffd518cc0483314,
                0x20a6c708a2ec7bb3, 0x24b4ddd0f6d1b755, 0x298018f86e982755, 0xb0f82e23a1e85822, 0xecac046a868165c2,
                0x5fdf358d794a07c7, 0xb72398853a968370, 0x7418df33c3ff125e, 0x172bcb5300b3aab7, 0x2a4048baec8aeabe,
                0x7ea98016f7c85ec1, 0x1afc507dc66113a8, 0xde2cbcb467b19ba1, 0xf5a60fe30f9e61e5, 0x25cccf868b6b0e3b,
                0xce8a8992473f0d92, 0x2bed9615c305de79, 0x99cd4a3a30b13dcd, 0x264a0526e87720ef, 0x7c7b33cb6639c9af,
                0x8b19555ad810fc7c, 0xdac5e39d6f01a264, 0x6fabe6f54bfe61cf, 0xee542526eaea7792, 0xf7feffe02cab8d9f,
                0xc21130a67aaf401f,
            },
            {
                0x8c9519953a0631c2, 0x865d637bb43b22be, 0xacb65a491f886c05, 0xfc4d38f8192d9757, 0x8d395f9f6dc42f0c,
                0x53087dcadb04465e, 0x5a986f1d06d0fdd7, 0x60ecef2322d82ca6, 0xecb5726c2d321cf0, 0xc35d5a4bef2702b1,
                0x5dd2b609c322b708, 0xbcf3b95dc3d729ca, 0x1d9170b9e1453366, 0x186553051a4e25be, 0x2a53d48fb82f77ee,
                0xf46c760a61c999c1, 0xd0cc0a9782d8fb6c, 0x527f4ba4e3b5aeaf, 0x4bbe8f93317c3443, 0x045620b59ed19311,
                0xc7b0c55124f293df, 0x4109b240dd664668, 0xe3eb86d84935282e, 0xdb084f7c80268067, 0x77453a7266c1d524,
                0x64f313320afdd17e, 0x8bae4a0bda3e3e66, 0x2de67df4d33aa930, 0xa3068a663be947d2, 0xcea196429334afa7,
                0xe7e0cea87cfab510, 0x741c10a48562fa44, 0x14494d5a04a6c202, 0x40e7a06890d50be4, 0xe1aa5f52250a65b5,
                0xf4a25f6b7e5a6194, 0x565c00e7636c7c52, 0xf75c7fb672be9b3f, 0xe6afa72a659ee1bf, 0x6b64f6e131206bbc,
                0xc890512940bbd56f, 0x2534086dc1a69681, 0xb00843f54690b56e, 0xcc5516083f0bdb0c, 0xa95c2e34fdc988c9,
                0x849b826becbbf7f3, 0xebeb852f8b802332, 0x961f2b23e86a263c, 0xd72a88270464d6f2, 0x6763243e613eeff2,
                0x254cdfdd48b072a1, 0xac3ab9855e94f5f0, 0x9fb7af5dbfd5dbb9, 0xc94e6163f1c0a8cf, 0xf288005dd9ade2e1,
                0x87ef56d822a9de2d, 0x8ecab7bed5a7e85e, 0x83ea1bcff015e925, 0x1271137d38e2ff89, 0xa27a077e369d6a32,
                0xb9f8913c07ce52b7, 0x5fe99b17468419b0, 0x9fdd39945a89b502, 0x794bc51777290510, 0xe852021031f3314c,
                0xf67cff6fd964bac3, 0x7ac582022256c0c0, 0x77604f6943584c22, 0x323a2ce1c1306628, 0x08dd88c98350de11,
                0xb8342e4f577818ca, 0xa5bc1d61a6d047c1, 0xddaa89ea44bbbf88, 0xc1a542d3e1620265, 0x8b764ba499a45119,
                0x22d9c1904111885d, 0x101ae7d9ebba120f, 0x82ca6f603e96e10e, 0xd05317bd6f339e88, 0x54a4848959d20898,
                0x00b0acad87d94f8e, 0xf2ab9bac6fdc358c, 0x95ca172cb6bf18f7, 0x08291eaf145b6f9c, 0xa485afdec21bf027,
                0x2c25fdf6dc219361, 0x1a5318cefb7c2036, 0xcfcfafedd3ab62d8, 0x4c7ed76ef4b8ccd6, 0x473466ac0ac16385,
                0x2c04553a07032b8a, 0xd31e4a32538843c7, 0x12dd65e138f6644d, 0x161130caf6577e8b, 0x63e5b4ff2c5441b1,
                0xa8b0a84d06a58ed6, 0x125dac5910d5ea2b, 0xa97c12ae44459255, 0xb3cc802562a5fd6c, 0x4409c1575626adaa,
                0xa067d92c8e8b8dbb, 0xfededf949bad949a, 0xb9d172a91a3a9009, 0x2ec5d64517692699, 0xc8412695f56eb641,
                0x7ad115e4cb00eddd, 0x32c2242ed438b420, 0xa429ceede7138969, 0x7f6db9822cf384e9, 0x2dc39b0bca1f2932,
                0xde92a487585eaca6, 0x87c87d0858a70e1e, 0xd2419508213e36f4, 0x18bbfbb774311ea8, 0xb625e058422964c0,
                0x4334b1e764f683ad, 0xde817e4a6a1f7368, 0x39329302a8c2c2a9, 0xaa4c1e6c6f785843, 0x8dc7ef86d9d2d0f7,
                0x597965f6f249653a, 0xff346827822aa645, 0x368a3637d29d39f2, 0x93775a1b92fc4b06, 0x310b2ad79909ac04,
                0x689a60e616b90265, 0x85e4b485dab67c13, 0xd90f3f8b9d54d5d5, 0x7e14be6743d94267, 0x48faebe7430aef01,
                0xae5b11c885ecdfb5, 0x52eb6c5cef5587c3, 0x17023e5091ba5663, 0x5bdd13b983a987aa, 0x8a398958b5466a69,
                0x6dc644518b81ab09, 0xfb4bb0dd2d533832, 0x1d4baa7ded0c80ed, 0x97d94a33f3f1dc18, 0x4eaa82471d6d2466,
                0xdc80226d2e5a87b8, 0x6e7946ed519df314, 0x5e78f110eb7b9c3d, 0xae1445dc0b05b119, 0xaf3a2b4ad6a7e4f3,
                0x2b3fab4aff638a6f, 0x482ee0a0ee944717, 0x6c638acb12e6ef9a, 0xf59ffb896ccd4720, 0x4c3e1c2d1a151201,
                0xb6f9b34cec313119, 0x4c44261e36facdd5, 0xc59d9c212c3ea06c, 0x9388664dc1d30ca2, 0x29b9d9b5eabffb2c,
                0x7808d154b0ec1b3a, 0xc4aaf60b740308fa, 0x6f7aca6f0741b2b5, 0x2edf4f889c09621d, 0x1fa820ee83261292,
                0x7a162fd7a082003c, 0x870c2de2f130ff5a, 0xc32d6a90658eb347, 0x0b5a6e242082dc01, 0xf15db35e61e3e4f9,
                0x05cb98edaeddde3b, 0xf42a568ba8650943, 0x7a70bb17d9251446, 0x1192a280cb3f6e41, 0x1e793c64522c4580,
                0x29a80c9474c9b619, 0x4ccd8e988ea18381, 0xb8838a1136fbfc48, 0x108a0978ea3b5737, 0xe8d5e36bf6eebbc7,
                0x62b9f51dbf53b7e6, 0x6996fcba08c05853, 0x500713ada63dec92, 0x55e48c824b0341da, 0xa76a0dee1b82ae93,
                0x95706b1d4d8ec8ec, 0x2b100b833913b0a5, 0x2dcc7ccc931f4a69, 0xf198714708999d3e, 0x36d8a256f6be869f,
                0x74791453a9159591, 0x3e0e78593608f415, 0xc18f5072b7288cc1, 0x1e045ab5b4deeb8b, 0x55ce3a1e68cc7318,
                0xadb069bc0ea75ce6, 0x5f0c72e82faa5d12, 0xb0dcec4f4a5b32eb, 0xb2af1e588a6b3d69, 0x7133d52a1259083f,
                0xaf5cbeb8c7d7b228, 0xe00640c93ba3c4e0, 0x5264e4f06b24e751, 0xf4b8ad26a7e50c2d, 0x034dcdb8121f9b91,
                0x8d18c62ff6c0b2c9, 0x04c378dbb5c92962, 0x7c7c041b704a39bb, 0x8c7cac2bd6fab046, 0xe830751a18b1fb5e,
                0x9e30fb31b333ccec, 0x4fba9d374256093e, 0x598628d4b2871fde, 0xd6854917cc217ab4, 0x4da3839966d614cd,
                0x6c2ee98d9f0a6bf9, 0xa643c8991753f6e4, 0x4a7982d0be1d0930, 0x441b590a0694d4f4, 0xac70c5107d531b97,
                0xbb9e36477a76bbd2, 0x921ccfb831039d8e, 0x61f6991dc827545c, 0x6c5afe13298cf2ad, 0xecf28b9022ab3a75,
                0x11e0265d86c2d913, 0x51b4aeded81317ec, 0x5bdccaa59f2cbeb8, 0xb9c76e9f66388e78, 0xa6babe827af99e38,
                0x7c92e55ca21c6159, 0xe49bad2924782213, 0x3c2f72423ad5f50d, 0xb3755cfa70e505b4, 0x9f55bc675f2dd8d0,
                0xf2891d2b3c912007, 0xbfe5cf184e166eff, 0x0e43e71fdd72d966, 0xf56228bcd5c95ca0, 0x80fa47660411d1a6,
                0x92166503e32b5c2f, 0x542096f618073022, 0x5dd3a8ea782205c5, 0xb520095d8dff2a5e, 0x045b81afc2f56ade,
                0xb85681ed6f1de692, 0xb9a75fdde941cf34, 0x58e17def17bb5d6b, 0xd4b11a833adbd178, 0x787ab0355e2fda17,
                0x38e5bda322c1a58a, 0x0c1bf5f6457d6d33, 0x93172c3a82e1c498, 0xf3d6f541b2b86965, 0x4e7f9e55316d0a31,
                0xc2e824b016aab50b, 0x1d6c62558ea1c109, 0x5370f2b9133e09ee, 0x43137d4fa9a8437f, 0xe5239ad79830662b,
                0x4e109cd3220f67dd,
            },
        };
        size_t res = 0;

        for (size_t i = 0; i < 8; ++i)
        {
            res ^= random[i][UInt8(x)];
            x >>= 8;
        }

        return res;
    }
};
} // namespace Hashes


template <template <typename...> class Map, typename Hash>
void NO_INLINE test(const Key * data, size_t size, std::function<void(Map<Key, Value, Hash> &)> init = {})
{
    Stopwatch watch;

    Map<Key, Value, Hash> map;
    if (init)
        init(map);

    for (auto end = data + size; data < end; ++data)
        ++map[*data];

    watch.stop();
    std::cerr << __PRETTY_FUNCTION__ << ":\nElapsed: " << watch.elapsedSeconds() << " ("
              << size / watch.elapsedSeconds() << " elem/sec.)"
              << ", map size: " << map.size() << "\n";
}

template <template <typename...> class Map, typename Init>
void NO_INLINE testForEachHash(const Key * data, size_t size, Init && init)
{
    test<Map, Hashes::IdentityHash>(data, size, init);
    test<Map, Hashes::SimpleMultiplyHash>(data, size, init);
    test<Map, Hashes::MultiplyAndMixHash>(data, size, init);
    test<Map, Hashes::MixMultiplyMixHash>(data, size, init);
    test<Map, Hashes::MurMurMixHash>(data, size, init);
    test<Map, Hashes::MixAllBitsHash>(data, size, init);
    test<Map, Hashes::IntHash32>(data, size, init);
    test<Map, Hashes::ArcadiaNumericHash>(data, size, init);
    test<Map, Hashes::MurMurButDifferentHash>(data, size, init);
    test<Map, Hashes::TwoRoundsTwoVarsHash>(data, size, init);
    test<Map, Hashes::TwoRoundsLessOpsHash>(data, size, init);
    test<Map, Hashes::CRC32Hash>(data, size, init);
    test<Map, Hashes::MulShiftHash>(data, size, init);
    test<Map, Hashes::TabulationHash>(data, size, init);
    test<Map, Hashes::CityHash>(data, size, init);
    test<Map, Hashes::SipHash>(data, size, init);
}

void NO_INLINE testForEachMapAndHash(const Key * data, size_t size)
{
    auto nothing = [](auto &) {
    };

    testForEachHash<HashMap>(data, size, nothing);
    testForEachHash<std::unordered_map>(data, size, nothing);
    testForEachHash<google::dense_hash_map>(data, size, [](auto & map) { map.set_empty_key(-1); });
    testForEachHash<google::sparse_hash_map>(data, size, nothing);
}


int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: program n\n";
        return 1;
    }

    size_t n = atoi(argv[1]);
    //    size_t m = atoi(argv[2]);

    std::cerr << std::fixed << std::setprecision(3);

    std::vector<Key> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        in2.readStrict(reinterpret_cast<char *>(&data[0]), sizeof(data[0]) * n);

        watch.stop();
        std::cerr << "Vector. Size: " << n << ", elapsed: " << watch.elapsedSeconds() << " ("
                  << n / watch.elapsedSeconds() << " elem/sec.)" << std::endl;
    }

    /** Actually we should not run multiple test within same invocation of binary,
      *  because order of test could alter test results (due to state of allocator and various minor reasons),
      *  but in this case it's Ok.
      */

    testForEachMapAndHash(data.data(), data.size());
    return 0;
}
