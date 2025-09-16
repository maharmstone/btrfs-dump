module;

#include <stdint.h>
#include <array>
#include <format>
#include <chrono>

export module cxxbtrfs;

using namespace std;

static const uint32_t crctable[] = {
    0x00000000, 0xf26b8303, 0xe13b70f7, 0x1350f3f4, 0xc79a971f, 0x35f1141c, 0x26a1e7e8, 0xd4ca64eb,
    0x8ad958cf, 0x78b2dbcc, 0x6be22838, 0x9989ab3b, 0x4d43cfd0, 0xbf284cd3, 0xac78bf27, 0x5e133c24,
    0x105ec76f, 0xe235446c, 0xf165b798, 0x030e349b, 0xd7c45070, 0x25afd373, 0x36ff2087, 0xc494a384,
    0x9a879fa0, 0x68ec1ca3, 0x7bbcef57, 0x89d76c54, 0x5d1d08bf, 0xaf768bbc, 0xbc267848, 0x4e4dfb4b,
    0x20bd8ede, 0xd2d60ddd, 0xc186fe29, 0x33ed7d2a, 0xe72719c1, 0x154c9ac2, 0x061c6936, 0xf477ea35,
    0xaa64d611, 0x580f5512, 0x4b5fa6e6, 0xb93425e5, 0x6dfe410e, 0x9f95c20d, 0x8cc531f9, 0x7eaeb2fa,
    0x30e349b1, 0xc288cab2, 0xd1d83946, 0x23b3ba45, 0xf779deae, 0x05125dad, 0x1642ae59, 0xe4292d5a,
    0xba3a117e, 0x4851927d, 0x5b016189, 0xa96ae28a, 0x7da08661, 0x8fcb0562, 0x9c9bf696, 0x6ef07595,
    0x417b1dbc, 0xb3109ebf, 0xa0406d4b, 0x522bee48, 0x86e18aa3, 0x748a09a0, 0x67dafa54, 0x95b17957,
    0xcba24573, 0x39c9c670, 0x2a993584, 0xd8f2b687, 0x0c38d26c, 0xfe53516f, 0xed03a29b, 0x1f682198,
    0x5125dad3, 0xa34e59d0, 0xb01eaa24, 0x42752927, 0x96bf4dcc, 0x64d4cecf, 0x77843d3b, 0x85efbe38,
    0xdbfc821c, 0x2997011f, 0x3ac7f2eb, 0xc8ac71e8, 0x1c661503, 0xee0d9600, 0xfd5d65f4, 0x0f36e6f7,
    0x61c69362, 0x93ad1061, 0x80fde395, 0x72966096, 0xa65c047d, 0x5437877e, 0x4767748a, 0xb50cf789,
    0xeb1fcbad, 0x197448ae, 0x0a24bb5a, 0xf84f3859, 0x2c855cb2, 0xdeeedfb1, 0xcdbe2c45, 0x3fd5af46,
    0x7198540d, 0x83f3d70e, 0x90a324fa, 0x62c8a7f9, 0xb602c312, 0x44694011, 0x5739b3e5, 0xa55230e6,
    0xfb410cc2, 0x092a8fc1, 0x1a7a7c35, 0xe811ff36, 0x3cdb9bdd, 0xceb018de, 0xdde0eb2a, 0x2f8b6829,
    0x82f63b78, 0x709db87b, 0x63cd4b8f, 0x91a6c88c, 0x456cac67, 0xb7072f64, 0xa457dc90, 0x563c5f93,
    0x082f63b7, 0xfa44e0b4, 0xe9141340, 0x1b7f9043, 0xcfb5f4a8, 0x3dde77ab, 0x2e8e845f, 0xdce5075c,
    0x92a8fc17, 0x60c37f14, 0x73938ce0, 0x81f80fe3, 0x55326b08, 0xa759e80b, 0xb4091bff, 0x466298fc,
    0x1871a4d8, 0xea1a27db, 0xf94ad42f, 0x0b21572c, 0xdfeb33c7, 0x2d80b0c4, 0x3ed04330, 0xccbbc033,
    0xa24bb5a6, 0x502036a5, 0x4370c551, 0xb11b4652, 0x65d122b9, 0x97baa1ba, 0x84ea524e, 0x7681d14d,
    0x2892ed69, 0xdaf96e6a, 0xc9a99d9e, 0x3bc21e9d, 0xef087a76, 0x1d63f975, 0x0e330a81, 0xfc588982,
    0xb21572c9, 0x407ef1ca, 0x532e023e, 0xa145813d, 0x758fe5d6, 0x87e466d5, 0x94b49521, 0x66df1622,
    0x38cc2a06, 0xcaa7a905, 0xd9f75af1, 0x2b9cd9f2, 0xff56bd19, 0x0d3d3e1a, 0x1e6dcdee, 0xec064eed,
    0xc38d26c4, 0x31e6a5c7, 0x22b65633, 0xd0ddd530, 0x0417b1db, 0xf67c32d8, 0xe52cc12c, 0x1747422f,
    0x49547e0b, 0xbb3ffd08, 0xa86f0efc, 0x5a048dff, 0x8ecee914, 0x7ca56a17, 0x6ff599e3, 0x9d9e1ae0,
    0xd3d3e1ab, 0x21b862a8, 0x32e8915c, 0xc083125f, 0x144976b4, 0xe622f5b7, 0xf5720643, 0x07198540,
    0x590ab964, 0xab613a67, 0xb831c993, 0x4a5a4a90, 0x9e902e7b, 0x6cfbad78, 0x7fab5e8c, 0x8dc0dd8f,
    0xe330a81a, 0x115b2b19, 0x020bd8ed, 0xf0605bee, 0x24aa3f05, 0xd6c1bc06, 0xc5914ff2, 0x37faccf1,
    0x69e9f0d5, 0x9b8273d6, 0x88d28022, 0x7ab90321, 0xae7367ca, 0x5c18e4c9, 0x4f48173d, 0xbd23943e,
    0xf36e6f75, 0x0105ec76, 0x12551f82, 0xe03e9c81, 0x34f4f86a, 0xc69f7b69, 0xd5cf889d, 0x27a40b9e,
    0x79b737ba, 0x8bdcb4b9, 0x988c474d, 0x6ae7c44e, 0xbe2da0a5, 0x4c4623a6, 0x5f16d052, 0xad7d5351,
};

static uint32_t calc_crc32c(uint32_t seed, span<const uint8_t> msg) {
    uint32_t rem = seed;

    for (auto b : msg) {
        rem = crctable[(rem ^ b) & 0xff] ^ (rem >> 8);
    }

    return rem;
}

template<integral T>
class little_endian {
public:
    little_endian() = default;

    constexpr little_endian(T t) {
        for (unsigned int i = 0; i < sizeof(T); i++) {
            val[i] = t & 0xff;
            t >>= 8;
        }
    }

    constexpr operator T() const {
        T t = 0;

#pragma GCC unroll 8
        for (unsigned int i = 0; i < sizeof(T); i++) {
            t <<= 8;
            t |= val[sizeof(T) - i - 1];
        }

        return t;
    }

    little_endian<T>& operator=(T t) {
        for (unsigned int i = 0; i < sizeof(T); i++) {
            val[i] = t & 0xff;
            t >>= 8;
        }

        return *this;
    }

private:
    uint8_t val[sizeof(T)];
} __attribute__((packed));

template<integral T>
struct std::formatter<little_endian<T>> {
    constexpr auto parse(format_parse_context& ctx) {
        std::formatter<int> f;
        auto it = ctx.begin();
        auto ret = f.parse(ctx);

        fmt = "{:"s + std::string{std::string_view(it, ret - it)} + "}"s;

        return ret;
    }

    template<typename format_context>
    auto format(little_endian<T> t, format_context& ctx) const {
        auto num = (T)t;

        return std::vformat_to(ctx.out(), fmt, std::make_format_args(num));
    }

    std::string fmt;
};

export namespace btrfs {

using le16 = little_endian<uint16_t>;
using le32 = little_endian<uint32_t>;
using le64 = little_endian<uint64_t>;

constexpr uint64_t superblock_addrs[] = { 0x10000, 0x4000000, 0x4000000000, 0x4000000000000 };

constexpr uint64_t MAGIC = 0x4d5f53665248425f;

constexpr uint64_t FEATURE_INCOMPAT_MIXED_BACKREF = 1 << 0;
constexpr uint64_t FEATURE_INCOMPAT_DEFAULT_SUBVOL = 1 << 1;
constexpr uint64_t FEATURE_INCOMPAT_MIXED_GROUPS = 1 << 2;
constexpr uint64_t FEATURE_INCOMPAT_COMPRESS_LZO = 1 << 3;
constexpr uint64_t FEATURE_INCOMPAT_COMPRESS_ZSTD = 1 << 4;
constexpr uint64_t FEATURE_INCOMPAT_BIG_METADATA = 1 << 5;
constexpr uint64_t FEATURE_INCOMPAT_EXTENDED_IREF = 1 << 6;
constexpr uint64_t FEATURE_INCOMPAT_RAID56 = 1 << 7;
constexpr uint64_t FEATURE_INCOMPAT_SKINNY_METADATA = 1 << 8;
constexpr uint64_t FEATURE_INCOMPAT_NO_HOLES = 1 << 9;
constexpr uint64_t FEATURE_INCOMPAT_METADATA_UUID = 1 << 10;
constexpr uint64_t FEATURE_INCOMPAT_RAID1C34 = 1 << 11;
constexpr uint64_t FEATURE_INCOMPAT_ZONED = 1 << 12;
constexpr uint64_t FEATURE_INCOMPAT_EXTENT_TREE_V2 = 1 << 13;
constexpr uint64_t FEATURE_INCOMPAT_RAID_STRIPE_TREE = 1 << 14;
constexpr uint64_t FEATURE_INCOMPAT_SIMPLE_QUOTA = 1 << 16;
constexpr uint64_t FEATURE_INCOMPAT_REMAP_TREE = 1 << 17;

constexpr uint64_t FEATURE_COMPAT_RO_FREE_SPACE_TREE = 1 << 0;
constexpr uint64_t FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID = 1 << 1;
constexpr uint64_t FEATURE_COMPAT_RO_VERITY = 1 << 2;
constexpr uint64_t FEATURE_COMPAT_RO_BLOCK_GROUP_TREE = 1 << 3;

constexpr uint64_t HEADER_FLAG_WRITTEN = 1 << 0;
constexpr uint64_t HEADER_FLAG_RELOC = 1 << 1;
constexpr uint64_t HEADER_FLAG_MIXED_BACKREF = (uint64_t)1 << 56;
constexpr uint64_t SUPER_FLAG_ERROR = 1 << 2;
constexpr uint64_t SUPER_FLAG_SEEDING = (uint64_t)1 << 32;
constexpr uint64_t SUPER_FLAG_METADUMP = (uint64_t)1 << 33;
constexpr uint64_t SUPER_FLAG_METADUMP_V2 = (uint64_t)1 << 34;
constexpr uint64_t SUPER_FLAG_CHANGING_FSID = (uint64_t)1 << 35;
constexpr uint64_t SUPER_FLAG_CHANGING_FSID_V2 = (uint64_t)1 << 36;
constexpr uint64_t SUPER_FLAG_CHANGING_BG_TREE = (uint64_t)1 << 38;
constexpr uint64_t SUPER_FLAG_CHANGING_DATA_CSUM = (uint64_t)1 << 39;
constexpr uint64_t SUPER_FLAG_CHANGING_META_CSUM = (uint64_t)1 << 40;

constexpr uint64_t BLOCK_GROUP_DATA = 1 << 0;
constexpr uint64_t BLOCK_GROUP_SYSTEM = 1 << 1;
constexpr uint64_t BLOCK_GROUP_METADATA = 1 << 2;
constexpr uint64_t BLOCK_GROUP_RAID0 = 1 << 3;
constexpr uint64_t BLOCK_GROUP_RAID1 = 1 << 4;
constexpr uint64_t BLOCK_GROUP_DUP = 1 << 5;
constexpr uint64_t BLOCK_GROUP_RAID10 = 1 << 6;
constexpr uint64_t BLOCK_GROUP_RAID5 = 1 << 7;
constexpr uint64_t BLOCK_GROUP_RAID6 = 1 << 8;
constexpr uint64_t BLOCK_GROUP_RAID1C3 = 1 << 9;
constexpr uint64_t BLOCK_GROUP_RAID1C4 = 1 << 10;
constexpr uint64_t BLOCK_GROUP_REMAPPED = 1 << 11;
constexpr uint64_t BLOCK_GROUP_REMAP = 1 << 12;

constexpr uint64_t FIRST_CHUNK_TREE_OBJECTID = 0x100;

constexpr uint64_t DEV_STATS_OBJECTID = 0x0;
constexpr uint64_t ROOT_TREE_OBJECTID = 0x1;
constexpr uint64_t EXTENT_TREE_OBJECTID = 0x2;
constexpr uint64_t CHUNK_TREE_OBJECTID = 0x3;
constexpr uint64_t DEV_TREE_OBJECTID = 0x4;
constexpr uint64_t FS_TREE_OBJECTID = 0x5;
constexpr uint64_t ROOT_TREE_DIR_OBJECTID = 0x6;
constexpr uint64_t CSUM_TREE_OBJECTID = 0x7;
constexpr uint64_t UUID_TREE_OBJECTID = 0x9;
constexpr uint64_t FREE_SPACE_TREE_OBJECTID = 0xa;
constexpr uint64_t BLOCK_GROUP_TREE_OBJECTID = 0xb;
constexpr uint64_t RAID_STRIPE_TREE_OBJECTID = 0xc;
constexpr uint64_t REMAP_TREE_OBJECTID = 0xd;
constexpr uint64_t FREE_INO_OBJECTID = 0xfffffffffffffff4;
constexpr uint64_t FREE_SPACE_OBJECTID = 0xfffffffffffffff5;
constexpr uint64_t EXTENT_CSUM_OBJECTID = 0xfffffffffffffff6;
constexpr uint64_t DATA_RELOC_TREE_OBJECTID = 0xfffffffffffffff7;
constexpr uint64_t TREE_RELOC_OBJECTID = 0xfffffffffffffff8;
constexpr uint64_t TREE_LOG_FIXUP_OBJECTID = 0xfffffffffffffff9;
constexpr uint64_t TREE_LOG_OBJECTID = 0xfffffffffffffffa;
constexpr uint64_t ORPHAN_OBJECTID = 0xfffffffffffffffb;
constexpr uint64_t BALANCE_OBJECTID = 0xfffffffffffffffc;

constexpr uint64_t DEVICE_RANGE_RESERVED = 0x100000;

constexpr uint64_t INODE_NODATASUM = 1 << 0;
constexpr uint64_t INODE_NODATACOW = 1 << 1;
constexpr uint64_t INODE_READONLY = 1 << 2;
constexpr uint64_t INODE_NOCOMPRESS = 1 << 3;
constexpr uint64_t INODE_PREALLOC = 1 << 4;
constexpr uint64_t INODE_SYNC = 1 << 5;
constexpr uint64_t INODE_IMMUTABLE = 1 << 6;
constexpr uint64_t INODE_APPEND = 1 << 7;
constexpr uint64_t INODE_NODUMP = 1 << 8;
constexpr uint64_t INODE_NOATIME = 1 << 9;
constexpr uint64_t INODE_DIRSYNC = 1 << 10;
constexpr uint64_t INODE_COMPRESS = 1 << 11;
constexpr uint64_t INODE_ROOT_ITEM_INIT = (uint64_t)1 << 31;

constexpr uint64_t INODE_RO_VERITY = 1 << 0;

constexpr uint64_t FREE_SPACE_USING_BITMAPS = 1 << 0;

constexpr uint64_t EXTENT_FLAG_DATA = 1 << 0;
constexpr uint64_t EXTENT_FLAG_TREE_BLOCK = 1 << 1;
constexpr uint64_t BLOCK_FLAG_FULL_BACKREF = 1 << 8;

constexpr uint64_t ROOT_SUBVOL_DEAD = (uint64_t)1 << 48;

struct uuid {
    array<uint8_t, 16> uuid;
};

struct dev_item {
    le64 devid;
    le64 total_bytes;
    le64 bytes_used;
    le32 io_align;
    le32 io_width;
    le32 sector_size;
    le64 type;
    le64 generation;
    le64 start_offset;
    le32 dev_group;
    uint8_t seek_speed;
    uint8_t bandwidth;
    btrfs::uuid uuid;
    btrfs::uuid fsid;
} __attribute__((packed));

struct root_backup {
    le64 tree_root;
    le64 tree_root_gen;
    le64 chunk_root;
    le64 chunk_root_gen;
    le64 extent_root;
    le64 extent_root_gen;
    le64 fs_root;
    le64 fs_root_gen;
    le64 dev_root;
    le64 dev_root_gen;
    le64 csum_root;
    le64 csum_root_gen;
    le64 total_bytes;
    le64 bytes_used;
    le64 num_devices;
    le64 unused_64[4];
    uint8_t tree_root_level;
    uint8_t chunk_root_level;
    uint8_t extent_root_level;
    uint8_t fs_root_level;
    uint8_t dev_root_level;
    uint8_t csum_root_level;
    uint8_t unused_8[10];
} __attribute__((packed));

enum class csum_type : uint16_t {
    CRC32 = 0,
    XXHASH = 1,
    SHA256 = 2,
    BLAKE2 = 3,
};

struct super_block {
    array<uint8_t, 32> csum;
    uuid fsid;
    le64 bytenr;
    le64 flags;
    le64 magic;
    le64 generation;
    le64 root;
    le64 chunk_root;
    le64 log_root;
    le64 __unused_log_root_transid;
    le64 total_bytes;
    le64 bytes_used;
    le64 root_dir_objectid;
    le64 num_devices;
    le32 sectorsize;
    le32 nodesize;
    le32 __unused_leafsize;
    le32 stripesize;
    le32 sys_chunk_array_size;
    le64 chunk_root_generation;
    le64 compat_flags;
    le64 compat_ro_flags;
    le64 incompat_flags;
    enum csum_type csum_type;
    uint8_t root_level;
    uint8_t chunk_root_level;
    uint8_t log_root_level;
    btrfs::dev_item dev_item;
    array<char, 0x100> label;
    le64 cache_generation;
    le64 uuid_tree_generation;
    uuid metadata_uuid;
    le64 nr_global_roots;
    le64 remap_root;
    le64 remap_root_generation;
    uint8_t remap_root_level;
    uint8_t reserved[199];
    array<uint8_t, 0x800> sys_chunk_array;
    array<root_backup, 4> super_roots;
    uint8_t padding[565];
};

static_assert(sizeof(super_block) == 4096);

enum class key_type : uint8_t {
    INODE_ITEM = 0x01,
    INODE_REF = 0x0c,
    INODE_EXTREF = 0x0d,
    XATTR_ITEM = 0x18,
    VERITY_DESC_ITEM = 0x24,
    VERITY_MERKLE_ITEM = 0x25,
    ORPHAN_ITEM = 0x30,
    DIR_LOG_INDEX = 0x48,
    DIR_ITEM = 0x54,
    DIR_INDEX = 0x60,
    EXTENT_DATA = 0x6c,
    EXTENT_CSUM = 0x80,
    ROOT_ITEM = 0x84,
    ROOT_BACKREF = 0x90,
    ROOT_REF = 0x9c,
    EXTENT_ITEM = 0xa8,
    METADATA_ITEM = 0xa9,
    EXTENT_OWNER_REF = 0xac,
    TREE_BLOCK_REF = 0xb0,
    EXTENT_DATA_REF = 0xb2,
    SHARED_BLOCK_REF = 0xb6,
    SHARED_DATA_REF = 0xb8,
    BLOCK_GROUP_ITEM = 0xc0,
    FREE_SPACE_INFO = 0xc6,
    FREE_SPACE_EXTENT = 0xc7,
    FREE_SPACE_BITMAP = 0xc8,
    DEV_EXTENT = 0xcc,
    DEV_ITEM = 0xd8,
    CHUNK_ITEM = 0xe4,
    RAID_STRIPE = 0xe6,
    IDENTITY_REMAP = 0xea,
    REMAP = 0xeb,
    REMAP_BACKREF = 0xec,
    QGROUP_STATUS = 0xf0,
    QGROUP_INFO = 0xf2,
    QGROUP_LIMIT = 0xf4,
    QGROUP_RELATION = 0xf6,
    TEMPORARY_ITEM = 0xf8,
    PERSISTENT_ITEM = 0xf9,
    DEV_REPLACE = 0xfa,
    UUID_SUBVOL = 0xfb,
    UUID_RECEIVED_SUBVOL = 0xfc,
    STRING_ITEM = 0xfd
};

struct key {
    le64 objectid;
    key_type type;
    le64 offset;

    bool operator==(const key& k) const = default;

    strong_ordering operator<=>(const key& k) const {
        auto cmp = objectid <=> k.objectid;

        if (cmp != strong_ordering::equal)
            return cmp;

        cmp = type <=> k.type;

        if (cmp != strong_ordering::equal)
            return cmp;

        return offset <=> k.offset;
    }
} __attribute__((packed));

static_assert(sizeof(key) == 17);

struct stripe {
    le64 devid;
    le64 offset;
    uuid dev_uuid;
} __attribute__((packed));

struct chunk {
    le64 length;
    le64 owner;
    le64 stripe_len;
    le64 type;
    le32 io_align;
    le32 io_width;
    le32 sector_size;
    le16 num_stripes;
    le16 sub_stripes;
    btrfs::stripe stripe[1];
} __attribute__((packed));

struct header {
    array<uint8_t, 32> csum;
    uuid fsid;
    le64 bytenr;
    le64 flags;
    uuid chunk_tree_uuid;
    le64 generation;
    le64 owner;
    le32 nritems;
    uint8_t level;
} __attribute__((packed));

static_assert(sizeof(header) == 101);

struct item {
    btrfs::key key;
    le32 offset;
    le32 size;
} __attribute__((packed));

static_assert(sizeof(item) == 25);

struct key_ptr {
    btrfs::key key;
    le64 blockptr;
    le64 generation;
} __attribute__((packed));

static_assert(sizeof(key_ptr) == 33);

struct timespec {
    le64 sec;
    le32 nsec;
} __attribute__((packed));

struct inode_item {
    le64 generation;
    le64 transid;
    le64 size;
    le64 nbytes;
    le64 block_group;
    le32 nlink;
    le32 uid;
    le32 gid;
    le32 mode;
    le64 rdev;
    le64 flags;
    le64 sequence;
    le64 reserved[4];
    timespec atime;
    timespec ctime;
    timespec mtime;
    timespec otime;
} __attribute__((packed));

static_assert(sizeof(inode_item) == 160);

struct root_item {
    inode_item inode;
    le64 generation;
    le64 root_dirid;
    le64 bytenr;
    le64 byte_limit;
    le64 bytes_used;
    le64 last_snapshot;
    le64 flags;
    le32 refs;
    key drop_progress;
    uint8_t drop_level;
    uint8_t level;
    le64 generation_v2;
    btrfs::uuid uuid;
    btrfs::uuid parent_uuid;
    btrfs::uuid received_uuid;
    le64 ctransid;
    le64 otransid;
    le64 stransid;
    le64 rtransid;
    timespec ctime;
    timespec otime;
    timespec stime;
    timespec rtime;
    le64 reserved[8];
} __attribute__((packed));

static_assert(sizeof(root_item) == 439);

struct dev_extent {
    le64 chunk_tree;
    le64 chunk_objectid;
    le64 chunk_offset;
    le64 length;
    uuid chunk_tree_uuid;
} __attribute__ ((__packed__));

struct remap {
    le64 address;
} __attribute__ ((__packed__));

struct inode_ref {
    le64 index;
    le16 name_len;
} __attribute__ ((__packed__));

struct inode_extref {
    le64 parent_objectid;
    le64 index;
    le16 name_len;
    char name[0];
} __attribute__ ((__packed__));

enum class dir_item_type : uint8_t {
    unknown = 0,
    reg_file = 1,
    dir = 2,
    chrdev = 3,
    blkdev = 4,
    fifo = 5,
    sock = 6,
    symlink = 7,
    xattr = 8,
};

struct dir_item {
    key location;
    le64 transid;
    le16 data_len;
    le16 name_len;
    dir_item_type type;
} __attribute__ ((__packed__));

struct block_group_item {
    le64 used;
    le64 chunk_objectid;
    le64 flags;
} __attribute__ ((__packed__));

struct free_space_info {
    le32 extent_count;
    le32 flags;
} __attribute__ ((__packed__));

struct extent_item {
    le64 refs;
    le64 generation;
    le64 flags;
} __attribute__ ((__packed__));

struct extent_inline_ref {
    key_type type;
    le64 offset;
} __attribute__ ((__packed__));

struct tree_block_info {
    btrfs::key key;
    uint8_t level;
} __attribute__ ((__packed__));

struct extent_data_ref {
    le64 root;
    le64 objectid;
    le64 offset;
    le32 count;
} __attribute__ ((__packed__));

struct shared_data_ref {
    le32 count;
} __attribute__ ((__packed__));

enum class file_extent_item_type : uint8_t {
    inline_extent = 0,
    reg = 1,
    prealloc = 2,
};

enum class compression_type : uint8_t {
    none = 0,
    zlib = 1,
    lzo = 2,
    zstd = 3,
};

struct file_extent_item {
    le64 generation;
    le64 ram_bytes;
    compression_type compression;
    uint8_t encryption;
    le16 other_encoding;
    file_extent_item_type type;
    le64 disk_bytenr;
    le64 disk_num_bytes;
    le64 offset;
    le64 num_bytes;
} __attribute__ ((__packed__));

struct root_ref {
    le64 dirid;
    le64 sequence;
    le16 name_len;
} __attribute__ ((__packed__));

struct free_space_header {
    btrfs::key location;
    le64 generation;
    le64 num_entries;
    le64 num_bitmaps;
} __attribute__ ((__packed__));

struct verity_descriptor_item {
    le64 size;
    le64 reserved[2];
    uint8_t encryption;
} __attribute__ ((__packed__));

enum class fsverity_hash_algorithm : uint8_t {
    SHA256 = 1,
    SHA512 = 2,
};

struct fsverity_descriptor {
    uint8_t version;
    fsverity_hash_algorithm hash_algorithm;
    uint8_t log_blocksize;
    uint8_t salt_size;
    le32 __reserved_0x04;
    le64 data_size;
    uint8_t root_hash[64];
    uint8_t salt[32];
    uint8_t __reserved[144];
} __attribute__ ((__packed__));

enum class raid_type {
    SINGLE,
    RAID0,
    RAID1,
    DUP,
    RAID10,
    RAID5,
    RAID6,
    RAID1C3,
    RAID1C4,
};

enum raid_type get_chunk_raid_type(const chunk& c) {
    if (c.type & BLOCK_GROUP_RAID0)
        return raid_type::RAID0;
    else if (c.type & BLOCK_GROUP_RAID1)
        return raid_type::RAID1;
    else if (c.type & BLOCK_GROUP_DUP)
        return raid_type::DUP;
    else if (c.type & BLOCK_GROUP_RAID10)
        return raid_type::RAID10;
    else if (c.type & BLOCK_GROUP_RAID5)
        return raid_type::RAID5;
    else if (c.type & BLOCK_GROUP_RAID6)
        return raid_type::RAID6;
    else if (c.type & BLOCK_GROUP_RAID1C3)
        return raid_type::RAID1C3;
    else if (c.type & BLOCK_GROUP_RAID1C4)
        return raid_type::RAID1C4;
    else
        return raid_type::SINGLE;
}

bool check_superblock_csum(const super_block& sb) {
    // FIXME - xxhash, sha256, blake2

    if (sb.csum_type != csum_type::CRC32)
        return false;

    auto crc32 = ~calc_crc32c(0xffffffff,
                              span((uint8_t*)&sb.fsid, sizeof(super_block) - sizeof(sb.csum)));

    return *(le32*)sb.csum.data() == crc32;
}

bool check_tree_csum(const header& h, const super_block& sb) {
    // FIXME - xxhash, sha256, blake2

    if (sb.csum_type != csum_type::CRC32)
        return false;

    auto crc32 = ~calc_crc32c(0xffffffff, span((uint8_t*)&h.fsid, sb.nodesize - sizeof(h.csum)));

    return *(le32*)h.csum.data() == crc32;
}

}

template<>
struct std::formatter<enum btrfs::key_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::key_type t, format_context& ctx) const {
        switch (t) {
            case btrfs::key_type::INODE_ITEM:
                return format_to(ctx.out(), "INODE_ITEM");
            case btrfs::key_type::INODE_REF:
                return format_to(ctx.out(), "INODE_REF");
            case btrfs::key_type::INODE_EXTREF:
                return format_to(ctx.out(), "INODE_EXTREF");
            case btrfs::key_type::XATTR_ITEM:
                return format_to(ctx.out(), "XATTR_ITEM");
            case btrfs::key_type::VERITY_DESC_ITEM:
                return format_to(ctx.out(), "VERITY_DESC_ITEM");
            case btrfs::key_type::VERITY_MERKLE_ITEM:
                return format_to(ctx.out(), "VERITY_MERKLE_ITEM");
            case btrfs::key_type::ORPHAN_ITEM:
                return format_to(ctx.out(), "ORPHAN_ITEM");
            case btrfs::key_type::DIR_LOG_INDEX:
                return format_to(ctx.out(), "DIR_LOG_INDEX");
            case btrfs::key_type::DIR_ITEM:
                return format_to(ctx.out(), "DIR_ITEM");
            case btrfs::key_type::DIR_INDEX:
                return format_to(ctx.out(), "DIR_INDEX");
            case btrfs::key_type::EXTENT_DATA:
                return format_to(ctx.out(), "EXTENT_DATA");
            case btrfs::key_type::EXTENT_CSUM:
                return format_to(ctx.out(), "EXTENT_CSUM");
            case btrfs::key_type::ROOT_ITEM:
                return format_to(ctx.out(), "ROOT_ITEM");
            case btrfs::key_type::ROOT_BACKREF:
                return format_to(ctx.out(), "ROOT_BACKREF");
            case btrfs::key_type::ROOT_REF:
                return format_to(ctx.out(), "ROOT_REF");
            case btrfs::key_type::EXTENT_ITEM:
                return format_to(ctx.out(), "EXTENT_ITEM");
            case btrfs::key_type::METADATA_ITEM:
                return format_to(ctx.out(), "METADATA_ITEM");
            case btrfs::key_type::EXTENT_OWNER_REF:
                return format_to(ctx.out(), "EXTENT_OWNER_REF");
            case btrfs::key_type::TREE_BLOCK_REF:
                return format_to(ctx.out(), "TREE_BLOCK_REF");
            case btrfs::key_type::EXTENT_DATA_REF:
                return format_to(ctx.out(), "EXTENT_DATA_REF");
            case btrfs::key_type::SHARED_BLOCK_REF:
                return format_to(ctx.out(), "SHARED_BLOCK_REF");
            case btrfs::key_type::SHARED_DATA_REF:
                return format_to(ctx.out(), "SHARED_DATA_REF");
            case btrfs::key_type::BLOCK_GROUP_ITEM:
                return format_to(ctx.out(), "BLOCK_GROUP_ITEM");
            case btrfs::key_type::FREE_SPACE_INFO:
                return format_to(ctx.out(), "FREE_SPACE_INFO");
            case btrfs::key_type::FREE_SPACE_EXTENT:
                return format_to(ctx.out(), "FREE_SPACE_EXTENT");
            case btrfs::key_type::FREE_SPACE_BITMAP:
                return format_to(ctx.out(), "FREE_SPACE_BITMAP");
            case btrfs::key_type::DEV_EXTENT:
                return format_to(ctx.out(), "DEV_EXTENT");
            case btrfs::key_type::DEV_ITEM:
                return format_to(ctx.out(), "DEV_ITEM");
            case btrfs::key_type::CHUNK_ITEM:
                return format_to(ctx.out(), "CHUNK_ITEM");
            case btrfs::key_type::RAID_STRIPE:
                return format_to(ctx.out(), "RAID_STRIPE");
            case btrfs::key_type::IDENTITY_REMAP:
                return format_to(ctx.out(), "IDENTITY_REMAP");
            case btrfs::key_type::REMAP:
                return format_to(ctx.out(), "REMAP");
            case btrfs::key_type::REMAP_BACKREF:
                return format_to(ctx.out(), "REMAP_BACKREF");
            case btrfs::key_type::QGROUP_STATUS:
                return format_to(ctx.out(), "QGROUP_STATUS");
            case btrfs::key_type::QGROUP_INFO:
                return format_to(ctx.out(), "QGROUP_INFO");
            case btrfs::key_type::QGROUP_LIMIT:
                return format_to(ctx.out(), "QGROUP_LIMIT");
            case btrfs::key_type::QGROUP_RELATION:
                return format_to(ctx.out(), "QGROUP_RELATION");
            case btrfs::key_type::TEMPORARY_ITEM:
                return format_to(ctx.out(), "TEMPORARY_ITEM");
            case btrfs::key_type::PERSISTENT_ITEM:
                return format_to(ctx.out(), "PERSISTENT_ITEM");
            case btrfs::key_type::DEV_REPLACE:
                return format_to(ctx.out(), "DEV_REPLACE");
            case btrfs::key_type::UUID_SUBVOL:
                return format_to(ctx.out(), "UUID_SUBVOL");
            case btrfs::key_type::UUID_RECEIVED_SUBVOL:
                return format_to(ctx.out(), "UUID_RECEIVED_SUBVOL");
            case btrfs::key_type::STRING_ITEM:
                return format_to(ctx.out(), "STRING_ITEM");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<btrfs::key> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}') {
            switch (*it) {
                case 'x':
                    no_translate = true;
                    break;

                default:
                    throw format_error("invalid format");
            }

            it++;
        }

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::key& k, format_context& ctx) const {
        if (no_translate)
            return format_to(ctx.out(), "{:x},{:x},{:x}", k.objectid, (uint8_t)k.type, k.offset);
        else
            return format_to(ctx.out(), "{:x},{},{:x}", k.objectid, k.type, k.offset);
    }

    bool no_translate = false;
};

template<>
struct std::formatter<enum btrfs::raid_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::raid_type t, format_context& ctx) const {
        switch (t) {
            case btrfs::raid_type::SINGLE:
                return format_to(ctx.out(), "SINGLE");
            case btrfs::raid_type::RAID0:
                return format_to(ctx.out(), "RAID0");
            case btrfs::raid_type::RAID1:
                return format_to(ctx.out(), "RAID1");
            case btrfs::raid_type::DUP:
                return format_to(ctx.out(), "DUP");
            case btrfs::raid_type::RAID10:
                return format_to(ctx.out(), "RAID10");
            case btrfs::raid_type::RAID5:
                return format_to(ctx.out(), "RAID5");
            case btrfs::raid_type::RAID6:
                return format_to(ctx.out(), "RAID6");
            case btrfs::raid_type::RAID1C3:
                return format_to(ctx.out(), "RAID1C3");
            case btrfs::raid_type::RAID1C4:
                return format_to(ctx.out(), "RAID1C4");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<enum btrfs::csum_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::csum_type t, format_context& ctx) const {
        switch (t) {
            case btrfs::csum_type::CRC32:
                return format_to(ctx.out(), "crc32");
            case btrfs::csum_type::XXHASH:
                return format_to(ctx.out(), "xxhash");
            case btrfs::csum_type::SHA256:
                return format_to(ctx.out(), "sha256");
            case btrfs::csum_type::BLAKE2:
                return format_to(ctx.out(), "blake2");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<btrfs::uuid> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::uuid& u, format_context& ctx) const {
        return format_to(ctx.out(), "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                         u.uuid[15], u.uuid[14], u.uuid[13], u.uuid[12], u.uuid[11], u.uuid[10], u.uuid[9], u.uuid[8], u.uuid[7],
                         u.uuid[6], u.uuid[5], u.uuid[4], u.uuid[3], u.uuid[2], u.uuid[1], u.uuid[0]);
    }
};

template<>
struct std::formatter<btrfs::dev_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::dev_item& d, format_context& ctx) const {
        return format_to(ctx.out(), "devid={:x} total_bytes={:x} bytes_used={:x} io_align={:x} io_width={:x} sector_size={:x} type={:x} generation={:x} start_offset={:x} dev_group={:x} seek_speed={:x} bandwidth={:x} uuid={} fsid={}",
                         d.devid, d.total_bytes, d.bytes_used, d.io_align, d.io_width, d.sector_size, d.type, d.generation,
                         d.start_offset, d.dev_group, d.seek_speed, d.bandwidth, d.uuid, d.fsid);
    }
};

string block_group_item_flags(uint64_t f) {
    string ret;

    // FIXME - REMAPPED, REMAP

    if (f & btrfs::BLOCK_GROUP_DATA) {
        ret = "data";
        f &= ~btrfs::BLOCK_GROUP_DATA;
    }

    if (f & btrfs::BLOCK_GROUP_SYSTEM) {
        if (!ret.empty())
            ret += ",";

        ret += "system";
        f &= ~btrfs::BLOCK_GROUP_SYSTEM;
    }

    if (f & btrfs::BLOCK_GROUP_METADATA) {
        if (!ret.empty())
            ret += ",";

        ret += "metadata";
        f &= ~btrfs::BLOCK_GROUP_METADATA;
    }

    if (f & btrfs::BLOCK_GROUP_RAID0) {
        if (!ret.empty())
            ret += ",";

        ret += "raid0";
        f &= ~btrfs::BLOCK_GROUP_RAID0;
    }

    if (f & btrfs::BLOCK_GROUP_RAID1) {
        if (!ret.empty())
            ret += ",";

        ret += "raid1";
        f &= ~btrfs::BLOCK_GROUP_RAID1;
    }

    if (f & btrfs::BLOCK_GROUP_DUP) {
        if (!ret.empty())
            ret += ",";

        ret += "dup";
        f &= ~btrfs::BLOCK_GROUP_DUP;
    }

    if (f & btrfs::BLOCK_GROUP_RAID10) {
        if (!ret.empty())
            ret += ",";

        ret += "raid10";
        f &= ~btrfs::BLOCK_GROUP_RAID10;
    }

    if (f & btrfs::BLOCK_GROUP_RAID5) {
        if (!ret.empty())
            ret += ",";

        ret += "raid5";
        f &= ~btrfs::BLOCK_GROUP_RAID5;
    }

    if (f & btrfs::BLOCK_GROUP_RAID6) {
        if (!ret.empty())
            ret += ",";

        ret += "raid6";
        f &= ~btrfs::BLOCK_GROUP_RAID6;
    }

    if (f & btrfs::BLOCK_GROUP_RAID1C3) {
        if (!ret.empty())
            ret += ",";

        ret += "raid1c3";
        f &= ~btrfs::BLOCK_GROUP_RAID1C3;
    }

    if (f & btrfs::BLOCK_GROUP_RAID1C4) {
        if (!ret.empty())
            ret += ",";

        ret += "raid1c4";
        f &= ~btrfs::BLOCK_GROUP_RAID1C4;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::chunk> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::chunk& c, format_context& ctx) const {
        auto r = format_to(ctx.out(), "length={:x} owner={:x} stripe_len={:x} type={} io_align={:x} io_width={:x} sector_size={:x} num_stripes={:x} sub_stripes={:x}",
                           c.length, c.owner, c.stripe_len, block_group_item_flags(c.type), c.io_align, c.io_width, c.sector_size, c.num_stripes, c.sub_stripes);

        for (unsigned int i = 0; i < c.num_stripes; i++) {
            auto& s = c.stripe[i];

            r = format_to(r, " stripe({}) devid={:x} offset={:x} dev_uuid={}", i, s.devid, s.offset, s.dev_uuid);
        }

        return r;
    }
};

template<>
struct std::formatter<btrfs::root_backup> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::root_backup& b, format_context& ctx) const {
        return format_to(ctx.out(), "tree_root={:x} tree_root_gen={:x} chunk_root={:x} chunk_root_gen={:x} extent_root={:x} extent_root_gen={:x} fs_root={:x} fs_root_gen={:x} dev_root={:x} dev_root_gen={:x} csum_root={:x} csum_root_gen={:x} total_bytes={:x} bytes_used={:x} num_devices={:x} tree_root_level={:x} chunk_root_level={:x} extent_root_level={:x} fs_root_level={:x} dev_root_level={:x} csum_root_level={:x}",
                         b.tree_root, b.tree_root_gen, b.chunk_root, b.chunk_root_gen, b.extent_root, b.extent_root_gen, b.fs_root, b.fs_root_gen,
                         b.dev_root, b.dev_root_gen, b.csum_root, b.csum_root_gen, b.total_bytes, b.bytes_used, b.num_devices, b.tree_root_level,
                         b.chunk_root_level, b.extent_root_level, b.fs_root_level, b.dev_root_level, b.csum_root_level);
    }
};

string compat_ro_flags(uint64_t f) {
    string ret;

    if (f & btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE) {
        ret += "free_space_tree";
        f &= ~btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE;
    }

    if (f & btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID) {
        if (!ret.empty())
            ret += ",";

        ret += "free_space_tree_valid";
        f &= ~btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID;
    }

    if (f & btrfs::FEATURE_COMPAT_RO_VERITY) {
        if (!ret.empty())
            ret += ",";

        ret += "verity";
        f &= ~btrfs::FEATURE_COMPAT_RO_VERITY;
    }

    if (f & btrfs::FEATURE_COMPAT_RO_BLOCK_GROUP_TREE) {
        if (!ret.empty())
            ret += ",";

        ret += "block_group_tree";
        f &= ~btrfs::FEATURE_COMPAT_RO_BLOCK_GROUP_TREE;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

string incompat_flags(uint64_t f) {
    string ret;

    if (f & btrfs::FEATURE_INCOMPAT_MIXED_BACKREF) {
        if (!ret.empty())
            ret += ",";

        ret += "mixed_backref";
        f &= ~btrfs::FEATURE_INCOMPAT_MIXED_BACKREF;
    }

    if (f & btrfs::FEATURE_INCOMPAT_DEFAULT_SUBVOL) {
        if (!ret.empty())
            ret += ",";

        ret += "default_subvol";
        f &= ~btrfs::FEATURE_INCOMPAT_DEFAULT_SUBVOL;
    }

    if (f & btrfs::FEATURE_INCOMPAT_MIXED_GROUPS) {
        if (!ret.empty())
            ret += ",";

        ret += "mixed_groups";
        f &= ~btrfs::FEATURE_INCOMPAT_MIXED_GROUPS;
    }

    if (f & btrfs::FEATURE_INCOMPAT_COMPRESS_LZO) {
        if (!ret.empty())
            ret += ",";

        ret += "compress_lzo";
        f &= ~btrfs::FEATURE_INCOMPAT_COMPRESS_LZO;
    }

    if (f & btrfs::FEATURE_INCOMPAT_COMPRESS_ZSTD) {
        if (!ret.empty())
            ret += ",";

        ret += "compress_zstd";
        f &= ~btrfs::FEATURE_INCOMPAT_COMPRESS_ZSTD;
    }

    if (f & btrfs::FEATURE_INCOMPAT_BIG_METADATA) {
        if (!ret.empty())
            ret += ",";

        ret += "big_metadata";
        f &= ~btrfs::FEATURE_INCOMPAT_BIG_METADATA;
    }

    if (f & btrfs::FEATURE_INCOMPAT_EXTENDED_IREF) {
        if (!ret.empty())
            ret += ",";

        ret += "extended_iref";
        f &= ~btrfs::FEATURE_INCOMPAT_EXTENDED_IREF;
    }

    if (f & btrfs::FEATURE_INCOMPAT_RAID56) {
        if (!ret.empty())
            ret += ",";

        ret += "raid56";
        f &= ~btrfs::FEATURE_INCOMPAT_RAID56;
    }

    if (f & btrfs::FEATURE_INCOMPAT_SKINNY_METADATA) {
        if (!ret.empty())
            ret += ",";

        ret += "skinny_metadata";
        f &= ~btrfs::FEATURE_INCOMPAT_SKINNY_METADATA;
    }

    if (f & btrfs::FEATURE_INCOMPAT_NO_HOLES) {
        if (!ret.empty())
            ret += ",";

        ret += "no_holes";
        f &= ~btrfs::FEATURE_INCOMPAT_NO_HOLES;
    }

    if (f & btrfs::FEATURE_INCOMPAT_METADATA_UUID) {
        if (!ret.empty())
            ret += ",";

        ret += "metadata_uuid";
        f &= ~btrfs::FEATURE_INCOMPAT_METADATA_UUID;
    }

    if (f & btrfs::FEATURE_INCOMPAT_RAID1C34) {
        if (!ret.empty())
            ret += ",";

        ret += "raid1c34";
        f &= ~btrfs::FEATURE_INCOMPAT_RAID1C34;
    }

    if (f & btrfs::FEATURE_INCOMPAT_ZONED) {
        if (!ret.empty())
            ret += ",";

        ret += "zoned";
        f &= ~btrfs::FEATURE_INCOMPAT_ZONED;
    }

    if (f & btrfs::FEATURE_INCOMPAT_EXTENT_TREE_V2) {
        if (!ret.empty())
            ret += ",";

        ret += "extent_tree_v2";
        f &= ~btrfs::FEATURE_INCOMPAT_EXTENT_TREE_V2;
    }

    if (f & btrfs::FEATURE_INCOMPAT_RAID_STRIPE_TREE) {
        if (!ret.empty())
            ret += ",";

        ret += "raid_stripe_tree";
        f &= ~btrfs::FEATURE_INCOMPAT_RAID_STRIPE_TREE;
    }

    if (f & btrfs::FEATURE_INCOMPAT_SIMPLE_QUOTA) {
        if (!ret.empty())
            ret += ",";

        ret += "simple_quota";
        f &= ~btrfs::FEATURE_INCOMPAT_SIMPLE_QUOTA;
    }

    // FIXME - remap tree

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

string format_super_flags(uint64_t f) {
    string ret;

    if (f & btrfs::HEADER_FLAG_WRITTEN) {
        ret += "written";
        f &= ~btrfs::HEADER_FLAG_WRITTEN;
    }

    if (f & btrfs::HEADER_FLAG_RELOC) {
        if (!ret.empty())
            ret += ",";

        ret += "reloc";
        f &= ~btrfs::HEADER_FLAG_RELOC;
    }

    if (f & btrfs::SUPER_FLAG_ERROR) {
        if (!ret.empty())
            ret += ",";

        ret += "error";
        f &= ~btrfs::SUPER_FLAG_ERROR;
    }

    if (f & btrfs::SUPER_FLAG_SEEDING) {
        if (!ret.empty())
            ret += ",";

        ret += "seeding";
        f &= ~btrfs::SUPER_FLAG_SEEDING;
    }

    if (f & btrfs::SUPER_FLAG_METADUMP) {
        if (!ret.empty())
            ret += ",";

        ret += "metadump";
        f &= ~btrfs::SUPER_FLAG_METADUMP;
    }

    if (f & btrfs::SUPER_FLAG_METADUMP_V2) {
        if (!ret.empty())
            ret += ",";

        ret += "metadump_v2";
        f &= ~btrfs::SUPER_FLAG_METADUMP_V2;
    }

    if (f & btrfs::SUPER_FLAG_CHANGING_FSID) {
        if (!ret.empty())
            ret += ",";

        ret += "changing_fsid";
        f &= ~btrfs::SUPER_FLAG_CHANGING_FSID;
    }

    if (f & btrfs::SUPER_FLAG_CHANGING_FSID_V2) {
        if (!ret.empty())
            ret += ",";

        ret += "changing_fsid_v2";
        f &= ~btrfs::SUPER_FLAG_CHANGING_FSID_V2;
    }

    if (f & btrfs::SUPER_FLAG_CHANGING_BG_TREE) {
        if (!ret.empty())
            ret += ",";

        ret += "changing_bg_tree";
        f &= ~btrfs::SUPER_FLAG_CHANGING_BG_TREE;
    }

    if (f & btrfs::SUPER_FLAG_CHANGING_DATA_CSUM) {
        if (!ret.empty())
            ret += ",";

        ret += "changing_data_csum";
        f &= ~btrfs::SUPER_FLAG_CHANGING_DATA_CSUM;
    }

    if (f & btrfs::SUPER_FLAG_CHANGING_META_CSUM) {
        if (!ret.empty())
            ret += ",";

        ret += "changing_meta_csum";
        f &= ~btrfs::SUPER_FLAG_CHANGING_META_CSUM;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::super_block> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::super_block& sb, format_context& ctx) const {
        string csum;

        switch (sb.csum_type) {
            case btrfs::csum_type::CRC32:
                csum = std::format("{:08x}", *(uint32_t*)sb.csum.data());
                break;

            case btrfs::csum_type::XXHASH:
                csum = std::format("{:016x}", *(uint64_t*)sb.csum.data());
                break;

            case btrfs::csum_type::SHA256:
            case btrfs::csum_type::BLAKE2:
                csum = std::format("{:016x}{:016x}{:016x}{:016x}", *(uint64_t*)&sb.csum[0],
                                   *(uint64_t*)&sb.csum[sizeof(uint64_t)], *(uint64_t*)&sb.csum[2 * sizeof(uint64_t)],
                                   *(uint64_t*)&sb.csum[3 * sizeof(uint64_t)]);
                break;

            default:
                csum = "???";
                break;
        }

        auto label = string_view(sb.label.data(), sizeof(sb.label));
        auto magic = string_view((char*)&sb.magic, sizeof(sb.magic));

        if (auto nul = label.find_first_of('\x00'); nul != string_view::npos)
            label = label.substr(0, nul);

        auto r = format_to(ctx.out(), "csum={} fsid={} bytenr={:x} flags={} magic={} generation={:x} root={:x} chunk_root={:x} log_root={:x} log_root_transid={:x} total_bytes={:x} bytes_used={:x} root_dir_objectid={:x} num_devices={:x} sectorsize={:x} nodesize={:x} leafsize={:x} stripesize={:x} sys_chunk_array_size={:x} chunk_root_generation={:x} compat_flags={:x} compat_ro_flags={} incompat_flags={} csum_type={} root_level={:x} chunk_root_level={:x} log_root_level={:x} (dev_item {}) label={} cache_generation={:x} uuid_tree_generation={:x} metadata_uuid={}\n", csum, sb.fsid, sb.bytenr, format_super_flags(sb.flags), magic, sb.generation, sb.root, sb.chunk_root, sb.log_root, sb.__unused_log_root_transid, sb.total_bytes, sb.bytes_used, sb.root_dir_objectid, sb.num_devices, sb.sectorsize, sb.nodesize, sb.__unused_leafsize, sb.stripesize, sb.sys_chunk_array_size, sb.chunk_root_generation, sb.compat_flags, compat_ro_flags(sb.compat_ro_flags), incompat_flags(sb.incompat_flags), sb.csum_type, sb.root_level, sb.chunk_root_level, sb.log_root_level, sb.dev_item, label, sb.cache_generation, sb.uuid_tree_generation, sb.metadata_uuid);

        // FIXME - nr_global_roots
        // FIXME - remap_root
        // FIXME - remap_root_generation
        // FIXME - remap_root_level

        auto bootstrap = span(sb.sys_chunk_array.data(), sb.sys_chunk_array_size);

        while (!bootstrap.empty()) {
            const auto& k = *(btrfs::key*)bootstrap.data();

            r = format_to(r, "bootstrap {:x}\n", k);

            bootstrap = bootstrap.subspan(sizeof(btrfs::key));

            const auto& c = *(btrfs::chunk*)bootstrap.data();

            r = format_to(r, "chunk_item {}\n", c);

            bootstrap = bootstrap.subspan(offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)));
        }

        for (const auto& b : sb.super_roots) {
            r = format_to(r, "backup {}\n", b);
        }

        return r;
    }
};

string header_flags(uint64_t f) {
    string ret;

    if (f & btrfs::HEADER_FLAG_WRITTEN) {
        ret += "written";
        f &= ~btrfs::HEADER_FLAG_WRITTEN;
    }

    if (f & btrfs::HEADER_FLAG_RELOC) {
        if (!ret.empty())
            ret += ",";

        ret += "reloc";
        f &= ~btrfs::HEADER_FLAG_RELOC;
    }

    if (f & btrfs::HEADER_FLAG_MIXED_BACKREF) {
        if (!ret.empty())
            ret += ",";

        ret += "mixed_backref";
        f &= ~btrfs::HEADER_FLAG_MIXED_BACKREF;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::header> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}') {
            switch (*it) {
                case 'a':
                    csum_length = 4;
                    break;

                case 'b':
                    csum_length = 8;
                    break;

                case 'c':
                    csum_length = 16;
                    break;

                default:
                    throw format_error("invalid format");
            }

            it++;
        }

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::header& h, format_context& ctx) const {
        string csum;

        switch (csum_length) {
            case 4:
                csum = std::format("{:08x}", *(uint32_t*)h.csum.data());
                break;

            case 8:
                csum = std::format("{:016x}", *(uint64_t*)h.csum.data());
                break;

            case 16:
                csum = std::format("{:016x}{:016x}{:016x}{:016x}", *(uint64_t*)&h.csum[0],
                                   *(uint64_t*)&h.csum[sizeof(uint64_t)], *(uint64_t*)&h.csum[2 * sizeof(uint64_t)],
                                   *(uint64_t*)&h.csum[3 * sizeof(uint64_t)]);
                break;

            default:
                csum = "???";
                break;
        }

        return format_to(ctx.out(), "csum={} fsid={} bytenr={:x} flags={} chunk_tree_uuid={} generation={:x} owner={:x} nritems={:x} level={:x}",
                         csum, h.fsid, h.bytenr, header_flags(h.flags), h.chunk_tree_uuid, h.generation, h.owner, h.nritems, h.level);
    }

    unsigned int csum_length = 4;
};

template<>
struct std::formatter<btrfs::timespec> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::timespec& t, format_context& ctx) const {
        auto tp = chrono::time_point<chrono::system_clock>(chrono::days{t.sec / 86400});
        auto hms = chrono::hh_mm_ss{chrono::seconds{t.sec % 86400}};
        auto ymd = chrono::year_month_day{chrono::floor<chrono::days>(tp)};

        // FIXME - include nsec

        return format_to(ctx.out(), "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                         (int)ymd.year(), (unsigned int)ymd.month(), (unsigned int)ymd.day(),
                         hms.hours().count(), hms.minutes().count(), hms.seconds().count());
    }
};

string inode_item_flags(uint64_t f) {
    string ret;

    if (f & btrfs::INODE_NODATASUM) {
        if (!ret.empty())
            ret += ",";

        ret += "nodatasum";
        f &= ~btrfs::INODE_NODATASUM;
    }

    if (f & btrfs::INODE_NODATACOW) {
        if (!ret.empty())
            ret += ",";

        ret += "nodatacow";
        f &= ~btrfs::INODE_NODATACOW;
    }

    if (f & btrfs::INODE_READONLY) {
        if (!ret.empty())
            ret += ",";

        ret += "readonly";
        f &= ~btrfs::INODE_READONLY;
    }

    if (f & btrfs::INODE_NOCOMPRESS) {
        if (!ret.empty())
            ret += ",";

        ret += "nocompress";
        f &= ~btrfs::INODE_NOCOMPRESS;
    }

    if (f & btrfs::INODE_PREALLOC) {
        if (!ret.empty())
            ret += ",";

        ret += "prealloc";
        f &= ~btrfs::INODE_PREALLOC;
    }

    if (f & btrfs::INODE_SYNC) {
        if (!ret.empty())
            ret += ",";

        ret += "sync";
        f &= ~btrfs::INODE_SYNC;
    }

    if (f & btrfs::INODE_IMMUTABLE) {
        if (!ret.empty())
            ret += ",";

        ret += "immutable";
        f &= ~btrfs::INODE_IMMUTABLE;
    }

    if (f & btrfs::INODE_APPEND) {
        if (!ret.empty())
            ret += ",";

        ret += "append";
        f &= ~btrfs::INODE_APPEND;
    }

    if (f & btrfs::INODE_NODUMP) {
        if (!ret.empty())
            ret += ",";

        ret += "nodump";
        f &= ~btrfs::INODE_NODUMP;
    }

    if (f & btrfs::INODE_NOATIME) {
        if (!ret.empty())
            ret += ",";

        ret += "noatime";
        f &= ~btrfs::INODE_NOATIME;
    }

    if (f & btrfs::INODE_DIRSYNC) {
        if (!ret.empty())
            ret += ",";

        ret += "dirsync";
        f &= ~btrfs::INODE_DIRSYNC;
    }

    if (f & btrfs::INODE_COMPRESS) {
        if (!ret.empty())
            ret += ",";

        ret += "compress";
        f &= ~btrfs::INODE_COMPRESS;
    }

    if (f & btrfs::INODE_ROOT_ITEM_INIT) {
        if (!ret.empty())
            ret += ",";

        ret += "root_item_init";
        f &= ~btrfs::INODE_ROOT_ITEM_INIT;
    }

    if (f & (btrfs::INODE_RO_VERITY << 32)) {
        if (!ret.empty())
            ret += ",";

        ret += "ro_verity";
        f &= ~(btrfs::INODE_RO_VERITY << 32);
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::inode_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::inode_item& ii, format_context& ctx) const {
        return format_to(ctx.out(), "generation={:x} transid={:x} size={:x} nbytes={:x} block_group={:x} nlink={:x} uid={:x} gid={:x} mode={:o} rdev={:x} flags={} sequence={:x}"" atime={} ctime={} mtime={} otime={}",
                         ii.generation, ii.transid, ii.size, ii.nbytes, ii.block_group, ii.nlink, ii.uid, ii.gid, ii.mode,
                         ii.rdev, inode_item_flags(ii.flags), ii.sequence, ii.atime, ii.ctime, ii.mtime, ii.otime);
    }
};

string root_item_flags(uint64_t f) {
    string ret;

    if (f & btrfs::ROOT_SUBVOL_DEAD) {
        ret = "dead";
        f &= ~btrfs::ROOT_SUBVOL_DEAD;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::root_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::root_item& ri, format_context& ctx) const {
        return format_to(ctx.out(), "{}; generation={:x} root_dirid={:x} bytenr={:x} byte_limit={:x} bytes_used={:x} last_snapshot={:x} flags={} refs={:x} drop_progress={} drop_level={:x} level={:x} generation_v2={:x} uuid={} parent_uuid={} received_uuid={} ctransid={:x} otransid={:x} stransid={:x} rtransid={:x} ctime={} otime={} stime={} rtime={}",
                         ri.inode, ri.generation, ri.root_dirid, ri.bytenr, ri.byte_limit, ri.bytes_used, ri.last_snapshot, root_item_flags(ri.flags), ri.refs, ri.drop_progress, ri.drop_level, ri.level, ri.generation_v2, ri.uuid, ri.parent_uuid, ri.received_uuid, ri.ctransid, ri.otransid, ri.stransid, ri.rtransid, ri.ctime, ri.otime, ri.stime, ri.rtime);
    }
};

template<>
struct std::formatter<btrfs::inode_ref> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::inode_ref& ir, format_context& ctx) const {
        return format_to(ctx.out(), "index={:x} name_len={:x} name={}",
                         ir.index, ir.name_len,
                         string_view((char*)&ir + sizeof(btrfs::inode_ref), ir.name_len));
    }
};

template<>
struct std::formatter<btrfs::inode_extref> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::inode_extref& ier, format_context& ctx) const {
        return format_to(ctx.out(), "parent_objectid={:x} index={:x} name_len={:x} name={}",
                         ier.parent_objectid, ier.index, ier.name_len,
                         string_view(ier.name, ier.name_len));
    }
};

template<>
struct std::formatter<enum btrfs::dir_item_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::dir_item_type t, format_context& ctx) const {
        switch (t) {
            using enum btrfs::dir_item_type;

            case unknown:
                return format_to(ctx.out(), "unknown");
            case reg_file:
                return format_to(ctx.out(), "reg_file");
            case dir:
                return format_to(ctx.out(), "dir");
            case chrdev:
                return format_to(ctx.out(), "chrdev");
            case blkdev:
                return format_to(ctx.out(), "blkdev");
            case fifo:
                return format_to(ctx.out(), "fifo");
            case sock:
                return format_to(ctx.out(), "sock");
            case symlink:
                return format_to(ctx.out(), "symlink");
            case xattr:
                return format_to(ctx.out(), "xattr");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<btrfs::dir_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::dir_item& di, format_context& ctx) const {
        auto ret = format_to(ctx.out(), "location={:x} transid={:x} data_len={:x} name_len={:x} type={} name={}",
                             di.location, di.transid, di.data_len, di.name_len, di.type,
                             string_view((char*)&di + sizeof(btrfs::dir_item), di.name_len));

        if (di.data_len != 0)
            ret = format_to(ret, " data={}", string_view((char*)&di + sizeof(btrfs::dir_item) + di.name_len, di.data_len));

        return ret;
    }
};

template<>
struct std::formatter<btrfs::block_group_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::block_group_item& bgi, format_context& ctx) const {
        return format_to(ctx.out(), "used={:x} chunk_objectid={:x} flags={}",
                         bgi.used, bgi.chunk_objectid, block_group_item_flags(bgi.flags));
    }
};

template<>
struct std::formatter<btrfs::dev_extent> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::dev_extent& de, format_context& ctx) const {
        return format_to(ctx.out(), "chunk_tree={:x} chunk_objectid={:x} chunk_offset={:x} length={:x} chunk_tree_uuid={}",
                         de.chunk_tree, de.chunk_objectid, de.chunk_offset,
                         de.length, de.chunk_tree_uuid);
    }
};

string free_space_info_flags(uint32_t f) {
    string ret;

    if (f & btrfs::FREE_SPACE_USING_BITMAPS) {
        ret += "using_bitmaps";
        f &= ~btrfs::FREE_SPACE_USING_BITMAPS;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::free_space_info> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::free_space_info& fsi, format_context& ctx) const {
        return format_to(ctx.out(), "extent_count={:x} flags={:x}",
                         fsi.extent_count, fsi.flags);
    }
};

string extent_item_flags(uint64_t f) {
    string ret;

    if (f & btrfs::EXTENT_FLAG_DATA) {
        ret += "data";
        f &= ~btrfs::EXTENT_FLAG_DATA;
    }

    if (f & btrfs::EXTENT_FLAG_TREE_BLOCK) {
        if (!ret.empty())
            ret += ",";

        ret += "tree_block";
        f &= ~btrfs::EXTENT_FLAG_TREE_BLOCK;
    }

    if (f & btrfs::BLOCK_FLAG_FULL_BACKREF) {
        if (!ret.empty())
            ret += ",";

        ret += "full_backref";
        f &= ~btrfs::BLOCK_FLAG_FULL_BACKREF;
    }

    if (ret.empty())
        ret += format("{:x}", f);
    else if (f != 0)
        ret += format(",{:x}", f);

    return ret;
}

template<>
struct std::formatter<btrfs::extent_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::extent_item& ei, format_context& ctx) const {
        return format_to(ctx.out(), "refs={:x} generation={:x} flags={}",
                         ei.refs, ei.generation, extent_item_flags(ei.flags));
    }
};

template<>
struct std::formatter<btrfs::tree_block_info> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::tree_block_info& tbi, format_context& ctx) const {
        return format_to(ctx.out(), "key={:x} level={:x}",
                         tbi.key, tbi.level);
    }
};

template<>
struct std::formatter<btrfs::extent_data_ref> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::extent_data_ref& edr, format_context& ctx) const {
        return format_to(ctx.out(), "root={:x} objectid={:x} offset={:x} count={:x}",
                         edr.root, edr.objectid, edr.offset, edr.count);
    }
};

template<>
struct std::formatter<btrfs::shared_data_ref> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::shared_data_ref& sdr, format_context& ctx) const {
        return format_to(ctx.out(), "count={:x}", sdr.count);
    }
};

template<>
struct std::formatter<enum btrfs::file_extent_item_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::file_extent_item_type t, format_context& ctx) const {
        switch (t) {
            using enum btrfs::file_extent_item_type;

            case inline_extent:
                return format_to(ctx.out(), "inline");
            case reg:
                return format_to(ctx.out(), "reg");
            case prealloc:
                return format_to(ctx.out(), "prealloc");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<enum btrfs::compression_type> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::compression_type t, format_context& ctx) const {
        switch (t) {
            using enum btrfs::compression_type;

            case none:
                return format_to(ctx.out(), "none");
            case zlib:
                return format_to(ctx.out(), "zlib");
            case lzo:
                return format_to(ctx.out(), "lzo");
            case zstd:
                return format_to(ctx.out(), "zstd");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<btrfs::file_extent_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::file_extent_item& fei, format_context& ctx) const {
        auto ret = format_to(ctx.out(), "generation={:x} ram_bytes={:x} compression={} encryption={:x} other_encoding={:x} type={}",
                         fei.generation, fei.ram_bytes, fei.compression, fei.encryption,
                         fei.other_encoding, fei.type);

        if (fei.type != btrfs::file_extent_item_type::inline_extent) {
            ret = format_to(ret, " disk_bytenr={:x} disk_num_bytes={:x} offset={:x} num_bytes={:x}",
                            fei.disk_bytenr, fei.disk_num_bytes, fei.offset,
                            fei.num_bytes);
        }

        return ret;
    }
};

template<>
struct std::formatter<btrfs::key_ptr> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::key_ptr& p, format_context& ctx) const {
        return format_to(ctx.out(), "{:x} blockptr={:x} generation={:x}",
                         p.key, p.blockptr, p.generation);
    }
};

template<>
struct std::formatter<btrfs::root_ref> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::root_ref& rr, format_context& ctx) const {
        return format_to(ctx.out(), "dirid={:x} sequence={:x} name_len={:x} name={}",
                         rr.dirid, rr.sequence, rr.name_len,
                         string_view((char*)&rr + sizeof(btrfs::root_ref), rr.name_len));
    }
};

template<>
struct std::formatter<btrfs::free_space_header> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::free_space_header& fsh, format_context& ctx) const {
        return format_to(ctx.out(), "location=({:x}) generation={:x} num_entries={:x} num_bitmaps={:x}",
                         fsh.location, fsh.generation, fsh.num_entries,
                         fsh.num_bitmaps);
    }
};

template<>
struct std::formatter<btrfs::verity_descriptor_item> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::verity_descriptor_item& vdi, format_context& ctx) const {
        return format_to(ctx.out(), "size={:x} encryption={:x}",
                         vdi.size, vdi.encryption);
    }
};

template<>
struct std::formatter<enum btrfs::fsverity_hash_algorithm> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum btrfs::fsverity_hash_algorithm t, format_context& ctx) const {
        switch (t) {
            using enum btrfs::fsverity_hash_algorithm;

            case SHA256:
                return format_to(ctx.out(), "sha256");
            case SHA512:
                return format_to(ctx.out(), "sha512");
            default:
                return format_to(ctx.out(), "{:x}", (uint8_t)t);
        }
    }
};

template<>
struct std::formatter<btrfs::fsverity_descriptor> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(const btrfs::fsverity_descriptor& desc, format_context& ctx) const {
        auto& root_hash = *(array<btrfs::le64, 8>*)desc.root_hash;

        auto ret = format_to(ctx.out(), "version={:x} hash_algorithm={} log_blocksize={:x} salt_size={:x} data_size={:x} ",
                             desc.version, desc.hash_algorithm, desc.log_blocksize,
                             desc.salt_size, desc.data_size);

        if (desc.hash_algorithm == btrfs::fsverity_hash_algorithm::SHA256) {
            ret = format_to(ret, "root_hash={:016x}{:016x}{:016x}{:016x}",
                            root_hash[0], root_hash[1], root_hash[2],
                            root_hash[3]);
        } else {
            ret = format_to(ret, "root_hash={:016x}{:016x}{:016x}{:016x}{:016x}{:016x}{:016x}{:016x}",
                            root_hash[0], root_hash[1], root_hash[2],
                            root_hash[3], root_hash[4], root_hash[5],
                            root_hash[6], root_hash[7]);
        }

        if (desc.salt_size != 0) {
            ret = format_to(ret, " salt=");
            for (unsigned int i = 0; i < desc.salt_size; i++) {
                ret = format_to(ret, "{:02x}", desc.salt[i]);
            }
        }

        return ret;
    }
};
