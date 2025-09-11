#include <iostream>
#include <filesystem>
#include <fstream>
#include <format>

import cxxbtrfs;
import formatted_error;

using namespace std;

static string compat_ro_flags(uint64_t f) {
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

static string incompat_flags(uint64_t f) {
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

static void read_superblock(ifstream& f) {
    btrfs::super_block sb;
    string csum;

    f.seekg(btrfs::superblock_addrs[0]);
    f.read((char*)&sb, sizeof(sb));

    // FIXME - throw exception if no magic

//     ($roottree, $chunktree, $logtree) = unpack("x80QQQ", $sb);
//     @b = unpack("a32a16QQa8QQQQQQQQQVVVVVQQQQvCCCa98A256QQa16x224a2048a672", $sb);
//     @di = unpack("QQQVVVQQQVCCa16a16", $b[27]);

    switch (sb.csum_type) {
        case btrfs::csum_type::CRC32:
            csum = format("{:08x}", *(uint32_t*)sb.csum.data());
            break;

        case btrfs::csum_type::XXHASH:
            csum = format("{:016x}", *(uint64_t*)sb.csum.data());
            break;

        case btrfs::csum_type::SHA256:
        case btrfs::csum_type::BLAKE2:
            csum = format("{:016x}{:016x}{:016x}{:016x}", *(uint64_t*)&sb.csum[0],
                          *(uint64_t*)&sb.csum[sizeof(uint64_t)], *(uint64_t*)&sb.csum[2 * sizeof(uint64_t)],
                          *(uint64_t*)&sb.csum[3 * sizeof(uint64_t)]);
        break;

        default:
            throw formatted_error("unrecognized csum type {}", sb.csum_type);
    }

    // array<uint8_t, 32> csum;
    // uuid fsid;
    // le64 bytenr;
// le64 flags;
// le64 magic;
    // le64 generation;
    // le64 root;
    // le64 chunk_root;
    // le64 log_root;
    // le64 __unused_log_root_transid;
    // le64 total_bytes;
    // le64 bytes_used;
    // le64 root_dir_objectid;
    // le64 num_devices;
    // le32 sectorsize;
    // le32 nodesize;
    // le32 __unused_leafsize;
    // le32 stripesize;
    // le32 sys_chunk_array_size;
    // le64 chunk_root_generation;
    // le64 compat_flags;
    // le64 compat_ro_flags;
    // le64 incompat_flags;
    // enum csum_type csum_type;
    // uint8_t root_level;
    // uint8_t chunk_root_level;
    // uint8_t log_root_level;
    // btrfs::dev_item dev_item;
    // array<char, 0x100> label;
    // le64 cache_generation;
    // le64 uuid_tree_generation;
    // uuid metadata_uuid;

    auto label = string_view(sb.label.data(), sizeof(sb.label));

    if (auto nul = label.find_first_of('\x00'); nul != string_view::npos)
        label = label.substr(0, nul);

    cout << format("superblock csum={} fsid={} bytenr={:x} flags=%s magic=%s generation={:x} root={:x} chunk_root={:x} log_root={:x} log_root_transid={:x} total_bytes={:x} bytes_used={:x} root_dir_objectid={:x} num_devices={:x} sectorsize={:x} nodesize={:x} leafsize={:x} stripesize={:x} sys_chunk_array_size={:x} chunk_root_generation={:x} compat_flags={:x} compat_ro_flags={} incompat_flags={} csum_type={} root_level={:x} chunk_root_level={:x} log_root_level={:x} (dev_item {}) label={} cache_generation={:x} uuid_tree_generation={:x} metadata_uuid={}", csum, sb.fsid, sb.bytenr/*, format_super_flags(b[3]), b[4]*/, sb.generation, sb.root, sb.chunk_root, sb.log_root, sb.__unused_log_root_transid, sb.total_bytes, sb.bytes_used, sb.root_dir_objectid, sb.num_devices, sb.sectorsize, sb.nodesize, sb.__unused_leafsize, sb.stripesize, sb.sys_chunk_array_size, sb.chunk_root_generation, sb.compat_flags, compat_ro_flags(sb.compat_ro_flags), incompat_flags(sb.incompat_flags), sb.csum_type, sb.root_level, sb.chunk_root_level, sb.log_root_level, sb.dev_item, label, sb.cache_generation, sb.uuid_tree_generation, sb.metadata_uuid) << endl;

    // FIXME - nr_global_roots;
    // FIXME - remap_root;
    // FIXME - remap_root_generation;
    // FIXME - remap_root_level;

//     my $devid = format_uuid($di[12]);
//
//     $blocksize = $b[14];
//     $nodesize = $b[15];
//
//     $devs{$di[0]} = $f;
//
//     my $bootstrap = substr($b[32], 0, $b[18]);
//
//     while (length($bootstrap) > 0) {
//         #print Dumper($bootstrap)."\n";
//         @b2 = unpack("QCQ", $bootstrap);
//         printf("bootstrap %x,%x,%x\n", @b2[0], @b2[1], @b2[2]);
//         $bootstrap = substr($bootstrap, 0x11);
//
//         my @c = unpack("QQQQVVVvv", $bootstrap);
//         dump_item(0xe4, substr($bootstrap, 0, 0x30 + ($c[7] * 0x20)), "", 0);
//
//         $bootstrap = substr($bootstrap, 0x30);
//
//         my %obj;
//
//         $obj{'offset'} = $b2[2];
//         $obj{'size'} = $c[0];
//         $obj{'type'} = $c[3];
//         $obj{'num_stripes'} = $c[7];
//         $obj{'stripe_len'} = $c[2];
//         $obj{'sub_stripes'} = $c[8];
//
//         for (my $i = 0; $i < $c[7]; $i++) {
//             my @cis = unpack("QQa16", $bootstrap);
//             $bootstrap = substr($bootstrap, 0x20);
//
//             $obj{'stripes'}[$i]{'physoffset'} = $cis[1];
//             $obj{'stripes'}[$i]{'devid'} = $cis[0];
//         }
//
//         push @l2p_bs, \%obj;
//     }
//
//     my $backups = $b[33];
//
//     while (length($backups) > 0) {
//         my $backup = substr($backups, 0, 168);
//         $backups = substr($backups, 168);
//
//         my @b3 = unpack("QQQQQQQQQQQQQQQx32CCCCCCx10", $backup);
//
//         printf("backup tree_root=%x tree_root_gen=%x chunk_root=%x chunk_root_gen=%x extent_root=%x extent_root_gen=%x fs_root=%x fs_root_gen=%x dev_root=%x dev_root_gen=%x csum_root=%x csum_root_gen=%x total_bytes=%x bytes_used=%x num_devices=%x tree_root_level=%x chunk_root_level=%x extent_root_level=%x fs_root_level=%x dev_root_level=%x csum_root_level=%x\n", @b3);
//     }
//
//     print "\n";
}

static void dump(const filesystem::path& fn) {
    ifstream f(fn);

    if (f.fail())
        throw formatted_error("Failed to open {}", fn.string()); // FIXME - include why

    read_superblock(f);

    // FIXME
}

int main() {
    // FIXME - solicit filename
    // FIXME - use libblkid to find other devices

    try {
        dump("test");
    } catch (const exception& e) {
        cerr << "Exception: " << e.what() << endl;
    }
}
