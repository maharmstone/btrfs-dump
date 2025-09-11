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

static string format_super_flags(uint64_t f) {
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

    auto label = string_view(sb.label.data(), sizeof(sb.label));
    auto magic = string_view((char*)&sb.magic, sizeof(sb.magic));

    if (auto nul = label.find_first_of('\x00'); nul != string_view::npos)
        label = label.substr(0, nul);

    cout << format("superblock csum={} fsid={} bytenr={:x} flags={} magic={} generation={:x} root={:x} chunk_root={:x} log_root={:x} log_root_transid={:x} total_bytes={:x} bytes_used={:x} root_dir_objectid={:x} num_devices={:x} sectorsize={:x} nodesize={:x} leafsize={:x} stripesize={:x} sys_chunk_array_size={:x} chunk_root_generation={:x} compat_flags={:x} compat_ro_flags={} incompat_flags={} csum_type={} root_level={:x} chunk_root_level={:x} log_root_level={:x} (dev_item {}) label={} cache_generation={:x} uuid_tree_generation={:x} metadata_uuid={}", csum, sb.fsid, sb.bytenr, format_super_flags(sb.flags), magic, sb.generation, sb.root, sb.chunk_root, sb.log_root, sb.__unused_log_root_transid, sb.total_bytes, sb.bytes_used, sb.root_dir_objectid, sb.num_devices, sb.sectorsize, sb.nodesize, sb.__unused_leafsize, sb.stripesize, sb.sys_chunk_array_size, sb.chunk_root_generation, sb.compat_flags, compat_ro_flags(sb.compat_ro_flags), incompat_flags(sb.incompat_flags), sb.csum_type, sb.root_level, sb.chunk_root_level, sb.log_root_level, sb.dev_item, label, sb.cache_generation, sb.uuid_tree_generation, sb.metadata_uuid) << endl;

    // FIXME - nr_global_roots
    // FIXME - remap_root
    // FIXME - remap_root_generation
    // FIXME - remap_root_level

//     $blocksize = $b[14];
//     $nodesize = $b[15];
//
//     $devs{$di[0]} = $f;

    auto bootstrap = span(sb.sys_chunk_array.data(), sb.sys_chunk_array_size);

    while (!bootstrap.empty()) {
        const auto& k = *(btrfs::key*)bootstrap.data();

        cout << format("bootstrap {}\n", k);

        bootstrap = bootstrap.subspan(sizeof(btrfs::key));

        const auto& c = *(btrfs::chunk*)bootstrap.data();

        cout << format("chunk_item {}", c) << endl;

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

        bootstrap = bootstrap.subspan(offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)));

//         push @l2p_bs, \%obj;
    }

    for (const auto& b : sb.super_roots) {
        cout << format("backup {}", b) << endl;
    }
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
