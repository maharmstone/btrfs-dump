#include <iostream>
#include <fstream>
#include <format>
#include <map>
#include <vector>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <getopt.h>
#include <string.h>
#include "config.h"

import cxxbtrfs;
import formatted_error;
import b64;
import crc32c;
import xxhash;
import sha256;
import blake2b;

using namespace std;

static uint8_t hex_char(char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    else if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    else if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;

    throw formatted_error("invalid hex character '{}'", c);
}

static vector<uint8_t> parse_hex_bytes(string_view sv) {
    vector<uint8_t> ret;

    if (sv.size() % 2 != 0)
        throw formatted_error("hex string has odd length");

    ret.reserve(sv.size() / 2);

    for (size_t i = 0; i < sv.size(); i += 2) {
        ret.push_back((hex_char(sv[i]) << 4) | hex_char(sv[i + 1]));
    }

    return ret;
}

template<integral T>
static T parse_hex(string_view sv) {
    T val;

    auto [ptr, ec] = from_chars(sv.data(), sv.data() + sv.size(), val, 16);

    if (ec != errc{} || ptr != sv.data() + sv.size())
        throw formatted_error("could not parse hex value '{}'", sv);

    return val;
}

static uint32_t parse_octal(string_view sv) {
    uint32_t val;

    auto [ptr, ec] = from_chars(sv.data(), sv.data() + sv.size(), val, 8);

    if (ec != errc{} || ptr != sv.data() + sv.size())
        throw formatted_error("could not parse octal value '{}'", sv);

    return val;
}

static btrfs::uuid parse_uuid(string_view sv) {
    btrfs::uuid u;
    size_t pos = 0;

    for (size_t i = 0; i < 16; i++) {
        if (pos < sv.size() && sv[pos] == '-')
            pos++;

        if (pos + 2 > sv.size())
            throw formatted_error("UUID too short: '{}'", sv);

        u.uuid[i] = (hex_char(sv[pos]) << 4) | hex_char(sv[pos + 1]);
        pos += 2;
    }

    return u;
}

static btrfs::timespec parse_timestamp(string_view sv) {
    // Format: YYYY-MM-DDTHH:MM:SS
    if (sv.size() < 19)
        throw formatted_error("timestamp too short: '{}'", sv);

    auto parse_int = [&](string_view s) {
        int val;

        auto [ptr, ec] = from_chars(s.data(), s.data() + s.size(), val);
        if (ec != errc{})
            throw formatted_error("could not parse integer in timestamp");

        return val;
    };

    auto year = parse_int(sv.substr(0, 4));
    auto month = parse_int(sv.substr(5, 2));
    auto day = parse_int(sv.substr(8, 2));
    auto hour = parse_int(sv.substr(11, 2));
    auto minute = parse_int(sv.substr(14, 2));
    auto second = parse_int(sv.substr(17, 2));

    auto ymd = chrono::year{year} / chrono::month{(unsigned int)month} / chrono::day{(unsigned int)day};
    auto tp = chrono::sys_days{ymd};
    auto secs = chrono::seconds{(hour * 3600) + (minute * 60) + second};

    btrfs::timespec ts;

    ts.sec = chrono::duration_cast<chrono::seconds>(tp.time_since_epoch()).count() + secs.count();
    ts.nsec = 0;

    return ts;
}

static string unescape_name(string_view sv) {
    string result;

    result.reserve(sv.size());

    for (size_t i = 0; i < sv.size(); i++) {
        if (sv[i] == '\\' && i + 1 < sv.size()) {
            i++;

            if (sv[i] == 'r')
                result += '\r';
            else if (sv[i] == 'n')
                result += '\n';
            else if (sv[i] == 'x' && i + 2 < sv.size()) {
                result += (char)((hex_char(sv[i + 1]) << 4) | hex_char(sv[i + 2]));
                i += 2;
            } else
                result += sv[i];
        } else
            result += sv[i];
    }

    return result;
}

static size_t find_unescaped_space(string_view sv) {
    for (size_t i = 0; i < sv.size(); i++) {
        if (sv[i] == '\\' && i + 1 < sv.size()) {
            i++; // skip escaped character
            continue;
        }

        if (sv[i] == ' ')
            return i;
    }

    return string_view::npos;
}

static pair<string_view, string_view> next_field(string_view& line) {
    // skip leading whitespace
    while (!line.empty() && line.front() == ' ') {
        line.remove_prefix(1);
    }

    if (line.empty())
        return {"", ""};

    // find the boundary of the current token (next unescaped space)

    string_view token, name;

    if (auto sp = find_unescaped_space(line); sp != string_view::npos)
        token = line.substr(0, sp);
    else
        token = line;

    if (auto eq = token.find('='); eq != string_view::npos) {
        name = token.substr(0, eq);
        line.remove_prefix(eq + 1);
    } else {
        line = line.substr(token.size());

        return {token, ""};
    }

    // check for value in brackets like (x,y,z)

    if (!line.empty() && line.front() == '(') {
        auto close = line.find(')');
        if (close == string_view::npos)
            throw formatted_error("unclosed bracket");

        auto val = line.substr(1, close - 1);
        line.remove_prefix(close + 1);

        return {name, val};
    }

    if (auto sp = find_unescaped_space(line); sp != string_view::npos) {
        auto val = line.substr(0, sp);

        line = line.substr(sp);

        return {name, val};
    } else {
        auto val = line;

        line = "";

        return {name, val};
    }
}

static btrfs::key_type parse_key_type(string_view sv) {
    if (sv == "INODE_ITEM")
        return btrfs::key_type::INODE_ITEM;
    else if (sv == "INODE_REF")
        return btrfs::key_type::INODE_REF;
    else if (sv == "INODE_EXTREF")
        return btrfs::key_type::INODE_EXTREF;
    else if (sv == "XATTR_ITEM")
        return btrfs::key_type::XATTR_ITEM;
    else if (sv == "VERITY_DESC_ITEM")
        return btrfs::key_type::VERITY_DESC_ITEM;
    else if (sv == "VERITY_MERKLE_ITEM")
        return btrfs::key_type::VERITY_MERKLE_ITEM;
    else if (sv == "ORPHAN_ITEM")
        return btrfs::key_type::ORPHAN_ITEM;
    else if (sv == "DIR_LOG_INDEX")
        return btrfs::key_type::DIR_LOG_INDEX;
    else if (sv == "DIR_ITEM")
        return btrfs::key_type::DIR_ITEM;
    else if (sv == "DIR_INDEX")
        return btrfs::key_type::DIR_INDEX;
    else if (sv == "EXTENT_DATA")
        return btrfs::key_type::EXTENT_DATA;
    else if (sv == "EXTENT_CSUM")
        return btrfs::key_type::EXTENT_CSUM;
    else if (sv == "ROOT_ITEM")
        return btrfs::key_type::ROOT_ITEM;
    else if (sv == "ROOT_BACKREF")
        return btrfs::key_type::ROOT_BACKREF;
    else if (sv == "ROOT_REF")
        return btrfs::key_type::ROOT_REF;
    else if (sv == "EXTENT_ITEM")
        return btrfs::key_type::EXTENT_ITEM;
    else if (sv == "METADATA_ITEM")
        return btrfs::key_type::METADATA_ITEM;
    else if (sv == "EXTENT_OWNER_REF")
        return btrfs::key_type::EXTENT_OWNER_REF;
    else if (sv == "TREE_BLOCK_REF")
        return btrfs::key_type::TREE_BLOCK_REF;
    else if (sv == "EXTENT_DATA_REF")
        return btrfs::key_type::EXTENT_DATA_REF;
    else if (sv == "SHARED_BLOCK_REF")
        return btrfs::key_type::SHARED_BLOCK_REF;
    else if (sv == "SHARED_DATA_REF")
        return btrfs::key_type::SHARED_DATA_REF;
    else if (sv == "BLOCK_GROUP_ITEM")
        return btrfs::key_type::BLOCK_GROUP_ITEM;
    else if (sv == "FREE_SPACE_INFO")
        return btrfs::key_type::FREE_SPACE_INFO;
    else if (sv == "FREE_SPACE_EXTENT")
        return btrfs::key_type::FREE_SPACE_EXTENT;
    else if (sv == "FREE_SPACE_BITMAP")
        return btrfs::key_type::FREE_SPACE_BITMAP;
    else if (sv == "DEV_EXTENT")
        return btrfs::key_type::DEV_EXTENT;
    else if (sv == "DEV_ITEM")
        return btrfs::key_type::DEV_ITEM;
    else if (sv == "CHUNK_ITEM")
        return btrfs::key_type::CHUNK_ITEM;
    else if (sv == "RAID_STRIPE")
        return btrfs::key_type::RAID_STRIPE;
    else if (sv == "IDENTITY_REMAP")
        return btrfs::key_type::IDENTITY_REMAP;
    else if (sv == "REMAP")
        return btrfs::key_type::REMAP;
    else if (sv == "REMAP_BACKREF")
        return btrfs::key_type::REMAP_BACKREF;
    else if (sv == "QGROUP_STATUS")
        return btrfs::key_type::QGROUP_STATUS;
    else if (sv == "QGROUP_INFO")
        return btrfs::key_type::QGROUP_INFO;
    else if (sv == "QGROUP_LIMIT")
        return btrfs::key_type::QGROUP_LIMIT;
    else if (sv == "QGROUP_RELATION")
        return btrfs::key_type::QGROUP_RELATION;
    else if (sv == "TEMPORARY_ITEM")
        return btrfs::key_type::TEMPORARY_ITEM;
    else if (sv == "PERSISTENT_ITEM")
        return btrfs::key_type::PERSISTENT_ITEM;
    else if (sv == "DEV_REPLACE")
        return btrfs::key_type::DEV_REPLACE;
    else if (sv == "UUID_SUBVOL")
        return btrfs::key_type::UUID_SUBVOL;
    else if (sv == "UUID_RECEIVED_SUBVOL")
        return btrfs::key_type::UUID_RECEIVED_SUBVOL;
    else if (sv == "STRING_ITEM")
        return btrfs::key_type::STRING_ITEM;
    else
        return (btrfs::key_type)parse_hex<uint8_t>(sv);
}

static btrfs::key parse_key(string_view sv) {
    btrfs::key k;

    auto c1 = sv.find(',');
    if (c1 == string_view::npos)
        throw formatted_error("invalid key format: '{}'", sv);

    k.objectid = parse_hex<uint64_t>(sv.substr(0, c1));

    auto rest = sv.substr(c1 + 1);
    auto c2 = rest.find(',');

    if (c2 == string_view::npos)
        throw formatted_error("invalid key format: '{}'", sv);

    k.type = parse_key_type(rest.substr(0, c2));
    k.offset = parse_hex<uint64_t>(rest.substr(c2 + 1));

    return k;
}

static uint64_t parse_block_group_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "data")
            flags |= btrfs::BLOCK_GROUP_DATA;
        else if (token == "system")
            flags |= btrfs::BLOCK_GROUP_SYSTEM;
        else if (token == "metadata")
            flags |= btrfs::BLOCK_GROUP_METADATA;
        else if (token == "raid0")
            flags |= btrfs::BLOCK_GROUP_RAID0;
        else if (token == "raid1")
            flags |= btrfs::BLOCK_GROUP_RAID1;
        else if (token == "dup")
            flags |= btrfs::BLOCK_GROUP_DUP;
        else if (token == "raid10")
            flags |= btrfs::BLOCK_GROUP_RAID10;
        else if (token == "raid5")
            flags |= btrfs::BLOCK_GROUP_RAID5;
        else if (token == "raid6")
            flags |= btrfs::BLOCK_GROUP_RAID6;
        else if (token == "raid1c3")
            flags |= btrfs::BLOCK_GROUP_RAID1C3;
        else if (token == "raid1c4")
            flags |= btrfs::BLOCK_GROUP_RAID1C4;
        else if (token == "remapped")
            flags |= btrfs::BLOCK_GROUP_REMAPPED;
        else if (token == "metadata_remap")
            flags |= btrfs::BLOCK_GROUP_METADATA_REMAP;
        else if (token == "stripe_removal_pending")
            flags |= btrfs::BLOCK_GROUP_STRIPE_REMOVAL_PENDING;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_inode_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "nodatasum")
            flags |= btrfs::INODE_NODATASUM;
        else if (token == "nodatacow")
            flags |= btrfs::INODE_NODATACOW;
        else if (token == "readonly")
            flags |= btrfs::INODE_READONLY;
        else if (token == "nocompress")
            flags |= btrfs::INODE_NOCOMPRESS;
        else if (token == "prealloc")
            flags |= btrfs::INODE_PREALLOC;
        else if (token == "sync")
            flags |= btrfs::INODE_SYNC;
        else if (token == "immutable")
            flags |= btrfs::INODE_IMMUTABLE;
        else if (token == "append")
            flags |= btrfs::INODE_APPEND;
        else if (token == "nodump")
            flags |= btrfs::INODE_NODUMP;
        else if (token == "noatime")
            flags |= btrfs::INODE_NOATIME;
        else if (token == "dirsync")
            flags |= btrfs::INODE_DIRSYNC;
        else if (token == "compress")
            flags |= btrfs::INODE_COMPRESS;
        else if (token == "root_item_init")
            flags |= btrfs::INODE_ROOT_ITEM_INIT;
        else if (token == "ro_verity")
            flags |= (btrfs::INODE_RO_VERITY << 32);
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_header_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "written")
            flags |= btrfs::HEADER_FLAG_WRITTEN;
        else if (token == "reloc")
            flags |= btrfs::HEADER_FLAG_RELOC;
        else if (token == "mixed_backref")
            flags |= btrfs::HEADER_FLAG_MIXED_BACKREF;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_super_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "written")
            flags |= btrfs::HEADER_FLAG_WRITTEN;
        else if (token == "reloc")
            flags |= btrfs::HEADER_FLAG_RELOC;
        else if (token == "error")
            flags |= btrfs::SUPER_FLAG_ERROR;
        else if (token == "seeding")
            flags |= btrfs::SUPER_FLAG_SEEDING;
        else if (token == "metadump")
            flags |= btrfs::SUPER_FLAG_METADUMP;
        else if (token == "metadump_v2")
            flags |= btrfs::SUPER_FLAG_METADUMP_V2;
        else if (token == "changing_fsid")
            flags |= btrfs::SUPER_FLAG_CHANGING_FSID;
        else if (token == "changing_fsid_v2")
            flags |= btrfs::SUPER_FLAG_CHANGING_FSID_V2;
        else if (token == "changing_bg_tree")
            flags |= btrfs::SUPER_FLAG_CHANGING_BG_TREE;
        else if (token == "changing_data_csum")
            flags |= btrfs::SUPER_FLAG_CHANGING_DATA_CSUM;
        else if (token == "changing_meta_csum")
            flags |= btrfs::SUPER_FLAG_CHANGING_META_CSUM;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_incompat_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "mixed_backref")
            flags |= btrfs::FEATURE_INCOMPAT_MIXED_BACKREF;
        else if (token == "default_subvol")
            flags |= btrfs::FEATURE_INCOMPAT_DEFAULT_SUBVOL;
        else if (token == "mixed_groups")
            flags |= btrfs::FEATURE_INCOMPAT_MIXED_GROUPS;
        else if (token == "compress_lzo")
            flags |= btrfs::FEATURE_INCOMPAT_COMPRESS_LZO;
        else if (token == "compress_zstd")
            flags |= btrfs::FEATURE_INCOMPAT_COMPRESS_ZSTD;
        else if (token == "big_metadata")
            flags |= btrfs::FEATURE_INCOMPAT_BIG_METADATA;
        else if (token == "extended_iref")
            flags |= btrfs::FEATURE_INCOMPAT_EXTENDED_IREF;
        else if (token == "raid56")
            flags |= btrfs::FEATURE_INCOMPAT_RAID56;
        else if (token == "skinny_metadata")
            flags |= btrfs::FEATURE_INCOMPAT_SKINNY_METADATA;
        else if (token == "no_holes")
            flags |= btrfs::FEATURE_INCOMPAT_NO_HOLES;
        else if (token == "metadata_uuid")
            flags |= btrfs::FEATURE_INCOMPAT_METADATA_UUID;
        else if (token == "raid1c34")
            flags |= btrfs::FEATURE_INCOMPAT_RAID1C34;
        else if (token == "zoned")
            flags |= btrfs::FEATURE_INCOMPAT_ZONED;
        else if (token == "extent_tree_v2")
            flags |= btrfs::FEATURE_INCOMPAT_EXTENT_TREE_V2;
        else if (token == "raid_stripe_tree")
            flags |= btrfs::FEATURE_INCOMPAT_RAID_STRIPE_TREE;
        else if (token == "simple_quota")
            flags |= btrfs::FEATURE_INCOMPAT_SIMPLE_QUOTA;
        else if (token == "remap_tree")
            flags |= btrfs::FEATURE_INCOMPAT_REMAP_TREE;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_compat_ro_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "free_space_tree")
            flags |= btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE;
        else if (token == "free_space_tree_valid")
            flags |= btrfs::FEATURE_COMPAT_RO_FREE_SPACE_TREE_VALID;
        else if (token == "verity")
            flags |= btrfs::FEATURE_COMPAT_RO_VERITY;
        else if (token == "block_group_tree")
            flags |= btrfs::FEATURE_COMPAT_RO_BLOCK_GROUP_TREE;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_extent_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "data")
            flags |= btrfs::EXTENT_FLAG_DATA;
        else if (token == "tree_block")
            flags |= btrfs::EXTENT_FLAG_TREE_BLOCK;
        else if (token == "full_backref")
            flags |= btrfs::BLOCK_FLAG_FULL_BACKREF;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_root_item_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "dead")
            flags |= btrfs::ROOT_SUBVOL_DEAD;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint32_t parse_free_space_info_flags(string_view sv) {
    uint32_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "using_bitmaps")
            flags |= btrfs::FREE_SPACE_USING_BITMAPS;
        else
            flags |= parse_hex<uint32_t>(token);
    }

    return flags;
}

static uint64_t parse_qgroup_status_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "on")
            flags |= btrfs::QGROUP_STATUS_FLAG_ON;
        else if (token == "rescan")
            flags |= btrfs::QGROUP_STATUS_FLAG_RESCAN;
        else if (token == "inconsistent")
            flags |= btrfs::QGROUP_STATUS_FLAG_INCONSISTENT;
        else if (token == "simple_mode")
            flags |= btrfs::QGROUP_STATUS_FLAG_SIMPLE_MODE;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_qgroup_limit_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "max_rfer")
            flags |= btrfs::QGROUP_LIMIT_MAX_RFER;
        else if (token == "max_excl")
            flags |= btrfs::QGROUP_LIMIT_MAX_EXCL;
        else if (token == "rsv_rfer")
            flags |= btrfs::QGROUP_LIMIT_RSV_RFER;
        else if (token == "rsv_excl")
            flags |= btrfs::QGROUP_LIMIT_RSV_EXCL;
        else if (token == "rfer_cmpr")
            flags |= btrfs::QGROUP_LIMIT_RFER_CMPR;
        else if (token == "excl_cmpr")
            flags |= btrfs::QGROUP_LIMIT_EXCL_CMPR;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_balance_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "data")
            flags |= btrfs::BALANCE_DATA;
        else if (token == "system")
            flags |= btrfs::BALANCE_SYSTEM;
        else if (token == "metadata")
            flags |= btrfs::BALANCE_METADATA;
        else if (token == "force")
            flags |= btrfs::BALANCE_FORCE;
        else if (token == "resume")
            flags |= btrfs::BALANCE_RESUME;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static uint64_t parse_balance_args_flags(string_view sv) {
    uint64_t flags = 0;

    while (!sv.empty()) {
        string_view token;

        if (auto comma = sv.find(','); comma != string_view::npos) {
            token = sv.substr(0, comma);
            sv = sv.substr(comma + 1);
        } else {
            token = sv;
            sv = "";
        }

        if (token == "profiles")
            flags |= btrfs::BALANCE_ARGS_PROFILES;
        else if (token == "usage")
            flags |= btrfs::BALANCE_ARGS_USAGE;
        else if (token == "devid")
            flags |= btrfs::BALANCE_ARGS_DEVID;
        else if (token == "drange")
            flags |= btrfs::BALANCE_ARGS_DRANGE;
        else if (token == "vrange")
            flags |= btrfs::BALANCE_ARGS_VRANGE;
        else if (token == "limit")
            flags |= btrfs::BALANCE_ARGS_LIMIT;
        else if (token == "limit_range")
            flags |= btrfs::BALANCE_ARGS_LIMIT_RANGE;
        else if (token == "stripes_range")
            flags |= btrfs::BALANCE_ARGS_STRIPES_RANGE;
        else if (token == "convert")
            flags |= btrfs::BALANCE_ARGS_CONVERT;
        else if (token == "soft")
            flags |= btrfs::BALANCE_ARGS_SOFT;
        else if (token == "usage_range")
            flags |= btrfs::BALANCE_ARGS_USAGE_RANGE;
        else
            flags |= parse_hex<uint64_t>(token);
    }

    return flags;
}

static btrfs::csum_type parse_csum_type(string_view sv) {
    if (sv == "crc32")
        return btrfs::csum_type::CRC32;
    else if (sv == "xxhash")
        return btrfs::csum_type::XXHASH;
    else if (sv == "sha256")
        return btrfs::csum_type::SHA256;
    else if (sv == "blake2")
        return btrfs::csum_type::BLAKE2;
    else
        return (btrfs::csum_type)parse_hex<uint16_t>(sv);
}

static btrfs::dir_item_type parse_dir_item_type(string_view sv) {
    if (sv == "unknown")
        return btrfs::dir_item_type::unknown;
    else if (sv == "reg_file")
        return btrfs::dir_item_type::reg_file;
    else if (sv == "dir")
        return btrfs::dir_item_type::dir;
    else if (sv == "chrdev")
        return btrfs::dir_item_type::chrdev;
    else if (sv == "blkdev")
        return btrfs::dir_item_type::blkdev;
    else if (sv == "fifo")
        return btrfs::dir_item_type::fifo;
    else if (sv == "sock")
        return btrfs::dir_item_type::sock;
    else if (sv == "symlink")
        return btrfs::dir_item_type::symlink;
    else if (sv == "xattr")
        return btrfs::dir_item_type::xattr;
    else
        return (btrfs::dir_item_type)parse_hex<uint8_t>(sv);
}

static btrfs::file_extent_item_type parse_file_extent_type(string_view sv) {
    if (sv == "inline")
        return btrfs::file_extent_item_type::inline_extent;
    else if (sv == "reg")
        return btrfs::file_extent_item_type::reg;
    else if (sv == "prealloc")
        return btrfs::file_extent_item_type::prealloc;
    else
        return (btrfs::file_extent_item_type)parse_hex<uint8_t>(sv);
}

static btrfs::compression_type parse_compression_type(string_view sv) {
    if (sv == "none")
        return btrfs::compression_type::none;
    else if (sv == "zlib")
        return btrfs::compression_type::zlib;
    else if (sv == "lzo")
        return btrfs::compression_type::lzo;
    else if (sv == "zstd")
        return btrfs::compression_type::zstd;
    else
        return (btrfs::compression_type)parse_hex<uint8_t>(sv);
}

static void compute_csum(btrfs::csum_type type, span<const uint8_t> data,
                         span<uint8_t, 32> out) {
    for (unsigned int i = 0; i < 32; i++) {
        out[i] = 0;
    }

    switch (type) {
        case btrfs::csum_type::CRC32: {
            auto crc32 = ~calc_crc32c(0xffffffff, data);
            *(btrfs::le32*)out.data() = crc32;
            break;
        }

        case btrfs::csum_type::XXHASH: {
            auto hash = calc_xxhash64(0, data);
            *(btrfs::le64*)out.data() = hash;
            break;
        }

        case btrfs::csum_type::SHA256: {
            auto hash = calc_sha256(data);
            out = hash;
            break;
        }

        case btrfs::csum_type::BLAKE2: {
            auto hash = calc_blake2b_256(data);
            out = hash;
            break;
        }
    }
}

static void parse_inode_item(string_view line, btrfs::inode_item& ii) {
    memset(&ii, 0, sizeof(ii));

    while (!line.empty()) {
        auto [name, val] = next_field(line);

        if (name.empty())
            break;
        else if (name == "generation")
            ii.generation = parse_hex<uint64_t>(val);
        else if (name == "transid")
            ii.transid = parse_hex<uint64_t>(val);
        else if (name == "size")
            ii.size = parse_hex<uint64_t>(val);
        else if (name == "nbytes")
            ii.nbytes = parse_hex<uint64_t>(val);
        else if (name == "block_group")
            ii.block_group = parse_hex<uint64_t>(val);
        else if (name == "nlink")
            ii.nlink = parse_hex<uint32_t>(val);
        else if (name == "uid")
            ii.uid = parse_hex<uint32_t>(val);
        else if (name == "gid")
            ii.gid = parse_hex<uint32_t>(val);
        else if (name == "mode")
            ii.mode = parse_octal(val);
        else if (name == "rdev")
            ii.rdev = parse_hex<uint64_t>(val);
        else if (name == "flags")
            ii.flags = parse_inode_flags(val);
        else if (name == "sequence")
            ii.sequence = parse_hex<uint64_t>(val);
        else if (name == "atime")
            ii.atime = parse_timestamp(val);
        else if (name == "ctime")
            ii.ctime = parse_timestamp(val);
        else if (name == "mtime")
            ii.mtime = parse_timestamp(val);
        else if (name == "otime")
            ii.otime = parse_timestamp(val);
        else
            throw formatted_error("unrecognized inode_item field '{}'", name);
    }
}

#define MAX_STRIPES 16

struct chunk_entry_stripe {
    uint64_t devid;
    uint64_t offset;
};

struct chunk_entry {
    uint64_t offset; // logical address
    uint64_t length;
    uint16_t num_stripes;
    chunk_entry_stripe stripes[MAX_STRIPES];
};

struct leaf_item {
    btrfs::key key;
    vector<uint8_t> data;
};

struct node_state {
    node_state() {
        memset(&fsid, 0, sizeof(fsid));
        memset(&chunk_tree_uuid, 0, sizeof(chunk_tree_uuid));
    }

    uint64_t bytenr = 0;
    uint8_t level = 0;
    uint64_t generation = 0;
    uint64_t owner = 0;
    uint64_t flags = 0;
    btrfs::uuid fsid;
    btrfs::uuid chunk_tree_uuid;
    bool has_fsid = false;
    bool has_chunk_tree_uuid = false;
    vector<btrfs::key_ptr> key_ptrs;
    vector<leaf_item> items;
};

static vector<uint64_t> resolve_physical(uint64_t log_addr, uint32_t size,
                                         const map<uint64_t, chunk_entry>& chunks) {
    vector<uint64_t> result;

    if (chunks.empty())
        throw formatted_error("no chunk map available for address {:x}", log_addr);

    auto it = chunks.upper_bound(log_addr);
    if (it == chunks.begin())
        throw formatted_error("could not find address {:x} in chunk map", log_addr);

    it--;
    auto& ce = it->second;

    if (log_addr < ce.offset || log_addr + size > ce.offset + ce.length)
        throw formatted_error("address {:x} not fully within chunk at {:x}", log_addr, ce.offset);

    uint64_t offset_in_chunk = log_addr - ce.offset;

    for (uint16_t i = 0; i < ce.num_stripes; i++) {
        result.push_back(ce.stripes[i].offset + offset_in_chunk);
    }

    return result;
}

static void write_node(fstream& out, const node_state& node, const btrfs::super_block& sb,
                        const map<uint64_t, chunk_entry>& chunks) {
    vector<uint8_t> buf(sb.nodesize, 0);

    auto& h = *(btrfs::header*)buf.data();

    h.bytenr = node.bytenr;
    h.generation = node.generation;
    h.owner = node.owner;
    h.flags = node.flags;
    h.level = node.level;

    if (node.has_fsid)
        h.fsid = node.fsid;
    else
        h.fsid = sb.fsid;

    if (node.has_chunk_tree_uuid)
        h.chunk_tree_uuid = node.chunk_tree_uuid;

    if (node.level > 0) {
        h.nritems = node.key_ptrs.size();

        auto ptrs = (btrfs::key_ptr*)(buf.data() + sizeof(btrfs::header));
        for (size_t i = 0; i < h.nritems; i++) {
            ptrs[i] = node.key_ptrs[i];
        }
    } else {
        h.nritems = node.items.size();

        // calculate total data size and check overflow
        size_t items_array_size = node.items.size() * sizeof(btrfs::item);
        size_t total_data_size = 0;

        for (const auto& it : node.items) {
            total_data_size += it.data.size();
        }

        if (sizeof(btrfs::header) + items_array_size + total_data_size > sb.nodesize) {
            throw formatted_error("node at bytenr {:x} overflows: {} bytes of items + {} bytes of data + {} byte header > {} nodesize",
                                  node.bytenr, items_array_size, total_data_size, sizeof(btrfs::header), sb.nodesize);
        }

        // pack items: item array after header, data from end backwards
        uint32_t data_end = sb.nodesize;

        auto item_dest = (btrfs::item*)(buf.data() + sizeof(btrfs::header));

        for (const auto& it : node.items) {
            data_end -= it.data.size();

            item_dest->key = it.key;
            item_dest->offset = data_end - sizeof(btrfs::header);
            item_dest->size = it.data.size();

            memcpy(buf.data() + data_end, it.data.data(), it.data.size());

            item_dest++;
        }
    }

    compute_csum(sb.csum_type, {buf.data() + h.csum.size(), sb.nodesize - h.csum.size()}, h.csum);

    auto phys_addrs = resolve_physical(node.bytenr, sb.nodesize, chunks);
    for (auto phys : phys_addrs) {
        out.seekp(phys);
        out.write((char*)buf.data(), sb.nodesize);
    }
}

static void write_superblock(fstream& out, btrfs::super_block& sb) {
    out.seekp(0, ios::end);
    auto file_size = out.tellp();

    for (unsigned int i = 0; i < sizeof(btrfs::superblock_addrs) / sizeof(*btrfs::superblock_addrs); i++) {
        auto addr = btrfs::superblock_addrs[i];

        if (file_size < (streamoff)(addr + sizeof(btrfs::super_block)))
            break;

        sb.bytenr = addr;

        compute_csum(sb.csum_type, {(uint8_t*)&sb.fsid, sizeof(btrfs::super_block) - sizeof(sb.csum)}, sb.csum);

        out.seekp(addr);
        out.write((char*)&sb, sizeof(sb));
    }
}

static vector<uint8_t> parse_item_data(string_view type_line, const btrfs::key& key,
                                       const btrfs::super_block& sb) {
    vector<uint8_t> data;

    auto append = [&](const void* ptr, size_t len) {
        auto p = (const uint8_t*)ptr;
        data.insert(data.end(), p, p + len);
    };

    string_view type_word, rest;

    if (auto sp = type_line.find(' '); sp != string_view::npos) {
        type_word = type_line.substr(0, sp);
        rest = type_line.substr(sp + 1);
    } else {
        type_word = type_line;
        rest = "";
    }

    if (type_word == "raw")
        return parse_hex_bytes(rest);
    else if (type_word == "inode_item") {
        btrfs::inode_item ii;

        parse_inode_item(rest, ii);

        append(&ii, sizeof(ii));
    } else if (type_word == "inode_ref") {
        // multiple entries: index=X name_len=X name=Y

        while (!rest.empty()) {
            string name;
            btrfs::inode_ref ir;

            memset(&ir, 0, sizeof(ir));

            while (!rest.empty()) {
                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "index")
                    ir.index = parse_hex<uint64_t>(fval);
                else if (fname == "name_len")
                    ir.name_len = parse_hex<uint16_t>(fval);
                else if (fname == "name") {
                    name = unescape_name(fval);
                    break;
                } else
                    throw formatted_error("unrecognized inode_ref field '{}'", fname);
            }

            if (name.empty() && (uint16_t)ir.name_len == 0)
                break;

            append(&ir, sizeof(ir));
            append(name.data(), name.size());
        }
    } else if (type_word == "inode_extref") {
        while (!rest.empty()) {
            string name;
            btrfs::inode_extref ier;

            memset(&ier, 0, sizeof(ier));

            while (!rest.empty()) {
                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "parent_objectid")
                    ier.parent_objectid = parse_hex<uint64_t>(fval);
                else if (fname == "index")
                    ier.index = parse_hex<uint64_t>(fval);
                else if (fname == "name_len")
                    ier.name_len = parse_hex<uint16_t>(fval);
                else if (fname == "name") {
                    name = unescape_name(fval);
                    break;
                } else
                    throw formatted_error("unrecognized inode_extref field '{}'", fname);
            }

            if (name.empty() && (uint16_t)ier.name_len == 0)
                break;

            append(&ier, offsetof(btrfs::inode_extref, name));
            append(name.data(), name.size());
        }
    } else if (type_word == "dir_item" || type_word == "dir_index" ||
               type_word == "xattr_item") {
        // can have multiple entries (dir_item, xattr_item) or single (dir_index)

        while (!rest.empty()) {
            string name;
            vector<uint8_t> di_data;
            btrfs::dir_item di;

            memset(&di, 0, sizeof(di));

            while (!rest.empty()) {
                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "location")
                    di.location = parse_key(fval);
                else if (fname == "transid")
                    di.transid = parse_hex<uint64_t>(fval);
                else if (fname == "data_len")
                    di.data_len = parse_hex<uint16_t>(fval);
                else if (fname == "name_len")
                    di.name_len = parse_hex<uint16_t>(fval);
                else if (fname == "type")
                    di.type = parse_dir_item_type(fval);
                else if (fname == "name") {
                    name = unescape_name(fval);

                    // after name, optionally data=
                    while (!rest.empty()) {
                        auto saved2 = rest;
                        auto [fname2, fval2] = next_field(rest);
                        if (fname2.empty())
                            break;

                        if (fname2 == "data") {
                            auto unescaped = unescape_name(fval2);

                            di_data.assign(unescaped.begin(), unescaped.end());
                        } else {
                            // not data= (could be location= for next entry, or extra=), put it back
                            rest = saved2;
                            break;
                        }
                    }
                    break;
                } else
                    throw formatted_error("unrecognized dir_item field '{}'", fname);
            }

            if (name.empty() && (uint16_t)di.name_len == 0)
                break;

            append(&di, sizeof(di));
            append(name.data(), name.size());
            append(di_data.data(), di_data.size());
        }
    } else if (type_word == "extent_data") {
        btrfs::file_extent_item fei;

        memset(&fei, 0, sizeof(fei));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "generation")
                fei.generation = parse_hex<uint64_t>(fval);
            else if (fname == "ram_bytes")
                fei.ram_bytes = parse_hex<uint64_t>(fval);
            else if (fname == "compression")
                fei.compression = parse_compression_type(fval);
            else if (fname == "encryption")
                fei.encryption = parse_hex<uint8_t>(fval);
            else if (fname == "other_encoding")
                fei.other_encoding = parse_hex<uint16_t>(fval);
            else if (fname == "type")
                fei.type = parse_file_extent_type(fval);
            else if (fname == "disk_bytenr")
                fei.disk_bytenr = parse_hex<uint64_t>(fval);
            else if (fname == "disk_num_bytes")
                fei.disk_num_bytes = parse_hex<uint64_t>(fval);
            else if (fname == "offset")
                fei.offset = parse_hex<uint64_t>(fval);
            else if (fname == "num_bytes")
                fei.num_bytes = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized extent_data field '{}'", fname);
        }

        if (fei.type == btrfs::file_extent_item_type::inline_extent) {
            append(&fei, offsetof(btrfs::file_extent_item, disk_bytenr));

            // inline data would follow - ram_bytes of zero data

            vector<uint8_t> inline_data(fei.ram_bytes, 0);

            append(inline_data.data(), inline_data.size());
        } else
            append(&fei, sizeof(fei));
    } else if (type_word == "extent_csum") {
        // space-separated hex csum values
        while (!rest.empty()) {
            while (!rest.empty() && rest.front() == ' ') {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            string_view token;

            if (auto sp = rest.find(' '); sp != string_view::npos) {
                token = rest.substr(0, sp);
                rest = rest.substr(sp);
            } else {
                token = rest;
                rest = "";
            }

            if (token.empty())
                continue;

            switch (sb.csum_type) {
                case btrfs::csum_type::CRC32: {
                    btrfs::le32 le = parse_hex<uint32_t>(token);
                    append(&le, sizeof(le));
                    break;
                }

                case btrfs::csum_type::XXHASH: {
                    btrfs::le64 le = parse_hex<uint64_t>(token);
                    append(&le, sizeof(le));
                    break;
                }

                case btrfs::csum_type::SHA256:
                case btrfs::csum_type::BLAKE2: {
                    // 64-char hex = 32 bytes
                    auto bytes = parse_hex_bytes(token);
                    append(bytes.data(), bytes.size());
                    break;
                }
            }
        }
    } else if (type_word == "root_item") {
        btrfs::root_item ri;

        memset(&ri, 0, sizeof(ri));

        // format: inode_item fields; root-specific fields

        if (auto sc = rest.find(';'); sc != string_view::npos) {
            auto inode_part = rest.substr(0, sc);

            parse_inode_item(inode_part, ri.inode);

            rest = rest.substr(sc + 1);
        }

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "generation")
                ri.generation = parse_hex<uint64_t>(fval);
            else if (fname == "root_dirid")
                ri.root_dirid = parse_hex<uint64_t>(fval);
            else if (fname == "bytenr")
                ri.bytenr = parse_hex<uint64_t>(fval);
            else if (fname == "byte_limit")
                ri.byte_limit = parse_hex<uint64_t>(fval);
            else if (fname == "bytes_used")
                ri.bytes_used = parse_hex<uint64_t>(fval);
            else if (fname == "last_snapshot")
                ri.last_snapshot = parse_hex<uint64_t>(fval);
            else if (fname == "flags")
                ri.flags = parse_root_item_flags(fval);
            else if (fname == "refs")
                ri.refs = parse_hex<uint32_t>(fval);
            else if (fname == "drop_progress")
                ri.drop_progress = parse_key(fval);
            else if (fname == "drop_level")
                ri.drop_level = parse_hex<uint8_t>(fval);
            else if (fname == "level")
                ri.level = parse_hex<uint8_t>(fval);
            else if (fname == "generation_v2")
                ri.generation_v2 = parse_hex<uint64_t>(fval);
            else if (fname == "uuid")
                ri.uuid = parse_uuid(fval);
            else if (fname == "parent_uuid")
                ri.parent_uuid = parse_uuid(fval);
            else if (fname == "received_uuid")
                ri.received_uuid = parse_uuid(fval);
            else if (fname == "ctransid")
                ri.ctransid = parse_hex<uint64_t>(fval);
            else if (fname == "otransid")
                ri.otransid = parse_hex<uint64_t>(fval);
            else if (fname == "stransid")
                ri.stransid = parse_hex<uint64_t>(fval);
            else if (fname == "rtransid")
                ri.rtransid = parse_hex<uint64_t>(fval);
            else if (fname == "ctime")
                ri.ctime = parse_timestamp(fval);
            else if (fname == "otime")
                ri.otime = parse_timestamp(fval);
            else if (fname == "stime")
                ri.stime = parse_timestamp(fval);
            else if (fname == "rtime")
                ri.rtime = parse_timestamp(fval);
            else
                throw formatted_error("unrecognized root_item field '{}'", fname);
        }

        append(&ri, sizeof(ri));
    } else if (type_word == "root_backref" || type_word == "root_ref") {
        string name;
        btrfs::root_ref rr;

        memset(&rr, 0, sizeof(rr));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "dirid")
                rr.dirid = parse_hex<uint64_t>(fval);
            else if (fname == "sequence")
                rr.sequence = parse_hex<uint64_t>(fval);
            else if (fname == "name_len")
                rr.name_len = parse_hex<uint16_t>(fval);
            else if (fname == "name") {
                name = unescape_name(fval);
                break;
            } else
                throw formatted_error("unrecognized root_ref field '{}'", fname);
        }

        append(&rr, sizeof(rr));
        append(name.data(), name.size());
    } else if (type_word == "extent_item" || type_word == "metadata_item") {
        btrfs::extent_item ei;

        memset(&ei, 0, sizeof(ei));

        // parse: refs=X generation=X flags=names [inline refs...]

        // first parse the extent_item fields
        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "refs")
                ei.refs = parse_hex<uint64_t>(fval);
            else if (fname == "generation")
                ei.generation = parse_hex<uint64_t>(fval);
            else if (fname == "flags") {
                ei.flags = parse_extent_flags(fval);
                break; // inline refs follow after flags
            } else
                throw formatted_error("unrecognized extent_item field '{}'", fname);
        }

        append(&ei, sizeof(ei));

        while (!rest.empty() && rest.front() == ' ') {
            rest.remove_prefix(1);
        }

        if (rest.starts_with("key=")) {
            btrfs::tree_block_info tbi;

            memset(&tbi, 0, sizeof(tbi));

            while (!rest.empty()) {
                auto saved = rest;

                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "key")
                    tbi.key = parse_key(fval);
                else if (fname == "level") {
                    tbi.level = parse_hex<uint8_t>(fval);
                    break;
                } else {
                    rest = saved;
                    break;
                }
            }

            append(&tbi, sizeof(tbi));
        }

        // now parse inline refs
        while (!rest.empty()) {
            while (!rest.empty() && rest.front() == ' ') {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            if (rest.starts_with("extra="))
                break; // will be handled below

            string_view ref_type;

            if (auto sp = rest.find(' '); sp != string_view::npos) {
                ref_type = rest.substr(0, sp);
                rest = rest.substr(sp);
            } else {
                ref_type = rest;
                rest = "";
            }

            if (ref_type == "tree_block_ref") {
                btrfs::extent_inline_ref eir;

                memset(&eir, 0, sizeof(eir));

                eir.type = btrfs::key_type::TREE_BLOCK_REF;

                auto [fname, fval] = next_field(rest);
                if (fname == "root")
                    eir.offset = parse_hex<uint64_t>(fval);
                else
                    throw formatted_error("unrecognized tree_block_ref field '{}'", fname);

                append(&eir, sizeof(eir));
            } else if (ref_type == "shared_block_ref") {
                btrfs::extent_inline_ref eir;

                memset(&eir, 0, sizeof(eir));

                eir.type = btrfs::key_type::SHARED_BLOCK_REF;

                auto [fname, fval] = next_field(rest);
                if (fname == "offset")
                    eir.offset = parse_hex<uint64_t>(fval);
                else
                    throw formatted_error("unrecognized shared_block_ref field '{}'", fname);

                append(&eir, sizeof(eir));
            } else if (ref_type == "extent_data_ref") {
                btrfs::extent_inline_ref eir;
                btrfs::extent_data_ref edr;

                memset(&eir, 0, sizeof(eir));
                memset(&edr, 0, sizeof(edr));

                eir.type = btrfs::key_type::EXTENT_DATA_REF;

                while (!rest.empty()) {
                    auto saved2 = rest;
                    auto [fname, fval] = next_field(rest);
                    if (fname.empty())
                        break;

                    if (fname == "root")
                        edr.root = parse_hex<uint64_t>(fval);
                    else if (fname == "objectid")
                        edr.objectid = parse_hex<uint64_t>(fval);
                    else if (fname == "offset")
                        edr.offset = parse_hex<uint64_t>(fval);
                    else if (fname == "count") {
                        edr.count = parse_hex<uint32_t>(fval);
                        break;
                    } else {
                        rest = saved2;
                        break;
                    }
                }

                // extent_inline_ref type then extent_data_ref packed as: type + edr overlapping offset
                append(&eir.type, sizeof(eir.type));
                append(&edr, sizeof(edr));
            } else if (ref_type == "shared_data_ref") {
                btrfs::extent_inline_ref eir;
                btrfs::shared_data_ref sdr;

                memset(&eir, 0, sizeof(eir));
                memset(&sdr, 0, sizeof(sdr));

                eir.type = btrfs::key_type::SHARED_DATA_REF;

                while (!rest.empty()) {
                    auto saved2 = rest;
                    auto [fname, fval] = next_field(rest);
                    if (fname.empty())
                        break;

                    if (fname == "offset")
                        eir.offset = parse_hex<uint64_t>(fval);
                    else if (fname == "count") {
                        sdr.count = parse_hex<uint32_t>(fval);
                        break;
                    } else {
                        rest = saved2;
                        break;
                    }
                }

                append(&eir, sizeof(eir));
                append(&sdr, sizeof(sdr));
            } else if (ref_type == "extent_owner_ref") {
                btrfs::extent_inline_ref eir;

                memset(&eir, 0, sizeof(eir));

                eir.type = btrfs::key_type::EXTENT_OWNER_REF;

                auto [fname, fval] = next_field(rest);
                if (fname == "root")
                    eir.offset = parse_hex<uint64_t>(fval);
                else
                    throw formatted_error("unrecognized extent_owner_ref field '{}'", fname);

                append(&eir, sizeof(eir));
            } else
                throw formatted_error("unrecognized inline ref type '{}'", ref_type);
        }
    } else if (type_word == "block_group_item") {
        btrfs::block_group_item_v2 bgi;
        bool has_v2 = false;

        // parse: used=X chunk_objectid=X flags=names [remap_bytes=X identity_remap_count=X]

        memset(&bgi, 0, sizeof(bgi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "used")
                bgi.used = parse_hex<uint64_t>(fval);
            else if (fname == "chunk_objectid")
                bgi.chunk_objectid = parse_hex<uint64_t>(fval);
            else if (fname == "flags")
                bgi.flags = parse_block_group_flags(fval);
            else if (fname == "remap_bytes") {
                bgi.remap_bytes = parse_hex<uint64_t>(fval);
                has_v2 = true;
            } else if (fname == "identity_remap_count") {
                bgi.identity_remap_count = parse_hex<uint32_t>(fval);
                has_v2 = true;
            } else
                throw formatted_error("unrecognized block_group_item field '{}'", fname);
        }

        if (has_v2)
            append(&bgi, sizeof(bgi));
        else
            append(&bgi, sizeof(btrfs::block_group_item));
    } else if (type_word == "free_space_info") {
        btrfs::free_space_info fsi;

        memset(&fsi, 0, sizeof(fsi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "extent_count")
                fsi.extent_count = parse_hex<uint32_t>(fval);
            else if (fname == "flags")
                fsi.flags = parse_free_space_info_flags(fval);
            else
                throw formatted_error("unrecognized free_space_info field '{}'", fname);
        }

        append(&fsi, sizeof(fsi));
    } else if (type_word == "free_space_extent") {
        // zero-length
    } else if (type_word == "free_space_bitmap") {
        auto bitmap_size = key.offset / (sb.sectorsize * 8);

        // dump format is "addr, len; addr, len; ..." where addr and len are hex
        // each bit in the 256-byte bitmap represents one sector

        data.resize(bitmap_size, 0);

        while (!rest.empty()) {
            while (!rest.empty() && (rest.front() == ' ' || rest.front() == ';')) {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            // parse addr

            string_view addr_sv;

            if (auto comma = rest.find(','); comma != string_view::npos) {
                addr_sv = rest.substr(0, comma);
                rest.remove_prefix(comma + 1);
            } else
                break;

            while (!addr_sv.empty() && addr_sv.back() == ' ') {
                addr_sv.remove_suffix(1);
            }

            auto addr = parse_hex<uint64_t>(addr_sv);

            while (!rest.empty() && rest.front() == ' ') {
                rest.remove_prefix(1);
            }

            // parse len

            string_view len_sv;

            if (auto sc = rest.find(';'); sc != string_view::npos) {
                len_sv = rest.substr(0, sc);
                rest = rest.substr(sc);
            } else {
                len_sv = rest;
                rest = "";
            }

            while (!len_sv.empty() && len_sv.back() == ' ') {
                len_sv.remove_suffix(1);
            }

            auto len = parse_hex<uint64_t>(len_sv);

            // set bits in bitmap

            if (sb.sectorsize > 0) {
                auto bit_start = (addr - key.objectid) / sb.sectorsize;
                auto bit_count = len / sb.sectorsize;

                for (uint64_t b = bit_start; b < bit_start + bit_count && b / 8 < data.size(); b++) {
                    data[b / 8] |= (1 << (b % 8));
                }
            }
        }
    } else if (type_word == "dev_extent") {
        btrfs::dev_extent de;

        memset(&de, 0, sizeof(de));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "chunk_tree")
                de.chunk_tree = parse_hex<uint64_t>(fval);
            else if (fname == "chunk_objectid")
                de.chunk_objectid = parse_hex<uint64_t>(fval);
            else if (fname == "chunk_offset")
                de.chunk_offset = parse_hex<uint64_t>(fval);
            else if (fname == "length")
                de.length = parse_hex<uint64_t>(fval);
            else if (fname == "chunk_tree_uuid")
                de.chunk_tree_uuid = parse_uuid(fval);
            else
                throw formatted_error("unrecognized dev_extent field '{}'", fname);
        }

        append(&de, sizeof(de));
    } else if (type_word == "dev_item") {
        btrfs::dev_item d;

        memset(&d, 0, sizeof(d));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "devid")
                d.devid = parse_hex<uint64_t>(fval);
            else if (fname == "total_bytes")
                d.total_bytes = parse_hex<uint64_t>(fval);
            else if (fname == "bytes_used")
                d.bytes_used = parse_hex<uint64_t>(fval);
            else if (fname == "io_align")
                d.io_align = parse_hex<uint32_t>(fval);
            else if (fname == "io_width")
                d.io_width = parse_hex<uint32_t>(fval);
            else if (fname == "sector_size")
                d.sector_size = parse_hex<uint32_t>(fval);
            else if (fname == "type")
                d.type = parse_hex<uint64_t>(fval);
            else if (fname == "generation")
                d.generation = parse_hex<uint64_t>(fval);
            else if (fname == "start_offset")
                d.start_offset = parse_hex<uint64_t>(fval);
            else if (fname == "dev_group")
                d.dev_group = parse_hex<uint32_t>(fval);
            else if (fname == "seek_speed")
                d.seek_speed = parse_hex<uint8_t>(fval);
            else if (fname == "bandwidth")
                d.bandwidth = parse_hex<uint8_t>(fval);
            else if (fname == "uuid")
                d.uuid = parse_uuid(fval);
            else if (fname == "fsid")
                d.fsid = parse_uuid(fval);
            else
                throw formatted_error("unrecognized dev_item field '{}'", fname);
        }

        append(&d, sizeof(d));
    } else if (type_word == "chunk_item") {
        struct {
            btrfs::chunk c;
            btrfs::stripe extra_stripes[MAX_STRIPES - 1];
        } __attribute__((packed)) chunk_buf;

        memset(&chunk_buf, 0, sizeof(chunk_buf));

        auto& c = chunk_buf.c;

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "length")
                c.length = parse_hex<uint64_t>(fval);
            else if (fname == "owner")
                c.owner = parse_hex<uint64_t>(fval);
            else if (fname == "stripe_len")
                c.stripe_len = parse_hex<uint64_t>(fval);
            else if (fname == "type")
                c.type = parse_block_group_flags(fval);
            else if (fname == "io_align")
                c.io_align = parse_hex<uint32_t>(fval);
            else if (fname == "io_width")
                c.io_width = parse_hex<uint32_t>(fval);
            else if (fname == "sector_size")
                c.sector_size = parse_hex<uint32_t>(fval);
            else if (fname == "num_stripes")
                c.num_stripes = parse_hex<uint16_t>(fval);
            else if (fname == "sub_stripes")
                c.sub_stripes = parse_hex<uint16_t>(fval);
            else if (fname.starts_with("stripe(")) {
                // stripe(N) devid=X offset=X dev_uuid=U

                auto open = fname.find('(');
                auto close = fname.find(')');

                if (open == string_view::npos || close == string_view::npos || close < open)
                    throw formatted_error("could not parse {}", rest);

                auto stripe_idx = parse_hex<uint16_t>(fname.substr(open + 1, close - open - 1));

                auto& s = c.stripe[stripe_idx];

                // fname is "stripe(N)", skip it and parse devid/offset/dev_uuid
                while (!rest.empty()) {
                    auto saved2 = rest;

                    auto [fname2, fval2] = next_field(rest);
                    if (fname2.empty())
                        break;

                    if (fname2 == "devid")
                        s.devid = parse_hex<uint64_t>(fval2);
                    else if (fname2 == "offset")
                        s.offset = parse_hex<uint64_t>(fval2);
                    else if (fname2 == "dev_uuid") {
                        s.dev_uuid = parse_uuid(fval2);
                        break;
                    } else {
                        rest = saved2;
                        break;
                    }
                }
            } else
                throw formatted_error("unrecognized chunk_item field '{}'", fname);
        }

        size_t total = offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe));

        append(&chunk_buf, total);
    } else if (type_word == "orphan_item") {
        // zero-length
    } else if (type_word == "qgroup_status") {
        btrfs::qgroup_status_item qsi;

        memset(&qsi, 0, sizeof(qsi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "version")
                qsi.version = parse_hex<uint64_t>(fval);
            else if (fname == "generation")
                qsi.generation = parse_hex<uint64_t>(fval);
            else if (fname == "flags")
                qsi.flags = parse_qgroup_status_flags(fval);
            else if (fname == "rescan")
                qsi.rescan = parse_hex<uint64_t>(fval);
            else if (fname == "enable_gen")
                qsi.enable_gen = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized qgroup_status field '{}'", fname);
        }

        append(&qsi, sizeof(qsi));
    } else if (type_word == "qgroup_info") {
        btrfs::qgroup_info_item qi;

        memset(&qi, 0, sizeof(qi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "generation")
                qi.generation = parse_hex<uint64_t>(fval);
            else if (fname == "rfer")
                qi.rfer = parse_hex<uint64_t>(fval);
            else if (fname == "rfer_cmpr")
                qi.rfer_cmpr = parse_hex<uint64_t>(fval);
            else if (fname == "excl")
                qi.excl = parse_hex<uint64_t>(fval);
            else if (fname == "excl_cmpr")
                qi.excl_cmpr = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized qgroup_info field '{}'", fname);
        }

        append(&qi, sizeof(qi));
    } else if (type_word == "qgroup_limit") {
        btrfs::qgroup_limit_item qli;

        memset(&qli, 0, sizeof(qli));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "flags")
                qli.flags = parse_qgroup_limit_flags(fval);
            else if (fname == "max_rfer")
                qli.max_rfer = parse_hex<uint64_t>(fval);
            else if (fname == "max_excl")
                qli.max_excl = parse_hex<uint64_t>(fval);
            else if (fname == "rsv_rfer")
                qli.rsv_rfer = parse_hex<uint64_t>(fval);
            else if (fname == "rsv_excl")
                qli.rsv_excl = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized qgroup_limit field '{}'", fname);
        }

        append(&qli, sizeof(qli));
    } else if (type_word == "qgroup_relation") {
        // zero-length
    } else if (type_word == "dev_stats") {
        // space-separated hex values as le64
        while (!rest.empty()) {
            while (!rest.empty() && rest.front() == ' ') {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            string_view token;

            if (auto sp = rest.find(' '); sp != string_view::npos) {
                token = rest.substr(0, sp);
                rest = rest.substr(sp);
            } else {
                token = rest;
                rest = "";
            }

            if (token.empty())
                continue;

            btrfs::le64 val = parse_hex<uint64_t>(token);
            append(&val, sizeof(val));
        }
    } else if (type_word == "free_space") {
        btrfs::free_space_header fsh;

        memset(&fsh, 0, sizeof(fsh));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "location")
                fsh.location = parse_key(fval);
            else if (fname == "generation")
                fsh.generation = parse_hex<uint64_t>(fval);
            else if (fname == "num_entries")
                fsh.num_entries = parse_hex<uint64_t>(fval);
            else if (fname == "num_bitmaps")
                fsh.num_bitmaps = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized free_space field '{}'", fname);
        }

        append(&fsh, sizeof(fsh));
    } else if (type_word == "balance") {
        btrfs::balance_item bi;

        // balance flags=X data=(args) meta=(args) sys=(args)

        memset(&bi, 0, sizeof(bi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "flags")
                bi.flags = parse_balance_flags(fval);
            else if (fname == "data" || fname == "meta" || fname == "sys") {
                // parse disk_balance_args from values in brackets
                auto& dba = fname == "data" ? bi.data : (fname == "meta" ? bi.meta : bi.sys);

                memset(&dba, 0, sizeof(dba));

                auto args = fval;
                while (!args.empty()) {
                    auto [aname, aval] = next_field(args);
                    if (aname.empty())
                        break;

                    if (aname == "profiles")
                        dba.profiles = parse_hex<uint64_t>(aval);
                    else if (aname == "usage")
                        dba.usage = parse_hex<uint64_t>(aval);
                    else if (aname == "usage_min")
                        dba.s1.usage_min = parse_hex<uint32_t>(aval);
                    else if (aname == "usage_max")
                        dba.s1.usage_max = parse_hex<uint32_t>(aval);
                    else if (aname == "devid")
                        dba.devid = parse_hex<uint64_t>(aval);
                    else if (aname == "pstart")
                        dba.pstart = parse_hex<uint64_t>(aval);
                    else if (aname == "pend")
                        dba.pend = parse_hex<uint64_t>(aval);
                    else if (aname == "vstart")
                        dba.vstart = parse_hex<uint64_t>(aval);
                    else if (aname == "vend")
                        dba.vend = parse_hex<uint64_t>(aval);
                    else if (aname == "target")
                        dba.target = parse_hex<uint64_t>(aval);
                    else if (aname == "flags")
                        dba.flags = parse_balance_args_flags(aval);
                    else if (aname == "limit")
                        dba.limit = parse_hex<uint64_t>(aval);
                    else if (aname == "limit_min")
                        dba.s2.limit_min = parse_hex<uint32_t>(aval);
                    else if (aname == "limit_max")
                        dba.s2.limit_max = parse_hex<uint32_t>(aval);
                    else if (aname == "stripes_min")
                        dba.stripes_min = parse_hex<uint32_t>(aval);
                    else if (aname == "stripes_max")
                        dba.stripes_max = parse_hex<uint32_t>(aval);
                    else
                        throw formatted_error("unrecognized balance field '{}'", aname);
                }
            } else
                throw formatted_error("unrecognized balance field '{}'", fname);
        }

        append(&bi, sizeof(bi));
    } else if (type_word == "uuid_subvol" || type_word == "uuid_rec_subvol") {
        btrfs::le64 val = parse_hex<uint64_t>(rest);
        append(&val, sizeof(val));
    } else if (type_word == "dev_replace") {
        btrfs::dev_replace_item dri;

        memset(&dri, 0, sizeof(dri));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "src_devid")
                dri.src_devid = parse_hex<uint64_t>(fval);
            else if (fname == "cursor_left")
                dri.cursor_left = parse_hex<uint64_t>(fval);
            else if (fname == "cursor_right")
                dri.cursor_right = parse_hex<uint64_t>(fval);
            else if (fname == "cont_reading_from_srcdev_mode") {
                if (fval == "always")
                    dri.cont_reading_from_srcdev_mode = 0;
                else if (fval == "avoid")
                    dri.cont_reading_from_srcdev_mode = 1;
                else
                    dri.cont_reading_from_srcdev_mode = parse_hex<uint64_t>(fval);
            } else if (fname == "replace_state") {
                if (fval == "never_started")
                    dri.replace_state = 0;
                else if (fval == "started")
                    dri.replace_state = 1;
                else if (fval == "finished")
                    dri.replace_state = 2;
                else if (fval == "cancelled")
                    dri.replace_state = 3;
                else if (fval == "suspended")
                    dri.replace_state = 4;
                else
                    dri.replace_state = parse_hex<uint64_t>(fval);
            } else if (fname == "time_started")
                dri.time_started = parse_timestamp(fval).sec;
            else if (fname == "time_stopped")
                dri.time_stopped = parse_timestamp(fval).sec;
            else if (fname == "num_write_errors")
                dri.num_write_errors = parse_hex<uint64_t>(fval);
            else if (fname == "num_uncorrectable_read_errors")
                dri.num_uncorrectable_read_errors = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized dev_replace field '{}'", fname);
        }

        append(&dri, sizeof(dri));
    } else if (type_word == "raid_stripe") {
        // multiple entries separated by semicolons:
        // devid=X physical=X; devid=X physical=X

        while (!rest.empty()) {
            btrfs::raid_stride rs;

            memset(&rs, 0, sizeof(rs));

            while (!rest.empty() && (rest.front() == ' ' || rest.front() == ';')) {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            while (!rest.empty()) {
                if (rest.front() == ';')
                    break;

                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "devid")
                    rs.devid = parse_hex<uint64_t>(fval);
                else if (fname == "physical") {
                    rs.physical = parse_hex<uint64_t>(fval);
                    break;
                } else
                    throw formatted_error("unrecognized raid_stripe field '{}'", fname);
            }

            append(&rs, sizeof(rs));
        }
    } else if (type_word == "identity_remap") {
        // zero-length
    } else if (type_word == "remap" || type_word == "remap_backref") {
        btrfs::remap_item r;

        memset(&r, 0, sizeof(r));

        while (!rest.empty() && rest.front() == ' ') {
            rest.remove_prefix(1);
        }

        if (!rest.empty()) {
            string_view token;

            if (auto sp = rest.find(' '); sp != string_view::npos) {
                token = rest.substr(0, sp);
                rest = rest.substr(sp);
            } else {
                token = rest;
                rest = "";
            }

            r.address = parse_hex<uint64_t>(token);
        }

        append(&r, sizeof(r));
    } else if (type_word == "dir_log_index") {
        btrfs::dir_log_item dli;

        memset(&dli, 0, sizeof(dli));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "end")
                dli.end = parse_hex<uint64_t>(fval);
            else
                throw formatted_error("unrecognized dir_log_index field '{}'", fname);
        }

        append(&dli, sizeof(dli));
    } else if (type_word == "extent_data_ref") {
        btrfs::extent_data_ref edr;

        memset(&edr, 0, sizeof(edr));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "root")
                edr.root = parse_hex<uint64_t>(fval);
            else if (fname == "objectid")
                edr.objectid = parse_hex<uint64_t>(fval);
            else if (fname == "offset")
                edr.offset = parse_hex<uint64_t>(fval);
            else if (fname == "count")
                edr.count = parse_hex<uint32_t>(fval);
            else
                throw formatted_error("unrecognized extent_data_ref field '{}'", fname);
        }

        append(&edr, sizeof(edr));
    } else if (type_word == "shared_data_ref") {
        btrfs::shared_data_ref sdr;

        memset(&sdr, 0, sizeof(sdr));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "count")
                sdr.count = parse_hex<uint32_t>(fval);
            else
                throw formatted_error("unrecognized shared_data_ref field '{}'", fname);
        }

        append(&sdr, sizeof(sdr));
    } else if (type_word == "tree_block_ref" || type_word == "shared_block_ref") {
        // zero-length
    } else if (type_word == "string_item") {
        // raw string data
        data.assign(rest.begin(), rest.end());
        rest = "";
    } else if (type_word == "verity_desc_item") {
        btrfs::verity_descriptor_item vdi;

        memset(&vdi, 0, sizeof(vdi));

        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "size")
                vdi.size = parse_hex<uint64_t>(fval);
            else if (fname == "encryption")
                vdi.encryption = parse_hex<uint8_t>(fval);
            else
                throw formatted_error("unrecognized verity_desc_item field '{}'", fname);
        }

        append(&vdi, sizeof(vdi));
    } else if (type_word == "fsverity_descriptor") {
        btrfs::fsverity_descriptor desc;

        memset(&desc, 0, sizeof(desc));

        while (!rest.empty()) {
            auto saved = rest;
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "version")
                desc.version = parse_hex<uint8_t>(fval);
            else if (fname == "hash_algorithm") {
                if (fval == "sha256")
                    desc.hash_algorithm = btrfs::fsverity_hash_algorithm::SHA256;
                else if (fval == "sha512")
                    desc.hash_algorithm = btrfs::fsverity_hash_algorithm::SHA512;
                else
                    desc.hash_algorithm = (btrfs::fsverity_hash_algorithm)parse_hex<uint8_t>(fval);
            } else if (fname == "log_blocksize")
                desc.log_blocksize = parse_hex<uint8_t>(fval);
            else if (fname == "salt_size")
                desc.salt_size = parse_hex<uint8_t>(fval);
            else if (fname == "data_size")
                desc.data_size = parse_hex<uint64_t>(fval);
            else if (fname == "root_hash") {
                // formatted as concatenated {:016x} le64 values

                auto* dest = (btrfs::le64*)desc.root_hash;
                for (size_t i = 0; (i * 16) + 16 <= fval.size() && i < sizeof(desc.root_hash) / 8; i++) {
                    dest[i] = parse_hex<uint64_t>(fval.substr(i * 16, 16));
                }
            } else if (fname == "salt") {
                auto bytes = parse_hex_bytes(fval);
                memcpy(desc.salt, bytes.data(), min(bytes.size(), sizeof(desc.salt)));
            } else if (fname == "sig") {
                // sig is base64-encoded, but handled below after append
                rest = saved;
                break;
            } else
                throw formatted_error("unrecognized fsverity_descriptor field '{}'", fname);
        }

        append(&desc, sizeof(desc));

        // handle sig= (base64-encoded signature appended after the descriptor)
        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;

            if (fname == "sig") {
                auto decoded = b64decode(fval);
                append(decoded.data(), decoded.size());
            }
        }
    } else if (type_word == "verity_merkle_item") {
        // space-separated groups of hex bytes (32 bytes per group)
        while (!rest.empty()) {
            while (!rest.empty() && rest.front() == ' ') {
                rest.remove_prefix(1);
            }

            if (rest.empty())
                break;

            string_view token;

            if (auto sp = rest.find(' '); sp != string_view::npos) {
                token = rest.substr(0, sp);
                rest = rest.substr(sp);
            } else {
                token = rest;
                rest = "";
            }

            if (token.empty())
                continue;

            auto bytes = parse_hex_bytes(token);

            append(bytes.data(), bytes.size());
        }
    } else if (type_word == "unknown") {
        // unknown (size=X) - zero-fill with the given size
        while (!rest.empty()) {
            auto [fname, fval] = next_field(rest);
            if (fname.empty())
                break;
        }
    } else
        throw formatted_error("unknown item type '{}'", type_word);

    // handle extra= at end of type_line

    static const string_view extra = " extra=";

    if (auto extra_pos = type_line.find(extra); extra_pos != string_view::npos) {
        auto hex_str = type_line.substr(extra_pos + extra.size());

        // Remove any trailing whitespace

        while (!hex_str.empty() && (hex_str.back() == ' ' || hex_str.back() == '\n' || hex_str.back() == '\r')) {
            hex_str.remove_suffix(1);
        }

        auto extra_bytes = parse_hex_bytes(hex_str);

        data.insert(data.end(), extra_bytes.begin(), extra_bytes.end());
    }

    return data;
}

static void parse_superblock_fields(string_view line, btrfs::super_block& sb) {
    while (!line.empty()) {
        auto [name, val] = next_field(line);

        if (name.empty())
            break;
        else if (name == "csum") {
            // ignored, computed automatically
        } else if (name == "fsid")
            sb.fsid = parse_uuid(val);
        else if (name == "bytenr")
            sb.bytenr = parse_hex<uint64_t>(val);
        else if (name == "flags")
            sb.flags = parse_super_flags(val);
        else if (name == "magic")
            memcpy(&sb.magic, val.data(), min(val.size(), sizeof(uint64_t)));
        else if (name == "generation")
            sb.generation = parse_hex<uint64_t>(val);
        else if (name == "root")
            sb.root = parse_hex<uint64_t>(val);
        else if (name == "chunk_root")
            sb.chunk_root = parse_hex<uint64_t>(val);
        else if (name == "log_root")
            sb.log_root = parse_hex<uint64_t>(val);
        else if (name == "log_root_transid")
            sb.__unused_log_root_transid = parse_hex<uint64_t>(val);
        else if (name == "total_bytes")
            sb.total_bytes = parse_hex<uint64_t>(val);
        else if (name == "bytes_used")
            sb.bytes_used = parse_hex<uint64_t>(val);
        else if (name == "root_dir_objectid")
            sb.root_dir_objectid = parse_hex<uint64_t>(val);
        else if (name == "num_devices")
            sb.num_devices = parse_hex<uint64_t>(val);
        else if (name == "sectorsize")
            sb.sectorsize = parse_hex<uint32_t>(val);
        else if (name == "nodesize")
            sb.nodesize = parse_hex<uint32_t>(val);
        else if (name == "leafsize")
            sb.__unused_leafsize = parse_hex<uint32_t>(val);
        else if (name == "stripesize")
            sb.stripesize = parse_hex<uint32_t>(val);
        else if (name == "sys_chunk_array_size")
            sb.sys_chunk_array_size = parse_hex<uint32_t>(val);
        else if (name == "chunk_root_generation")
            sb.chunk_root_generation = parse_hex<uint64_t>(val);
        else if (name == "compat_flags")
            sb.compat_flags = parse_hex<uint64_t>(val);
        else if (name == "compat_ro_flags")
            sb.compat_ro_flags = parse_compat_ro_flags(val);
        else if (name == "incompat_flags")
            sb.incompat_flags = parse_incompat_flags(val);
        else if (name == "csum_type")
            sb.csum_type = parse_csum_type(val);
        else if (name == "root_level")
            sb.root_level = parse_hex<uint8_t>(val);
        else if (name == "chunk_root_level")
            sb.chunk_root_level = parse_hex<uint8_t>(val);
        else if (name == "log_root_level")
            sb.log_root_level = parse_hex<uint8_t>(val);
        else if (name == "label") {
            auto size = min(val.size(), sizeof(sb.label) - 1);

            memcpy(sb.label.data(), val.data(), size);
            memset(sb.label.data() + size, 0, sizeof(sb.label) - size);
        } else if (name == "cache_generation")
            sb.cache_generation = parse_hex<uint64_t>(val);
        else if (name == "uuid_tree_generation")
            sb.uuid_tree_generation = parse_hex<uint64_t>(val);
        else if (name == "metadata_uuid")
            sb.metadata_uuid = parse_uuid(val);
        else if (name == "remap_root")
            sb.remap_root = parse_hex<uint64_t>(val);
        else if (name == "remap_root_generation")
            sb.remap_root_generation = parse_hex<uint64_t>(val);
        else if (name == "remap_root_level")
            sb.remap_root_level = parse_hex<uint8_t>(val);
        else
            throw formatted_error("unrecognized superblock field '{}'", name);
    }
}

static void parse_dev_item_fields(string_view line, btrfs::dev_item& d) {
    while (!line.empty()) {
        auto [name, val] = next_field(line);

        if (name.empty())
            break;
        else if (name == "devid")
            d.devid = parse_hex<uint64_t>(val);
        else if (name == "total_bytes")
            d.total_bytes = parse_hex<uint64_t>(val);
        else if (name == "bytes_used")
            d.bytes_used = parse_hex<uint64_t>(val);
        else if (name == "io_align")
            d.io_align = parse_hex<uint32_t>(val);
        else if (name == "io_width")
            d.io_width = parse_hex<uint32_t>(val);
        else if (name == "sector_size")
            d.sector_size = parse_hex<uint32_t>(val);
        else if (name == "type")
            d.type = parse_hex<uint64_t>(val);
        else if (name == "generation")
            d.generation = parse_hex<uint64_t>(val);
        else if (name == "start_offset")
            d.start_offset = parse_hex<uint64_t>(val);
        else if (name == "dev_group")
            d.dev_group = parse_hex<uint32_t>(val);
        else if (name == "seek_speed")
            d.seek_speed = parse_hex<uint8_t>(val);
        else if (name == "bandwidth")
            d.bandwidth = parse_hex<uint8_t>(val);
        else if (name == "uuid")
            d.uuid = parse_uuid(val);
        else if (name == "fsid")
            d.fsid = parse_uuid(val);
        else
            throw formatted_error("unrecognized dev_item field '{}'", name);
    }
}

static void assemble(string_view input_path, string_view output_path) {
    ifstream in(filesystem::path{input_path});
    if (!in)
        throw formatted_error("failed to open input file '{}'", input_path);

    fstream out(filesystem::path{output_path}, ios::binary | ios::in | ios::out | ios::trunc);
    if (!out)
        throw formatted_error("failed to open output file '{}'", output_path);

    btrfs::super_block sb;

    memset(&sb, 0, sizeof(sb));
    sb.magic = btrfs::MAGIC;
    sb.bytenr = btrfs::superblock_addrs[0];

    bool have_superblock = false;
    optional<node_state> current_node;
    optional<btrfs::key> current_key;
    uint32_t sys_chunk_offset = 0;
    unsigned int backup_index = 0;
    map<uint64_t, chunk_entry> chunks;
    string line;
    btrfs::key bootstrap_key;

    memset(&bootstrap_key, 0, sizeof(bootstrap_key));

    auto flush_node = [&]() {
        if (!current_node.has_value())
            return;

        // before writing, collect any chunk_items from this node to update the chunk map
        if (current_node->level == 0) {
            for (const auto& it : current_node->items) {
                if (it.key.type == btrfs::key_type::CHUNK_ITEM && it.data.size() >= offsetof(btrfs::chunk, stripe)) {
                    auto& c = *(const btrfs::chunk*)it.data.data();

                    chunk_entry ce;
                    ce.offset = it.key.offset;
                    ce.length = c.length;
                    ce.num_stripes = c.num_stripes;

                    for (uint16_t i = 0; i < c.num_stripes && i < MAX_STRIPES; i++) {
                        ce.stripes[i].devid = c.stripe[i].devid;
                        ce.stripes[i].offset = c.stripe[i].offset;
                    }

                    chunks[ce.offset] = ce;
                }
            }
        }

        write_node(out, *current_node, sb, chunks);

        current_node.reset();
        current_key.reset();
    };

    while (getline(in, line)) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        auto stripped = string_view(line);

        while (!stripped.empty() && stripped.front() == ' ') {
            stripped.remove_prefix(1);
        }

        if (stripped.empty())
            continue;

        // skip tree labels like "CHUNK:", "ROOT:", "Tree X:"
        if (stripped.back() == ':')
            continue;

        string_view type, rest;

        if (auto sp = stripped.find(' '); sp != string_view::npos) {
            type = stripped.substr(0, sp);
            rest = stripped.substr(sp + 1);
        } else {
            type = stripped;
            rest = "";
        }

        if (type == "superblock") {
            flush_node();

            // the superblock line includes "(dev_item ...)" - we need to extract that
            static constexpr string_view dev_item_prefix = "(dev_item ";

            if (auto dev_pos = rest.find(dev_item_prefix); dev_pos != string_view::npos) {
                auto before_dev = rest.substr(0, dev_pos);
                auto after_open = rest.substr(dev_pos + dev_item_prefix.size());

                if (auto close = after_open.find(')'); close != string_view::npos) {
                    auto dev_fields = after_open.substr(0, close);
                    auto after_dev = after_open.substr(close + 1);

                    parse_superblock_fields(before_dev, sb);
                    parse_dev_item_fields(dev_fields, sb.dev_item);

                    // parse remaining fields after the dev_item
                    parse_superblock_fields(after_dev, sb);
                } else
                    parse_superblock_fields(before_dev, sb);
            } else
                parse_superblock_fields(rest, sb);

            have_superblock = true;
            sys_chunk_offset = 0;
        } else if (type == "bootstrap") {
            bootstrap_key = parse_key(rest);

            *(btrfs::key*)(sb.sys_chunk_array.data() + sys_chunk_offset) = bootstrap_key;
            sys_chunk_offset += sizeof(btrfs::key);
        } else if (type == "chunk_item" && !current_node) {
            struct {
                btrfs::chunk c;
                btrfs::stripe extra_stripes[15];
            } __attribute__((packed)) chunk_buf;

            memset(&chunk_buf, 0, sizeof(chunk_buf));

            auto& c = chunk_buf.c;

            while (!rest.empty()) {
                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "length")
                    c.length = parse_hex<uint64_t>(fval);
                else if (fname == "owner")
                    c.owner = parse_hex<uint64_t>(fval);
                else if (fname == "stripe_len")
                    c.stripe_len = parse_hex<uint64_t>(fval);
                else if (fname == "type")
                    c.type = parse_block_group_flags(fval);
                else if (fname == "io_align")
                    c.io_align = parse_hex<uint32_t>(fval);
                else if (fname == "io_width")
                    c.io_width = parse_hex<uint32_t>(fval);
                else if (fname == "sector_size")
                    c.sector_size = parse_hex<uint32_t>(fval);
                else if (fname == "num_stripes")
                    c.num_stripes = parse_hex<uint16_t>(fval);
                else if (fname == "sub_stripes")
                    c.sub_stripes = parse_hex<uint16_t>(fval);
                else if (fname.starts_with("stripe(")) {
                    // parse stripe number from "stripe(N)"
                    auto open = fname.find('(');
                    auto close = fname.find(')');

                    unsigned int stripe_idx;

                    if (open != string_view::npos && close != string_view::npos)
                        stripe_idx = parse_hex<unsigned int>(fname.substr(open + 1, close - open - 1));
                    else
                        stripe_idx = 0;

                    auto& s = c.stripe[stripe_idx];

                    while (!rest.empty()) {
                        auto saved = rest;
                        auto [fname2, fval2] = next_field(rest);

                        if (fname2.empty())
                            break;

                        if (fname2 == "devid")
                            s.devid = parse_hex<uint64_t>(fval2);
                        else if (fname2 == "offset")
                            s.offset = parse_hex<uint64_t>(fval2);
                        else if (fname2 == "dev_uuid") {
                            s.dev_uuid = parse_uuid(fval2);
                            break;
                        } else {
                            rest = saved;
                            break;
                        }
                    }
                } else
                    throw formatted_error("unrecognized chunk_item field '{}'", fname);
            }

            auto chunk_size = offsetof(btrfs::chunk, stripe) +
                              (c.num_stripes * sizeof(btrfs::stripe));
            memcpy(sb.sys_chunk_array.data() + sys_chunk_offset, &chunk_buf, chunk_size);
            sys_chunk_offset += chunk_size;
            sb.sys_chunk_array_size = sys_chunk_offset;

            // add to chunk map

            chunk_entry ce;

            ce.offset = bootstrap_key.offset;
            ce.length = c.length;
            ce.num_stripes = c.num_stripes;

            for (uint16_t i = 0; i < c.num_stripes && i < MAX_STRIPES; i++) {
                ce.stripes[i].devid = c.stripe[i].devid;
                ce.stripes[i].offset = c.stripe[i].offset;
            }

            chunks[ce.offset] = ce;
        } else if (type == "backup") {
            if (backup_index < sb.super_roots.size()) {
                auto& b = sb.super_roots[backup_index];
                memset(&b, 0, sizeof(b));

                while (!rest.empty()) {
                    auto [name, val] = next_field(rest);
                    if (name.empty())
                        break;

                    if (name == "tree_root")
                        b.tree_root = parse_hex<uint64_t>(val);
                    else if (name == "tree_root_gen")
                        b.tree_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "chunk_root")
                        b.chunk_root = parse_hex<uint64_t>(val);
                    else if (name == "chunk_root_gen")
                        b.chunk_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "extent_root")
                        b.extent_root = parse_hex<uint64_t>(val);
                    else if (name == "extent_root_gen")
                        b.extent_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "fs_root")
                        b.fs_root = parse_hex<uint64_t>(val);
                    else if (name == "fs_root_gen")
                        b.fs_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "dev_root")
                        b.dev_root = parse_hex<uint64_t>(val);
                    else if (name == "dev_root_gen")
                        b.dev_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "csum_root")
                        b.csum_root = parse_hex<uint64_t>(val);
                    else if (name == "csum_root_gen")
                        b.csum_root_gen = parse_hex<uint64_t>(val);
                    else if (name == "total_bytes")
                        b.total_bytes = parse_hex<uint64_t>(val);
                    else if (name == "bytes_used")
                        b.bytes_used = parse_hex<uint64_t>(val);
                    else if (name == "num_devices")
                        b.num_devices = parse_hex<uint64_t>(val);
                    else if (name == "tree_root_level")
                        b.tree_root_level = parse_hex<uint8_t>(val);
                    else if (name == "chunk_root_level")
                        b.chunk_root_level = parse_hex<uint8_t>(val);
                    else if (name == "extent_root_level")
                        b.extent_root_level = parse_hex<uint8_t>(val);
                    else if (name == "fs_root_level")
                        b.fs_root_level = parse_hex<uint8_t>(val);
                    else if (name == "dev_root_level")
                        b.dev_root_level = parse_hex<uint8_t>(val);
                    else if (name == "csum_root_level")
                        b.csum_root_level = parse_hex<uint8_t>(val);
                    else
                        throw formatted_error("unrecognized backup field '{}'", name);
                }

                backup_index++;
            }
        } else if (type == "header") {
            flush_node();

            current_node.emplace();

            while (!rest.empty()) {
                auto [fname, fval] = next_field(rest);
                if (fname.empty())
                    break;

                if (fname == "csum") {
                    // ignored, computed automatically
                } else if (fname == "fsid") {
                    current_node->fsid = parse_uuid(fval);
                    current_node->has_fsid = true;
                } else if (fname == "bytenr")
                    current_node->bytenr = parse_hex<uint64_t>(fval);
                else if (fname == "flags")
                    current_node->flags = parse_header_flags(fval);
                else if (fname == "chunk_tree_uuid") {
                    current_node->chunk_tree_uuid = parse_uuid(fval);
                    current_node->has_chunk_tree_uuid = true;
                } else if (fname == "generation")
                    current_node->generation = parse_hex<uint64_t>(fval);
                else if (fname == "owner")
                    current_node->owner = parse_hex<uint64_t>(fval);
                else if (fname == "nritems") {
                    // ignored, computed automatically
                } else if (fname == "level")
                    current_node->level = parse_hex<uint8_t>(fval);
                else if (fname == "physical") {
                    // ignore physical (derived)
                } else
                    throw formatted_error("unrecognized header field '{}'", fname);
            }
        } else if (current_node) {
            // try to parse as key line: objectid,type,offset
            // key lines look like: "100,INODE_ITEM,0" or "100,1,0"
            if (stripped.find(',') != string_view::npos && !stripped.starts_with("stripe(")) {
                // could be a key line
                if (auto comma1 = stripped.find(','); comma1 != string_view::npos) {
                    if (auto comma2 = stripped.find(',', comma1 + 1); comma2 != string_view::npos) {
                        // check that the part before the first comma looks like hex
                        auto obj_part = stripped.substr(0, comma1);
                        bool looks_like_key = true;

                        for (auto c : obj_part) {
                            if ((c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F')) {
                                looks_like_key = false;
                                break;
                            }
                        }

                        if (looks_like_key && !obj_part.empty()) {
                            // this is a key line
                            // for internal nodes (level > 0): key has blockptr= and generation= after
                            auto key_end = stripped.find(' ');
                            auto key_sv = key_end == string_view::npos ? stripped : stripped.substr(0, key_end);
                            auto after_key = key_end == string_view::npos ? string_view{} : stripped.substr(key_end);

                            auto k = parse_key(key_sv);

                            if (current_node->level > 0) {
                                // internal node - parse blockptr and generation
                                btrfs::key_ptr kp;

                                memset(&kp, 0, sizeof(kp));
                                kp.key = k;

                                while (!after_key.empty()) {
                                    auto [fname, fval] = next_field(after_key);

                                    if (fname.empty())
                                        break;

                                    if (fname == "blockptr")
                                        kp.blockptr = parse_hex<uint64_t>(fval);
                                    else if (fname == "generation")
                                        kp.generation = parse_hex<uint64_t>(fval);
                                    else
                                        throw formatted_error("unrecognized key_ptr field '{}'", fname);
                                }

                                current_node->key_ptrs.push_back(kp);
                            } else {
                                // leaf node: save key, next line will be item data
                                current_key = k;
                            }

                            continue;
                        }
                    }
                }
            }

            // if we have a pending key, this line is the item data
            if (current_key.has_value() && current_node->level == 0) {
                auto item_data = parse_item_data(stripped, *current_key, sb);

                current_node->items.push_back({*current_key, move(item_data)});
                current_key.reset();
            }
        }
    }

    // flush last node
    flush_node();

    if (!have_superblock)
        throw runtime_error("no superblock found in input");

    // ensure image covers total_bytes
    if (sb.total_bytes > 0) {
        out.seekp(0, ios::end);

        if ((uint64_t)out.tellp() < sb.total_bytes) {
            out.seekp(sb.total_bytes - 1);
            out.put(0);
        }
    }

    write_superblock(out, sb);
}

int main(int argc, char* argv[]) {
    try {
        bool print_version = false, print_usage = false;

        enum {
            GETOPT_VAL_VERSION,
            GETOPT_VAL_HELP
        };

        static const struct option long_options[] = {
            { "version", no_argument, nullptr, GETOPT_VAL_VERSION },
            { "help", no_argument, nullptr, GETOPT_VAL_HELP },
            { nullptr, 0, nullptr, 0 }
        };

        int opt;
        while ((opt = getopt_long(argc, argv, "", long_options, nullptr)) != -1) {
            switch (opt) {
                case GETOPT_VAL_VERSION:
                    print_version = true;
                    break;
                case GETOPT_VAL_HELP:
                case '?':
                    print_usage = true;
                    break;
            }
        }

        if (print_version) {
            cout << "btrfs-assemble " << PROJECT_VER << endl;
            return 0;
        }

        if (print_usage || optind + 2 > argc) {
            cerr << R"(Usage: btrfs-assemble input.txt output.img

Assemble a text file produced by btrfs-dump into a btrfs image.

Options:
    --version           print version string
    --help              print this screen
)";
            return 1;
        }

        assemble(argv[optind], argv[optind + 1]);
    } catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }

    return 0;
}
