#include <iostream>
#include <filesystem>
#include <fstream>
#include <format>
#include <map>
#include <functional>
#include <memory>
#include <list>
#include <getopt.h>
#include <blkid.h>
#include "config.h"

import cxxbtrfs;
import formatted_error;
import b64;

using namespace std;

#define MAX_STRIPES 16

class blkid_cache_putter {
public:
    using pointer = blkid_cache;

    void operator()(blkid_cache cache) {
        blkid_put_cache(cache);
    }
};

using blkid_cache_ptr = unique_ptr<blkid_cache, blkid_cache_putter>;

class blkid_dev_iterate_ender {
public:
    using pointer = blkid_dev_iterate;

    void operator()(blkid_dev_iterate iter) {
        blkid_dev_iterate_end(iter);
    }
};

using blkid_dev_iterate_ptr = unique_ptr<blkid_dev_iterate, blkid_dev_iterate_ender>;

struct chunk : btrfs::chunk {
    btrfs::stripe next_stripes[MAX_STRIPES - 1];
};

struct device {
    device(ifstream& f) : f(f) { }

    ifstream& f;
    btrfs::super_block sb;
};

static void read_superblock(device& d) {
    string csum;

    d.f.seekg(btrfs::superblock_addrs[0]);
    d.f.read((char*)&d.sb, sizeof(d.sb));
}

static const pair<uint64_t, const chunk&> find_chunk(const map<uint64_t, chunk>& chunks,
                                                     uint64_t address) {
    auto it = chunks.upper_bound(address);

    if (it == chunks.begin())
        throw formatted_error("could not find address {:x} in chunks", address);

    const auto& p = *prev(it);

    if (p.first + p.second.length <= address)
        throw formatted_error("could not find address {:x} in chunks", address);

    return p;
}

static string read_data(map<uint64_t, device>& devices, uint64_t addr, uint64_t size, const map<uint64_t, chunk>& chunks) {
    auto& [chunk_start, c] = find_chunk(chunks, addr);

    string ret;

    ret.resize(size);

    // FIXME - handle degraded reads?

    switch (btrfs::get_chunk_raid_type(c)) {
        case btrfs::raid_type::RAID5:
        case btrfs::raid_type::RAID6: {
            auto data_stripes = c.num_stripes - 1;

            if (btrfs::get_chunk_raid_type(c) == btrfs::raid_type::RAID6)
                data_stripes--;

            auto stripeoff = (addr - chunk_start) % (data_stripes * c.stripe_len);
            auto parity = (((addr - chunk_start) / (data_stripes * c.stripe_len)) + c.num_stripes - 1) % c.num_stripes;
            auto stripe2 = stripeoff / c.stripe_len;
            auto stripe = (parity + stripe2 + 1) % c.num_stripes;

            if (devices.count(c.stripe[stripe].devid) == 0)
                throw formatted_error("device {} not found", c.stripe[stripe].devid);

            auto& d = devices.at(c.stripe[stripe].devid);

            d.f.seekg(c.stripe[stripe].offset + (((addr - chunk_start) / (data_stripes * c.stripe_len)) * c.stripe_len) + (stripeoff % c.stripe_len));
            d.f.read(ret.data(), size);

            break;
        }

        case btrfs::raid_type::RAID10: {
            auto stripe_num = (addr - chunk_start) / c.stripe_len;
            auto stripe_offset = (addr - chunk_start) % c.stripe_len;
            auto stripe = (stripe_num % (c.num_stripes / c.sub_stripes)) * c.sub_stripes;

            if (devices.count(c.stripe[stripe].devid) == 0)
                throw formatted_error("device {} not found", c.stripe[stripe].devid);

            auto& d = devices.at(c.stripe[stripe].devid);

            d.f.seekg(c.stripe[stripe].offset + ((stripe_num / (c.num_stripes / c.sub_stripes)) * c.stripe_len) + stripe_offset);
            d.f.read(ret.data(), size);

            break;
        }

        case btrfs::raid_type::RAID0: {
            auto stripe_num = (addr - chunk_start) / c.stripe_len;
            auto stripe_offset = (addr - chunk_start) % c.stripe_len;
            auto stripe = stripe_num % c.num_stripes;

            if (devices.count(c.stripe[stripe].devid) == 0)
                throw formatted_error("device {} not found", c.stripe[stripe].devid);

            auto& d = devices.at(c.stripe[stripe].devid);

            d.f.seekg(c.stripe[stripe].offset + ((stripe_num / c.num_stripes) * c.stripe_len) + stripe_offset);
            d.f.read(ret.data(), size);

            break;
        }

        default: { // SINGLE, DUP, RAID1, RAID1C3, RAID1C4
            if (devices.count(c.stripe[0].devid) == 0)
                throw formatted_error("device {} not found", c.stripe[0].devid);

            auto& d = devices.at(c.stripe[0].devid);

            d.f.seekg(c.stripe[0].offset + addr - chunk_start);
            d.f.read(ret.data(), size);

            break;
        }
    }

    return ret;
}

static string free_space_bitmap(span<const uint8_t> s, uint64_t offset,
                                uint32_t sector_size) {
    string runs;

    uint64_t run_start = 0;
    bool last_zero = true;

    for (size_t i = 0; i < s.size(); i++) {
        auto c = s[i];

        for (unsigned int j = 0; j < 8; j++) {
            if (c & 1) {
                if (last_zero)
                    run_start = (i * 8) + j;

                last_zero = false;
            } else {
                if (!last_zero) {
                    if (!runs.empty())
                        runs += "; ";

                    runs += format("{:x}, {:x}", offset + (run_start * sector_size),
                                   (((i * 8) + j) - run_start) * sector_size);
                }

                last_zero = true;
            }

            c >>= 1;
        }
    }

    if (!last_zero) {
        if (!runs.empty())
            runs += "; ";

        runs += format("{:x}, {:x}", offset + (run_start * sector_size),
                       ((s.size() * 8) - run_start) * sector_size);
    }

    return runs;
}

static void dump_item(span<const uint8_t> s, string_view pref,
                      const btrfs::key& key, const btrfs::super_block& sb) {
    bool unrecog = false;

    // FIXME - handle short items

    cout << pref;

    if ((uint8_t)key.type == 0 && key.objectid == btrfs::FREE_SPACE_OBJECTID) {
        const auto& fsh = *(btrfs::free_space_header*)s.data();

        cout << format("free_space {}", fsh);

        s = s.subspan(sizeof(btrfs::free_space_header));
    } else if (key.objectid == btrfs::BALANCE_OBJECTID && key.type == btrfs::key_type::TEMPORARY_ITEM) {
        const auto& bi = *(btrfs::balance_item*)s.data();

        cout << format("balance {}", bi);

        s = s.subspan(sizeof(btrfs::balance_item));
    } else {
        switch (key.type) {
            using enum btrfs::key_type;

            case INODE_ITEM: {
                const auto& ii = *(btrfs::inode_item*)s.data();

                cout << format("inode_item {}", ii);

                s = s.subspan(sizeof(btrfs::inode_item));

                break;
            }

            case INODE_REF: {
                cout << "inode_ref";

                do {
                    const auto& ir = *(btrfs::inode_ref*)s.data();

                    cout << format(" {}", ir);

                    s = s.subspan(sizeof(btrfs::inode_ref) + ir.name_len);
                } while (!s.empty());

                break;
            }

            case INODE_EXTREF: {
                cout << "inode_extref";

                do {
                    const auto& ier = *(btrfs::inode_extref*)s.data();

                    cout << format(" {}", ier);

                    s = s.subspan(offsetof(btrfs::inode_extref, name) + ier.name_len);
                } while (!s.empty());

                break;
            }

            case XATTR_ITEM: {
                cout << "xattr_item";

                do {
                    const auto& di = *(btrfs::dir_item*)s.data();

                    cout << format(" {}", di);

                    s = s.subspan(sizeof(btrfs::dir_item) + di.data_len + di.name_len);
                } while (!s.empty());

                break;
            };

            case VERITY_DESC_ITEM: {
                if (key.offset == 0) {
                    const auto& vdi = *(btrfs::verity_descriptor_item*)s.data();

                    cout << format("verity_desc_item {}", vdi);
                    s = s.subspan(sizeof(btrfs::verity_descriptor_item));
                } else {
                    const auto& desc = *(btrfs::fsverity_descriptor*)s.data();

                    cout << format("fsverity_descriptor {}", desc);
                    s = s.subspan(sizeof(btrfs::fsverity_descriptor));

                    if (!s.empty()) {
                        cout << format(" sig={}", b64encode(s));
                        s = s.subspan(s.size());
                    }
                }

                break;
            }

            case VERITY_MERKLE_ITEM: {
                cout << "verity_merkle_item";

                for (size_t i = 0; i < s.size(); i++) {
                    if (i % 32 == 0)
                        cout << " ";

                    cout << format("{:02x}", s[i]);
                }

                s = s.subspan(s.size());

                break;
            }

            case ORPHAN_ITEM:
                cout << "orphan_item";
                break;

            case DIR_LOG_INDEX: {
                const auto& dli = *(btrfs::dir_log_item*)s.data();

                cout << format("dir_log_index {}", dli);

                s = s.subspan(sizeof(btrfs::dir_log_item));

                break;
            }

            case DIR_ITEM: {
                cout << "dir_item";

                do {
                    const auto& di = *(btrfs::dir_item*)s.data();

                    cout << format(" {}", di);

                    s = s.subspan(sizeof(btrfs::dir_item) + di.data_len + di.name_len);
                } while (!s.empty());

                break;
            }

            case DIR_INDEX: {
                const auto& di = *(btrfs::dir_item*)s.data();

                cout << format("dir_index {}", di);

                s = s.subspan(sizeof(btrfs::dir_item) + di.data_len + di.name_len);

                break;
            }

            case EXTENT_DATA: {
                const auto& fei = *(btrfs::file_extent_item*)s.data();

                cout << format("extent_data {}", fei);

                if (fei.type == btrfs::file_extent_item_type::inline_extent) {
                    s = s.subspan(offsetof(btrfs::file_extent_item, disk_bytenr));
                    s = s.subspan(fei.ram_bytes);
                } else
                    s = s.subspan(sizeof(btrfs::file_extent_item));

                break;
            }

            case EXTENT_CSUM: {
                cout << format("extent_csum");

                switch (sb.csum_type) {
                    case btrfs::csum_type::CRC32: {
                        auto nums = span((btrfs::le32*)s.data(), s.size() / sizeof(btrfs::le32));

                        for (auto n : nums) {
                            cout << format(" {:08x}", n);
                        }

                        s = s.subspan(nums.size_bytes());
                        break;
                    }

                    case btrfs::csum_type::XXHASH: {
                        auto nums = span((btrfs::le64*)s.data(), s.size() / sizeof(btrfs::le64));

                        for (auto n : nums) {
                            cout << format(" {:016x}", n);
                        }

                        s = s.subspan(nums.size_bytes());
                        break;
                    }

                    case btrfs::csum_type::SHA256:
                    case btrfs::csum_type::BLAKE2: {
                        using arr = array<btrfs::le64, 4>;
                        auto nums = span((arr*)s.data(), s.size() / sizeof(arr));

                        for (auto n : nums) {
                            cout << format(" {:016x}{:016x}{:016x}{:016x}",
                                        n[0], n[1], n[2], n[3]);
                        }

                        s = s.subspan(nums.size_bytes());
                        break;
                    }
                }

                break;
            }

            case ROOT_ITEM: {
                const auto& ri = *(btrfs::root_item*)s.data();

                cout << format("root_item {}", ri);

                s = s.subspan(sizeof(btrfs::root_item));

                break;
            }

            case ROOT_BACKREF:
            case ROOT_REF: {
                const auto& rr = *(btrfs::root_ref*)s.data();

                cout << format("{} {}", key.type == ROOT_BACKREF ? "root_backref" : "root_ref",
                            rr);

                s = s.subspan(sizeof(btrfs::root_ref) + rr.name_len);

                break;
            }

            case EXTENT_ITEM:
            case METADATA_ITEM: {
                const auto& ei = *(btrfs::extent_item*)s.data();

                if (key.type == METADATA_ITEM)
                    cout << format("metadata_item {}", ei);
                else
                    cout << format("extent_item {}", ei);

                // FIXME - EXTENT_ITEM_V0(?)

                s = s.subspan(sizeof(btrfs::extent_item));

                if (key.type == EXTENT_ITEM && ei.flags & btrfs::EXTENT_FLAG_TREE_BLOCK) {
                    const auto& tbi = *(btrfs::tree_block_info*)s.data();

                    cout << format(" {}", tbi);
                    s = s.subspan(sizeof(btrfs::tree_block_info));
                }

                while (s.size() >= sizeof(btrfs::extent_inline_ref)) {
                    bool handled = true;
                    const auto& eir = *(btrfs::extent_inline_ref*)s.data();

                    s = s.subspan(sizeof(btrfs::extent_inline_ref));

                    switch (eir.type) {
                        case TREE_BLOCK_REF:
                            cout << format(" tree_block_ref root={:x}", eir.offset);
                        break;

                        case SHARED_BLOCK_REF:
                            cout << format(" shared_block_ref offset={:x}", eir.offset);
                        break;

                        case EXTENT_DATA_REF: {
                            const auto& edr = *(btrfs::extent_data_ref*)&eir.offset;

                            cout << format(" extent_data_ref {}", edr);
                            s = s.subspan(sizeof(btrfs::extent_data_ref) - sizeof(btrfs::le64));
                            break;
                        }

                        case SHARED_DATA_REF: {
                            const auto& sdr = *(btrfs::shared_data_ref*)((uint8_t*)&eir + sizeof(btrfs::extent_inline_ref));

                            cout << format(" shared_data_ref offset={:x} {}", eir.offset, sdr);
                            s = s.subspan(sizeof(btrfs::shared_data_ref));
                            break;
                        }

                        case EXTENT_OWNER_REF:
                            cout << format(" extent_owner_ref root={:x}", eir.offset);
                        break;

                        default:
                            cout << format(" {:02x}", (uint8_t)eir.type);
                            handled = false;
                        break;
                    }

                    if (!handled)
                        break;
                }

                break;
            }

            case TREE_BLOCK_REF:
                cout << "tree_block_ref";
                break;

            case EXTENT_DATA_REF: {
                const auto& edr = *(btrfs::extent_data_ref*)s.data();

                cout << format("extent_data_ref {}", edr);

                s = s.subspan(sizeof(btrfs::extent_data_ref));

                break;
            }

            // } elsif ($type == 0xb4) { # EXTENT_REF_V0
            //     @b = unpack("QQQv", $s);
            //     $s = substr($s, 28);
            //
            //     printf("extent_ref_v0 root=%x gen=%x objid=%x count=%x", @b);

            case SHARED_BLOCK_REF:
                cout << "shared_block_ref";
                break;

            case SHARED_DATA_REF: {
                const auto& sdr = *(btrfs::shared_data_ref*)s.data();

                cout << format("shared_data_ref {}", sdr);

                s = s.subspan(sizeof(btrfs::shared_data_ref));

                break;
            }

            case BLOCK_GROUP_ITEM: {
                const auto& bgi = *(btrfs::block_group_item*)s.data();

                cout << format("block_group_item {}", bgi);

                s = s.subspan(sizeof(btrfs::block_group_item));
                break;
            }

            case FREE_SPACE_INFO: {
                const auto& fsi = *(btrfs::free_space_info*)s.data();

                cout << format("free_space_info {}", fsi);

                s = s.subspan(sizeof(btrfs::free_space_info));
                break;
            }

            case FREE_SPACE_EXTENT: {
                cout << format("free_space_extent");
                break;
            }

            case FREE_SPACE_BITMAP: {
                cout << format("free_space_bitmap {}",
                               free_space_bitmap(s, key.objectid, sb.sectorsize));
                s = s.subspan(s.size());
                break;
            }

            case DEV_EXTENT: {
                const auto& de = *(btrfs::dev_extent*)s.data();

                cout << format("dev_extent {}", de);

                s = s.subspan(sizeof(btrfs::dev_extent));
                break;
            }

            case DEV_ITEM: {
                const auto& d = *(btrfs::dev_item*)s.data();

                cout << format("dev_item {}", d);

                s = s.subspan(sizeof(btrfs::dev_item));
                break;
            }

            case CHUNK_ITEM: {
                const auto& c = *(btrfs::chunk*)s.data();

                cout << format("chunk_item {}", c);

                s = s.subspan(offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)));
                break;
            }

            // FIXME - RAID_STRIPE

            case QGROUP_STATUS: {
                const auto& qsi = *(btrfs::qgroup_status_item*)s.data();

                cout << format("qgroup_status {}", qsi);

                s = s.subspan(sizeof(btrfs::qgroup_status_item));
                break;
            }

            case QGROUP_INFO: {
                const auto& qi = *(btrfs::qgroup_info_item*)s.data();

                cout << format("qgroup_info {}", qi);

                s = s.subspan(sizeof(btrfs::qgroup_info_item));
                break;
            }

            case QGROUP_LIMIT: {
                const auto& qli = *(btrfs::qgroup_limit_item*)s.data();

                cout << format("qgroup_limit {}", qli);

                s = s.subspan(sizeof(btrfs::qgroup_limit_item));
                break;
            }

            case QGROUP_RELATION:
                cout << "qgroup_relation";
                break;

            case PERSISTENT_ITEM: {
                auto nums = span((btrfs::le64*)s.data(), s.size() / sizeof(btrfs::le64));

                cout << format("dev_stats");

                for (auto n : nums) {
                    cout << format(" {:x}", n);
                }

                s = s.subspan(nums.size_bytes());
                break;
            }

            case DEV_REPLACE: {
                const auto& dri = *(btrfs::dev_replace_item*)s.data();

                cout << format("dev_replace {}", dri);

                s = s.subspan(sizeof(btrfs::dev_replace_item));
                break;
            }

            case UUID_SUBVOL: {
                auto num = *(btrfs::le64*)s.data();

                cout << format("uuid_subvol {:x}", num);

                s = s.subspan(sizeof(num));
                break;
            }

            case UUID_RECEIVED_SUBVOL: {
                auto num = *(btrfs::le64*)s.data();

                cout << format("uuid_rec_subvol {:x}", num);

                s = s.subspan(sizeof(num));
                break;
            }

            default:
                cerr << format("ERROR - unknown type {} (size {:x})", key.type, s.size()) << endl;

                cout << format("unknown (size={:x})", s.size());
                unrecog = true;
        }
    }

    if (!unrecog && !s.empty())
        cout << format(" (left={:x})", s.size());

    cout << endl;
}

static string physical_str(const map<uint64_t, chunk>& chunks, uint64_t addr) {
    string ret;

    auto& [chunk_start, c] = find_chunk(chunks, addr);

    switch (btrfs::get_chunk_raid_type(c)) {
        case btrfs::raid_type::RAID5:
        case btrfs::raid_type::RAID6: {
//             auto data_stripes = c.num_stripes - 1;
//
//             if (btrfs::get_chunk_raid_type(c) == btrfs::raid_type::RAID6)
//                 data_stripes--;
//
//             auto stripeoff = (addr - chunk_start) % (data_stripes * c.stripe_len);
//             auto parity = (((addr - chunk_start) / (data_stripes * c.stripe_len)) + c.num_stripes - 1) % c.num_stripes;
//             auto stripe2 = stripeoff / c.stripe_len;
//             auto stripe = (parity + stripe2 + 1) % c.num_stripes;
//
//             if (devices.count(c.stripe[stripe].devid) == 0)
//                 throw formatted_error("device {} not found", c.stripe[stripe].devid);
//
//             auto& d = devices.at(c.stripe[stripe].devid);
//
//             d.f.seekg(c.stripe[stripe].offset + (((addr - chunk_start) / (data_stripes * c.stripe_len)) * c.stripe_len) + (stripeoff % c.stripe_len));
//             d.f.read(ret.data(), size);
//
            ret = "?RAID5/6?";
            break;
        }

        case btrfs::raid_type::RAID10: {
//             auto stripe_num = (addr - chunk_start) / c.stripe_len;
//             auto stripe_offset = (addr - chunk_start) % c.stripe_len;
//             auto stripe = (stripe_num % (c.num_stripes / c.sub_stripes)) * c.sub_stripes;
//
//             if (devices.count(c.stripe[stripe].devid) == 0)
//                 throw formatted_error("device {} not found", c.stripe[stripe].devid);
//
//             auto& d = devices.at(c.stripe[stripe].devid);
//
//             d.f.seekg(c.stripe[stripe].offset + ((stripe_num / (c.num_stripes / c.sub_stripes)) * c.stripe_len) + stripe_offset);
//             d.f.read(ret.data(), size);
//
            ret = "?RAID10?";
            break;
        }

        case btrfs::raid_type::RAID0: {
//             auto stripe_num = (addr - chunk_start) / c.stripe_len;
//             auto stripe_offset = (addr - chunk_start) % c.stripe_len;
//             auto stripe = stripe_num % c.num_stripes;
//
//             if (devices.count(c.stripe[stripe].devid) == 0)
//                 throw formatted_error("device {} not found", c.stripe[stripe].devid);
//
//             auto& d = devices.at(c.stripe[stripe].devid);
//
//             d.f.seekg(c.stripe[stripe].offset + ((stripe_num / c.num_stripes) * c.stripe_len) + stripe_offset);
//             d.f.read(ret.data(), size);
//
            ret = "?RAID0?";
            break;
        }

        default: { // SINGLE, DUP, RAID1, RAID1C3, RAID1C4
            for (uint16_t i = 0; i < c.num_stripes; i++) {
                if (i != 0)
                    ret += ";";

                ret += format("{},{:x}", c.stripe[i].devid, c.stripe[i].offset + addr - chunk_start);
            }

            break;
        }
    }

    return ret;
}

static void dump_tree(map<uint64_t, device>& devices, uint64_t addr, string_view pref,
                      const map<uint64_t, chunk>& chunks, bool print,
                      optional<function<void(const btrfs::key&, span<const uint8_t>)>> func = nullopt) {
    const auto& sb = devices.begin()->second.sb;
    auto tree = read_data(devices, addr, sb.nodesize, chunks);

    const auto& h = *(btrfs::header*)tree.data();

    // FIXME - also die on generation or level mismatch? Or option to struggle on manfully?

    if (h.bytenr != addr)
        throw formatted_error("Address mismatch: expected {:x}, got {:x}", addr, h.bytenr);

    if (print) {
        string physical = physical_str(chunks, addr);

        // FIXME - make this less hacky (pass csum_type through to formatter?)
        switch (sb.csum_type) {
            case btrfs::csum_type::CRC32:
                cout << format("{}header {:a} physical={}", pref, h, physical) << endl;
                break;

            case btrfs::csum_type::XXHASH:
                cout << format("{}header {:b} physical={}", pref, h, physical) << endl;
                break;

            case btrfs::csum_type::SHA256:
            case btrfs::csum_type::BLAKE2:
                cout << format("{}header {:c} physical={}", pref, h, physical) << endl;
                break;

            default:
                cout << format("{}header {} physical={}", pref, h, physical) << endl;
                break;
        }
    }

    if (h.level == 0) {
        auto items = span((btrfs::item*)((uint8_t*)&h + sizeof(btrfs::header)), h.nritems);

        for (const auto& it : items) {
            if (print)
                cout << format("{}{:x}\n", pref, it.key);

            auto item = span((uint8_t*)tree.data() + sizeof(btrfs::header) + it.offset, it.size);

            if (print)
                dump_item(item, pref, it.key, sb);

            if (func.has_value())
                func.value()(it.key, item);
        }
    } else {
        auto items = span((btrfs::key_ptr*)((uint8_t*)&h + sizeof(btrfs::header)), h.nritems);
        auto pref2 = string{pref} + " "; // FIXME

        for (const auto& it : items) {
            cout << format("{}{}\n", pref, it);
            dump_tree(devices, it.blockptr, pref2, chunks, print, func);
        }
    }
}

static map<uint64_t, chunk> load_sys_chunks(const btrfs::super_block& sb) {
    map<uint64_t, chunk> sys_chunks;

    auto sys_array = span(sb.sys_chunk_array.data(), sb.sys_chunk_array_size);

    while (!sys_array.empty()) {
        if (sys_array.size() < sizeof(btrfs::key))
            throw runtime_error("sys array truncated");

        auto& k = *(btrfs::key*)sys_array.data();

        if (k.type != btrfs::key_type::CHUNK_ITEM)
            throw formatted_error("unexpected key type {} in sys array", k.type);

        sys_array = sys_array.subspan(sizeof(btrfs::key));

        if (sys_array.size() < offsetof(btrfs::chunk, stripe))
            throw runtime_error("sys array truncated");

        auto& c = *(chunk*)sys_array.data();

        if (sys_array.size() < offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)))
            throw runtime_error("sys array truncated");

        if (c.num_stripes > MAX_STRIPES) {
            throw formatted_error("chunk num_stripes is {}, maximum supported is {}",
                                  c.num_stripes, MAX_STRIPES);
        }

        sys_array = sys_array.subspan(offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)));

        sys_chunks.insert(make_pair((uint64_t)k.offset, c));
    }

    return sys_chunks;
}

static vector<string> find_devices(const btrfs::uuid& fsid) {
    vector<string> ret;
    blkid_cache_ptr cache;

    if (blkid_get_cache(out_ptr(cache), nullptr) < 0)
        throw runtime_error("blkid_get_cache failed");

    if (blkid_probe_all(cache.get()) < 0)
        throw runtime_error("blkid_probe_all failed");

    {
        blkid_dev_iterate_ptr iter{blkid_dev_iterate_begin(cache.get())};
        blkid_dev dev;

        auto fsid_str = format("{}", fsid);

        blkid_dev_set_search(iter.get(), "TYPE", "btrfs");
        blkid_dev_set_search(iter.get(), "UUID", fsid_str.c_str());

        while (blkid_dev_next(iter.get(), &dev) == 0) {
            ret.emplace_back(blkid_dev_devname(dev));
        }
    }

    return ret;
}


static void dump(const vector<filesystem::path>& fns, optional<uint64_t> tree_id) {
    map<int64_t, uint64_t> roots, log_roots;
    list<ifstream> files;
    map<uint64_t, device> devices;

    for (const auto& p : fns) {
        files.emplace_back(p);

        if (files.back().fail())
            throw formatted_error("Failed to open {}", p.string()); // FIXME - include why
    }

    for (auto& f : files) {
        device d(f);

        read_superblock(d);

        if (d.sb.magic != btrfs::MAGIC)
            throw runtime_error("not a btrfs device");

        if (devices.count(d.sb.dev_item.devid) != 0)
            throw formatted_error("device {} specified more than once", d.sb.dev_item.devid);

        devices.emplace(d.sb.dev_item.devid, move(d));
    }

    const auto& sb = devices.begin()->second.sb;

    if (fns.size() == 1 && sb.num_devices > 1) {
        auto other_fns = find_devices(sb.fsid);

        for (const auto& n : other_fns) {
            files.emplace_back(n);

            if (files.back().fail())
                cerr << "Failed to open {}" << endl; // FIXME - include why
        }

        for (auto& f : files) {
            if (&f == &files.front())
                continue;

            if (f.fail())
                continue;

            device d(f);

            read_superblock(d);

            // FIXME - close irrelevant files

            if (d.sb.magic != btrfs::MAGIC)
                continue;

            if (d.sb.fsid != sb.fsid)
                continue;

            if (devices.count(d.sb.dev_item.devid) != 0)
                continue;

            devices.emplace(d.sb.dev_item.devid, move(d));
        }

        if (devices.size() != sb.num_devices) {
            if (devices.size() == 1) {
                throw formatted_error("filesystem has {} devices, unable to find the others",
                                      sb.num_devices);
            } else {
                throw formatted_error("filesystem has {} devices, only able to find {} of them",
                                      sb.num_devices, devices.size());
            }
        }
    } else {
        if (devices.size() > 1) {
            for (const auto& [dev_id, d] : devices) {
                if (d.sb.fsid != sb.fsid) {
                    throw formatted_error("fsid mismatch (device {} is {}, device {} is {})",
                                          sb.dev_item.devid, sb.fsid, dev_id,
                                          d.sb.fsid);
                }
            }
        }

        if (devices.size() != sb.num_devices) {
            throw formatted_error("filesystem has {} devices, only {} found",
                                  sb.num_devices, devices.size());
        }
    }

    // FIXME - do we need to check that generation numbers match?

    if (!tree_id.has_value())
        cout << format("superblock {}", sb) << endl;

    auto sys_chunks = load_sys_chunks(sb);

    if (!tree_id.has_value())
        cout << "CHUNK:" << endl;

    map<uint64_t, chunk> chunks;

    dump_tree(devices, sb.chunk_root, "", sys_chunks, !tree_id.has_value() || *tree_id == btrfs::CHUNK_TREE_OBJECTID,
              [&chunks](const btrfs::key& key, span<const uint8_t> item) {
        if (key.type != btrfs::key_type::CHUNK_ITEM)
            return;

        const auto& c = *(chunk*)item.data();

        if (item.size() < offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)))
            throw runtime_error("chunk item truncated");

        if (c.num_stripes > MAX_STRIPES) {
            throw formatted_error("chunk num_stripes is {}, maximum supported is {}",
                                  c.num_stripes, MAX_STRIPES);
        }

        chunks.insert(make_pair((uint64_t)key.offset, c));
    });

    if (!tree_id.has_value()) {
        cout << endl;
        cout << "ROOT:" << endl;
    }

    dump_tree(devices, sb.root, "", chunks, !tree_id.has_value() || *tree_id == btrfs::ROOT_TREE_OBJECTID,
              [&roots](const btrfs::key& key, span<const uint8_t> item) {
        if (key.type != btrfs::key_type::ROOT_ITEM)
            return;

        const auto& ri = *(btrfs::root_item*)item.data();

        roots.insert(make_pair(key.objectid, ri.bytenr));
    });

    if (!tree_id.has_value())
        cout << endl;

    if ((!tree_id.has_value() || *tree_id == btrfs::TREE_LOG_OBJECTID) && sb.log_root != 0) {
        cout << "LOG:" << endl;

        dump_tree(devices, sb.log_root, "", chunks, true, [&log_roots](const btrfs::key& key, span<const uint8_t> item) {
            if (key.type != btrfs::key_type::ROOT_ITEM)
                return;

            const auto& ri = *(btrfs::root_item*)item.data();

            log_roots.insert(make_pair(key.offset, ri.bytenr));
        });

        cout << endl;
    }

    if (tree_id.has_value() && (*tree_id == btrfs::ROOT_TREE_OBJECTID || *tree_id == btrfs::CHUNK_TREE_OBJECTID))
        return;

    if (tree_id.has_value()) {
        if (*tree_id != btrfs::TREE_LOG_OBJECTID) {
            if (roots.count(*tree_id) == 0)
                throw formatted_error("tree {:x} not found", *tree_id);

            dump_tree(devices, roots.at(*tree_id), "", chunks, true);
        }
    } else {
        for (auto [root_num, bytenr] : roots) {
            cout << format("Tree {:x}:", (uint64_t)root_num) << endl;

            dump_tree(devices, bytenr, "", chunks, true);
            cout << endl;
        }
    }

    if (!tree_id.has_value() || *tree_id == btrfs::TREE_LOG_OBJECTID) {
        for (auto [root_num, bytenr] : log_roots) {
            cout << format("Tree {:x} (log):", (uint64_t)root_num) << endl;

            dump_tree(devices, bytenr, "", chunks, true);
            cout << endl;
        }
    }
}

static uint64_t parse_tree_id(string_view sv) {
    uint64_t val;

    static const string_view hex_prefix = "0x";
    static const string_view btrfs_prefix = "btrfs_";
    static const string_view tree_suffix = "_tree";
    static const string_view objectid_suffix = "_objectid";

    if (sv.starts_with(hex_prefix)) {
        if (auto [ptr, ec] = from_chars(sv.begin() + hex_prefix.size(), sv.end(), val, 16); ptr == sv.end())
            return val;
    } else if (auto [ptr, ec] = from_chars(sv.begin(), sv.end(), val); ptr == sv.end())
        return val;

    if (sv.starts_with("-")) {
        int64_t signed_val;

        if (auto [ptr, ec] = from_chars(sv.begin(), sv.end(), signed_val); ptr == sv.end())
            return (uint64_t)signed_val;
    }

    // same logic as tree_id_from_string in btrfs-progs

    string s;

    s.reserve(sv.size());

    for (auto c : sv) {
        if (c >= 'A' && c <= 'Z')
            c = c - 'A' + 'a';

        s += c;
    }

    auto orig_sv = sv;
    sv = s;

    if (sv.starts_with(btrfs_prefix))
        sv = sv.substr(btrfs_prefix.size());

    if (sv.ends_with(objectid_suffix))
        sv = sv.substr(0, sv.size() - objectid_suffix.size());

    if (sv.ends_with(tree_suffix))
        sv = sv.substr(0, sv.size() - tree_suffix.size());

    static const pair<string_view, uint64_t> trees[] = {
        { "root", btrfs::ROOT_TREE_OBJECTID },
        { "extent", btrfs::EXTENT_TREE_OBJECTID },
        { "chunk", btrfs::CHUNK_TREE_OBJECTID },
        { "device", btrfs::DEV_TREE_OBJECTID },
        { "dev", btrfs::DEV_TREE_OBJECTID },
        { "fs", btrfs::FS_TREE_OBJECTID },
        { "csum", btrfs::CSUM_TREE_OBJECTID },
        { "checksum", btrfs::CSUM_TREE_OBJECTID },
        { "quota", btrfs::QUOTA_TREE_OBJECTID },
        { "uuid", btrfs::UUID_TREE_OBJECTID },
        { "free_space", btrfs::FREE_SPACE_TREE_OBJECTID },
        { "free-space", btrfs::FREE_SPACE_TREE_OBJECTID },
        { "tree_log_fixup", btrfs::TREE_LOG_FIXUP_OBJECTID },
        { "tree-log-fixup", btrfs::TREE_LOG_FIXUP_OBJECTID },
        { "tree_log", btrfs::TREE_LOG_OBJECTID },
        { "tree-log", btrfs::TREE_LOG_OBJECTID },
        { "tree_reloc", btrfs::TREE_RELOC_OBJECTID },
        { "tree-reloc", btrfs::TREE_RELOC_OBJECTID },
        { "data_reloc", btrfs::DATA_RELOC_TREE_OBJECTID },
        { "data-reloc", btrfs::DATA_RELOC_TREE_OBJECTID },
        { "block_group", btrfs::BLOCK_GROUP_TREE_OBJECTID },
        { "block-group", btrfs::BLOCK_GROUP_TREE_OBJECTID },
        { "raid_stripe", btrfs::RAID_STRIPE_TREE_OBJECTID },
        { "raid-stripe", btrfs::RAID_STRIPE_TREE_OBJECTID },
    };

    for (const auto& t : trees) {
        if (t.first == sv)
            return t.second;
    }

    throw formatted_error("unable to parse tree ID {}", orig_sv);
}

int main(int argc, char** argv) {
    bool print_version = false, print_usage = false;
    optional<uint64_t> tree_id;

    try {
        while (true) {
            enum {
                GETOPT_VAL_VERSION,
                GETOPT_VAL_HELP
            };

            static const option long_opts[] = {
                { "tree", required_argument, nullptr, 't' },
                { "version", no_argument, nullptr, GETOPT_VAL_VERSION },
                { "help", no_argument, nullptr, GETOPT_VAL_HELP },
                { nullptr, 0, nullptr, 0 }
            };

            auto c = getopt_long(argc, argv, "t:", long_opts, nullptr);
            if (c < 0)
                break;

            switch (c) {
                case 't':
                    tree_id = parse_tree_id(optarg);
                    break;
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
            cout << "btrfs-dump " << PROJECT_VER << endl;
            return 0;
        }

        if (print_usage || optind == argc) {
            cerr << R"(Usage: btrfs-dump <device> [<device>...]

Dump the metadata of a btrfs image in text format.

Options:
    -t|--tree <tree_id> print only specified tree (string, decimal, or
                        hexadecimal number)
    --version           print version string
    --help              print this screen
)";
            return 1;
        }

        vector<filesystem::path> fns;

        for (int i = optind; i < argc; i++) {
            fns.emplace_back(argv[i]);
        }

        dump(fns, tree_id);
    } catch (const exception& e) {
        cerr << "Exception: " << e.what() << endl;
    }
}
