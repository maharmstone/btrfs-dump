#include <iostream>
#include <filesystem>
#include <fstream>
#include <format>
#include <map>
#include <functional>
#include <getopt.h>

import cxxbtrfs;
import formatted_error;
import b64;

using namespace std;

#define MAX_STRIPES 16

struct chunk : btrfs::chunk {
    btrfs::stripe next_stripes[MAX_STRIPES - 1];
};

static btrfs::super_block sb;

static void read_superblock(ifstream& f) {
    string csum;

    f.seekg(btrfs::superblock_addrs[0]);
    f.read((char*)&sb, sizeof(sb));

    // FIXME - throw exception if no magic

    cout << format("superblock {}", sb) << endl;
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

static string read_data(ifstream& f, uint64_t addr, uint64_t size, const map<uint64_t, chunk>& chunks) {
    uint64_t physoff;

    auto& [chunk_start, c] = find_chunk(chunks, addr);

    switch (btrfs::get_chunk_raid_type(c)) {
        case btrfs::raid_type::RAID5:
//             my $data_stripes = $obj->{'num_stripes'} - 1;
//             $stripeoff = ($addr - $obj->{'offset'}) % ($data_stripes * $obj->{'stripe_len'});
//             $parity = (int(($addr - $obj->{'offset'}) / ($data_stripes * $obj->{'stripe_len'})) + $obj->{'num_stripes'} - 1) % $obj->{'num_stripes'};
//             $stripe2 = int($stripeoff / $obj->{'stripe_len'});
//             $stripe = ($parity + $stripe2 + 1) % $obj->{'num_stripes'};
//
//             $f = $devs{$obj->{'stripes'}[$stripe]{'devid'}};
//             $physoff = $obj->{'stripes'}[$stripe]{'physoffset'} + (int(($addr - $obj->{'offset'}) / ($data_stripes * $obj->{'stripe_len'})) * $obj->{'stripe_len'}) + ($stripeoff % $obj->{'stripe_len'});
            throw runtime_error("FIXME - RAID5");

        case btrfs::raid_type::RAID6:
        //     my $data_stripes = $obj->{'num_stripes'} - 2;
        //     $stripeoff = ($addr - $obj->{'offset'}) % ($data_stripes * $obj->{'stripe_len'});
        //     $parity = (int(($addr - $obj->{'offset'}) / ($data_stripes * $obj->{'stripe_len'})) + $obj->{'num_stripes'} - 1) % $obj->{'num_stripes'};
        //     $stripe2 = int($stripeoff / $obj->{'stripe_len'});
        //     $stripe = ($parity + $stripe2 + 1) % $obj->{'num_stripes'};
        //
        //     $f = $devs{$obj->{'stripes'}[$stripe]{'devid'}};
        //     $physoff = $obj->{'stripes'}[$stripe]{'physoffset'} + (int(($addr - $obj->{'offset'}) / ($data_stripes * $obj->{'stripe_len'})) * $obj->{'stripe_len'}) + ($stripeoff % $obj->{'stripe_len'});
            throw runtime_error("FIXME - RAID6");

        case btrfs::raid_type::RAID10:
        //     my $stripe_num = ($addr - $obj->{'offset'}) / $obj->{'stripe_len'};
        //     my $stripe_offset = ($addr - $obj->{'offset'}) % $obj->{'stripe_len'};
        //     my $stripe = $stripe_num % ($obj->{'num_stripes'} / $obj->{'sub_stripes'}) * $obj->{'sub_stripes'};
        //
        //     $f = $devs{$obj->{'stripes'}[$stripe]{'devid'}};
        //     $physoff = $obj->{'stripes'}[$stripe]{'physoffset'} + (($stripe_num / ($obj->{'num_stripes'} / $obj->{'sub_stripes'})) * $obj->{'stripe_len'}) + $stripe_offset;
            throw runtime_error("FIXME - RAID10");

        case btrfs::raid_type::RAID0:
        //     my $stripe_num = ($addr - $obj->{'offset'}) / $obj->{'stripe_len'};
        //     my $stripe_offset = ($addr - $obj->{'offset'}) % $obj->{'stripe_len'};
        //     my $stripe = $stripe_num % $obj->{'num_stripes'};
        //
        //     $f = $devs{$obj->{'stripes'}[$stripe]{'devid'}};
        //     $physoff = $obj->{'stripes'}[$stripe]{'physoffset'} + (($stripe_num / $obj->{'num_stripes'}) * $obj->{'stripe_len'}) + $stripe_offset;
            throw runtime_error("FIXME - RAID0");

        default: // SINGLE, DUP, RAID1, RAID1C3, RAID1C4
            if (c.stripe[0].devid != sb.dev_item.devid)
                throw runtime_error("FIXME - multiple devices");

        //     $f = $devs{$obj->{'stripes'}[0]{'devid'}};
            physoff = c.stripe[0].offset + addr - chunk_start;
            break;
    }

    string ret;

    ret.resize(size);

    f.seekg(physoff);
    f.read(ret.data(), size);

    return ret;
}

static string free_space_bitmap(span<const uint8_t> s, uint64_t offset) {
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

                    runs += format("{:x}, {:x}", offset + (run_start * sb.sectorsize),
                                   (((i * 8) + j) - run_start) * sb.sectorsize);
                }

                last_zero = true;
            }

            c >>= 1;
        }
    }

    if (!last_zero) {
        if (!runs.empty())
            runs += "; ";

        runs += format("{:x}, {:x}", offset + (run_start * sb.sectorsize),
                       ((s.size() * 8) - run_start) * sb.sectorsize);
    }

    return runs;
}

static void dump_item(span<const uint8_t> s, string_view pref, const btrfs::key& key) {
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
                cout << format("free_space_bitmap {}", free_space_bitmap(s, key.objectid));
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

            // } elsif ($type == 0xf8 && $id == 0xfffffffffffffffc) { # balance
            //     my ($fl, @f);
            //
            //     @b = unpack("Q", $s);
            //     $s = substr($s, 8);
            //
            //     $fl = $b[0];
            //     @f = ();
            //
            //     if ($fl & (1 << 0)) {
            //         push @f, "data";
            //         $fl &= ~(1 << 0);
            //     }
            //
            //     if ($fl & (1 << 1)) {
            //         push @f, "system";
            //         $fl &= ~(1 << 1);
            //     }
            //
            //     if ($fl & (1 << 2)) {
            //         push @f, "metadata";
            //         $fl &= ~(1 << 2);
            //     }
            //
            //     if ($fl != 0 || $#f == -1) {
            //         push @f, $fl;
            //     }
            //
            //     printf("balance flags=%s data=(%s) metadata=(%s) sys=(%s)", join(',', @f), format_balance(substr($s, 0, 0x88)), format_balance(substr($s, 0x88, 0x88)), format_balance(substr($s, 0x110, 0x88)));
            //
            //     $s = substr($s, 0x1b8);

            case PERSISTENT_ITEM: {
                auto nums = span((btrfs::le64*)s.data(), s.size() / sizeof(btrfs::le64));

                cout << format("dev_stats");

                for (auto n : nums) {
                    cout << format(" {:x}", n);
                }

                s = s.subspan(nums.size_bytes());
                break;
            }

            // FIXME - DEV_REPLACE

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

static void dump_tree(ifstream& f, uint64_t addr, string_view pref, const map<uint64_t, chunk>& chunks,
                      optional<function<void(const btrfs::key&, span<const uint8_t>)>> func = nullopt) {
    auto tree = read_data(f, addr, sb.nodesize, chunks);

    const auto& h = *(btrfs::header*)tree.data();

    // FIXME - also die on generation or level mismatch? Or option to struggle on manfully?

    if (h.bytenr != addr)
        throw formatted_error("Address mismatch: expected {:x}, got {:x}", addr, h.bytenr);

    // FIXME - make this less hacky (pass csum_type through to formatter?)
    switch (sb.csum_type) {
        case btrfs::csum_type::CRC32:
            cout << format("{}header {:a}", pref, h) << endl;
            break;

        case btrfs::csum_type::XXHASH:
            cout << format("{}header {:b}", pref, h) << endl;
            break;

        case btrfs::csum_type::SHA256:
        case btrfs::csum_type::BLAKE2:
            cout << format("{}header {:c}", pref, h) << endl;
            break;

        default:
            cout << format("{}header {}", pref, h) << endl;
            break;
    }

    if (h.level == 0) {
        auto items = span((btrfs::item*)((uint8_t*)&h + sizeof(btrfs::header)), h.nritems);

        for (const auto& it : items) {
            cout << format("{}{:x}\n", pref, it.key);

            auto item = span((uint8_t*)tree.data() + sizeof(btrfs::header) + it.offset, it.size);

            dump_item(item, pref, it.key);

            if (func.has_value())
                func.value()(it.key, item);
        }
    } else {
        auto items = span((btrfs::key_ptr*)((uint8_t*)&h + sizeof(btrfs::header)), h.nritems);
        auto pref2 = string{pref} + " "; // FIXME

        for (const auto& it : items) {
            cout << format("{}{}\n", pref, it);
            dump_tree(f, it.blockptr, pref2, chunks, func);
        }
    }
}

static map<uint64_t, chunk> load_sys_chunks() {
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

static void dump(const vector<filesystem::path>& fns) {
    map<int64_t, uint64_t> roots, log_roots;

    vector<ifstream> files;

    for (const auto& p : fns) {
        files.emplace_back(p);

        if (files.back().fail())
            throw formatted_error("Failed to open {}", p.string()); // FIXME - include why
    }

    read_superblock(files[0]);

    // FIXME - multiple devices (including for SYSTEM chunks)

    auto sys_chunks = load_sys_chunks();

    cout << "CHUNK:" << endl;

    map<uint64_t, chunk> chunks;

    dump_tree(files[0], sb.chunk_root, "", sys_chunks, [&chunks](const btrfs::key& key, span<const uint8_t> item) {
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

    cout << endl;

    cout << "ROOT:" << endl;
    dump_tree(files[0], sb.root, "", chunks, [&roots](const btrfs::key& key, span<const uint8_t> item) {
        if (key.type != btrfs::key_type::ROOT_ITEM)
            return;

        const auto& ri = *(btrfs::root_item*)item.data();

        roots.insert(make_pair(key.objectid, ri.bytenr));
    });
    cout << endl;

    if (sb.log_root != 0) {
        cout << "LOG:" << endl;

        dump_tree(files[0], sb.log_root, "", chunks, [&log_roots](const btrfs::key& key, span<const uint8_t> item) {
            if (key.type != btrfs::key_type::ROOT_ITEM)
                return;

            const auto& ri = *(btrfs::root_item*)item.data();

            log_roots.insert(make_pair(key.offset, ri.bytenr));
        });

        cout << endl;
    }

    for (auto [root_num, bytenr] : roots) {
        cout << format("Tree {:x}:", (uint64_t)root_num) << endl;

        dump_tree(files[0], bytenr, "", chunks);
        cout << endl;
    }

    for (auto [root_num, bytenr] : log_roots) {
        cout << format("Tree {:x} (log):", (uint64_t)root_num) << endl;

        dump_tree(files[0], bytenr, "", chunks);
        cout << endl;
    }
}

int main(int argc, char** argv) {
    bool print_usage = false;

    // FIXME - use libblkid to find other devices

    while (true) {
        enum {
            GETOPT_VAL_HELP
        };

        static const option long_opts[] = {
            { "help", no_argument, nullptr, GETOPT_VAL_HELP },
            { nullptr, 0, nullptr, 0 }
        };

        auto c = getopt_long(argc, argv, "", long_opts, nullptr);
        if (c < 0)
            break;

        switch (c) {
            case GETOPT_VAL_HELP:
            case '?':
                print_usage = true;
                break;
        }
    }

    if (print_usage || optind == argc) {
        fprintf(stderr, R"(Usage: btrfs-dump <device> [<device>...]

    Dump the metadata of a btrfs image in text format.

    Options:
    --help              print this screen
)");
        return 1;
    }

    vector<filesystem::path> fns;

    for (int i = optind; i < argc; i++) {
        fns.emplace_back(argv[i]);
    }

    try {
        dump(fns);
    } catch (const exception& e) {
        cerr << "Exception: " << e.what() << endl;
    }
}
