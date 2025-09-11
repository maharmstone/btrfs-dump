#include <iostream>
#include <filesystem>
#include <fstream>
#include <format>
#include <map>

import cxxbtrfs;
import formatted_error;

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

static void dump_item(span<const uint8_t> s, string_view pref, const btrfs::key& key) {
    bool unrecog = false;

    // FIXME - handle short items

    switch (key.type) {
        // if ($type == 0x1 || $type == 0x84) { # INODE_ITEM or ROOT_ITEM
        //     if (length($s) < 0xa0) {
        //         $s .= chr(0) x (0xa0 - length($s));
        //     }
        //
        //     @b = unpack("QQQQQVVVVQQQx32QVQVQVQV", $s);
        //     $s = substr($s, 0xa0);
        //
        //     if ($type == 0x84) {
        //         print "root_item";
        //     } else {
        //         print "inode_item";
        //     }
        //
        //     printf(" gen=%x transid=%x size=%x blocks=%x blockgroup=%x nlink=%x uid=%x gid=%x mode=%o rdev=%x flags=%s seq=%x atime=%s ctime=%s mtime=%s otime=%s", $b[0], $b[1], $b[2], $b[3], $b[4], $b[5], $b[6], $b[7], $b[8], $b[9], inode_flags($b[10]), $b[11], format_time($b[12], $b[13]), format_time($b[14], $b[15]), format_time($b[16], $b[17]), format_time($b[18], $b[19]));
        //
        //     if ($type != 0x1) {
        //         @b = unpack("QQQQQQQVQCQCC", $s);
        //         $s = substr($s, 0x4f);
        //
        //         #print Dumper(@b)."\n";
        //         printf("; expgen=%x objid=%x blocknum=%x bytelimit=%x bytesused=%x snapshotgen=%x flags=%x numrefs=%x dropprogress=%x,%x,%x droplevel=%x rootlevel=%x", @b);
        //
        //         @b = unpack("Qa16a16a16QQQQQVQVQVQV", $s);
        //         $s = substr($s, 0xc8); # above + 64 blank bytes
        //
        //         printf(" gen2=%x uuid=%s par_uuid=%s rec_uuid=%s ctransid=%x otransid=%x stransid=%x rtransid=%x ctime=%s otime=%s stime=%s rtime=%s", $b[0], format_uuid($b[1]), format_uuid($b[2]), format_uuid($b[3]), $b[4], $b[5], $b[6], $b[7], format_time($b[8], $b[9]), format_time($b[10], $b[11]), format_time($b[12], $b[13]), format_time($b[14], $b[15]));
        //     }
        // } elsif ($type == 0xc) { # INODE_REF
        //     printf("inode_ref");
        //
        //     do {
        //         @b = unpack("Qv", $s);
        //         $s = substr($s, 0xa);
        //         my $name = substr($s, 0, $b[1]);
        //         $s = substr($s, $b[1]);
        //
        //         printf(" index=%x n=%x name=%s", $b[0], $b[1], $name);
        //     } while (length($s) > 0);
        // } elsif ($type == 0xd) { # INODE_EXTREF
        //     printf("inode_extref");
        //
        //     do {
        //         @b = unpack("QQv", $s);
        //         $s = substr($s, 0x12);
        //         my $name = substr($s, 0, $b[2]);
        //         $s = substr($s, $b[2]);
        //
        //         printf(" dir=%x index=%x n=%x name=%s", $b[0], $b[1], $b[2], $name);
        //     } while (length($s) > 0);
        // } elsif ($type == 0x18 || $type == 0x54 || $type == 0x60) { # XATTR_ITEM, DIR_ITEM or DIR_INDEX
        //     print $type == 0x54 ? "dir_item" : ($type == 0x18 ? "xattr_item" : "dir_index");
        //
        //     while (length($s) > 0) {
        //         @b = unpack("QCQQvvC", $s);
        //         $s = substr($s, 0x1e);
        //
        //         my $name = substr($s, 0, $b[5]);
        //         $s = substr($s, $b[5]);
        //
        //         my $name2 = substr($s, 0, $b[4]);
        //         $s = substr($s, $b[4]);
        //
        //         printf(" key=%x,%x,%x transid=%x m=%x n=%x type=%x name=%s%s", $b[0], $b[1], $b[2], $b[3], $b[4], $b[5], $b[6], $name, $name2 eq "" ? "" : (" name2=" . $name2));
        //     }
        // } elsif ($type == 0x24) { # VERITY_DESC_ITEM
        //     printf("verity_desc_item");
        //
        //     if ($off == 0) {
        //         @b = unpack("Qx16C", $s);
        //         $s = substr($s, 25);
        //
        //         printf(" size=%x enc=%x", $b[0], $b[1]);
        //     } else {
        //         while (length($s) > 0) {
        //             @b = unpack("C", $s);
        //             printf(" %02x", $b[0]);
        //             $s = substr($s, 1);
        //         }
        //     }
        // } elsif ($type == 0x25) { # VERITY_MERKLE_ITEM
        //     while (length($s) > 0) {
        //         @b = unpack("NNNNNNNN", $s);
        //         printf(" %008x%008x%008x%008x%008x%008x%008x%008x", $b[0], $b[1], $b[2], $b[3], $b[4], $b[5], $b[6], $b[7]);
        //         $s = substr($s, 32);
        //     }
        // } elsif ($type == 0x30) { # ORPHAN_ITEM
        //     printf("orphan_item");
        // } elsif ($type == 0x48) { # LOG_INDEX
        //     @b = unpack("Q", $s);
        //     $s = substr($s, 8);
        //
        //     printf("log_index end=%x", $b[0]);
        // } elsif ($type == 0x6c) { # EXTENT_DATA
        //     @b = unpack("QQCCvC", $s);
        //     $s = substr($s, 0x15);
        //
        //     printf("extent_data gen=%x size=%x comp=%s enc=%s otherenc=%s type=%s", $b[0], $b[1], $b[2], $b[3], $b[4], $b[5]);
        //
        //     if ($b[5] != 0) {
        //         @b = unpack("QQQQ", $s);
        //         $s = substr($s, 0x20);
        //
        //         printf(" ea=%x es=%x o=%x s=%x", @b);
        //     } else {
        //         $s = substr($s, $b[1]);
        //     }
        // } elsif ($type == 0x80) { # EXTENT_CSUM
        //     print "extent_csum";
        //
        //     if ($csum_type == 1) { # xxhash
        //         while (length($s) > 0) {
        //             printf(" %016x", unpack("Q", $s));
        //             $s = substr($s, 8);
        //         }
        //     } elsif ($csum_type == 2 || $csum_type == 3) { # sha256 or blake2
        //         while (length($s) > 0) {
        //             printf(" %016x%016x%016x%016x", unpack("QQQQ", $s));
        //             $s = substr($s, 32);
        //         }
        //     } else {
        //         while (length($s) > 0) {
        //             printf(" %08x", unpack("V", $s));
        //             $s = substr($s, 4);
        //         }
        //     }
        // } elsif ($type == 0x90 || $type == 0x9c) { # ROOT_BACKREF or ROOT_REF
        //     @b = unpack("QQv", $s);
        //     $s = substr($s, 18);
        //
        //     my $name = substr($s, 0, $b[2]);
        //     $s = substr($s, $b[2]);
        //
        //     printf("%s id=%x seq=%x n=%x name=%s", $type == 0x90 ? "root_backref" : "root_ref", $b[0], $b[1], $b[2], $name);
        // } elsif ($type == 0xa8 || $type == 0xa9) { # EXTENT_ITEM_KEY or METADATA_ITEM_KEY
        //     # FIXME - TREE_BLOCK is out by one byte (why?)
        //     if (length($s) == 4) {
        //         @b = unpack("L", $s);
        //         $s = substr($s, 4);
        //         printf("extent_item_v0 refcount=%x", $b[0]);
        //     } else {
        //         @b = unpack("QQQ", $s);
        //         printf("%s refcount=%x gen=%x flags=%s ", $type == 0xa9 ? "metadata_item_key" : "extent_item_key",
        //             $b[0], $b[1], extent_item_flags($b[2]));
        //
        //         $s = substr($s, 24);
        //
        //         my $refcount = $b[0];
        //         if ($b[2] & 2 && $type != 0xa9) {
        //             @b = unpack("QCQC", $s);
        //             printf("key=%x,%x,%x level=%u ", $b[0], $b[1], $b[2], $b[3]);
        //             $s = substr($s, 18);
        //         }
        //
        //         while (length($s) > 0) {
        //             my $irt = unpack("C", $s);
        //             $s = substr($s, 1);
        //
        //             if ($irt == 0xac) {
        //                 @b = unpack("Q", $s);
        //                 $s = substr($s, 8);
        //                 printf("extent_owner_ref root=%x ", $b[0]);
        //             } elsif ($irt == 0xb0) {
        //                 @b = unpack("Q", $s);
        //                 $s = substr($s, 8);
        //                 printf("tree_block_ref root=%x ", $b[0]);
        //             } elsif ($irt == 0xb2) {
        //                 @b = unpack("QQQv", $s);
        //                 $s = substr($s, 28);
        //                 printf("extent_data_ref root=%x objid=%x offset=%x count=%x ", @b);
        //                 $refcount -= $b[3] - 1;
        //             } elsif ($irt == 0xb6) {
        //                 @b = unpack("Q", $s);
        //                 $s = substr($s, 8);
        //                 printf("shared_block_ref offset=%x ", $b[0]);
        //             } elsif ($irt == 0xb8) {
        //                 @b = unpack("Qv", $s);
        //                 $s = substr($s, 12);
        //                 printf("shared_data_ref offset=%x count=%x ", @b);
        //                 $refcount -= $b[1] - 1;
        //             } else {
        //                 printf("unknown %x (length %u)", $irt, length($s));
        //             }
        //         }
        //     }
        // } elsif ($type == 0xb0) { # TREE_BLOCK_REF
        //     printf("tree_block_ref ");
        // } elsif ($type == 0xb2) { # EXTENT_DATA_REF
        //     @b = unpack("QQQv", $s);
        //     $s = substr($s, 28);
        //     printf("extent_data_ref root=%x objid=%x offset=%x count=%x ", @b);
        // } elsif ($type == 0xb4) { # EXTENT_REF_V0
        //     @b = unpack("QQQv", $s);
        //     $s = substr($s, 28);
        //
        //     printf("extent_ref_v0 root=%x gen=%x objid=%x count=%x", @b);
        // } elsif ($type == 0xb6) { # SHARED_BLOCK_REF
        //     printf("shared_block_ref ");
        // } elsif ($type == 0xb8) { # SHARED_DATA_REF
        //     @b = unpack("v", $s);
        //     $s = substr($s, 4);
        //
        //     printf("shared_data_ref count=%x", @b);
        // } elsif ($type == 0xc0) { # BLOCK_GROUP_ITEM
        //     @b = unpack("QQQ", $s);
        //     $s = substr($s, 0x18);
        //     printf("block_group_item size=%x chunktreeid=%x flags=%s", $b[0], $b[1], block_group_item_flags($b[2]));
        // } elsif ($type == 0xc6) { # FREE_SPACE_INFO
        //     @b = unpack("VV", $s);
        //     $s = substr($s, 0x8);
        //     printf("free_space_info count=%x flags=%x", $b[0], $b[1]);
        // } elsif ($type == 0xc7) { # FREE_SPACE_EXTENT
        //     printf("free_space_extent");
        // } elsif ($type == 0xc8) { # FREE_SPACE_BITMAP
        //     printf("free_space_bitmap %s", free_space_bitmap($s, $id));
        //     $s = "";
        // } elsif ($type == 0xcc) { # DEV_EXTENT
        //     @b = unpack("QQQQa16", $s);
        //     $s = substr($s, 0x30);
        //     printf("dev_extent chunktree=%x, chunkobjid=%x, logaddr=%x, size=%x, chunktreeuuid=%s", $b[0], $b[1], $b[2], $b[3], format_uuid($b[4]));

        case btrfs::key_type::DEV_ITEM: {
            const auto& d = *(btrfs::dev_item*)s.data();

            cout << format("dev_item {}", d);

            s = s.subspan(sizeof(btrfs::dev_item));
            break;
        }

        case btrfs::key_type::CHUNK_ITEM: {
            const auto& c = *(btrfs::chunk*)s.data();

            cout << format("chunk_item {}", c);

            s = s.subspan(offsetof(btrfs::chunk, stripe) + (c.num_stripes * sizeof(btrfs::stripe)));
            break;
        }

        // } elsif ($type == 0xf0) { # QGROUP_STATUS
        //     @b = unpack("QQQQQ", $s);
        //     printf("qgroup_status version=%x generation=%x flags=%s rescan=%x enable_gen=%x", $b[0], $b[1], qgroup_status_flags($b[2]), $b[3], $b[4]);
        //     $s = substr($s, 0x28);
        // } elsif ($type == 0xf2) { # QGROUP_INFO
        //     @b = unpack("QQQQQ", $s);
        //     printf("qgroup_info generation=%x rfer=%x rfer_cmpr=%x excl=%x excl_cmpr=%x", $b[0], $b[1], $b[2], $b[3], $b[4]);
        //     $s = substr($s, 0x28);
        // } elsif ($type == 0xf4) { # QGROUP_LIMIT
        //     @b = unpack("QQQQQ", $s);
        //     printf("qgroup_limit flags=%x max_rfer=%x max_excl=%x rsv_rfer=%x rsv_excl=%x", $b[0], $b[1], $b[2], $b[3], $b[4]);
        //     $s = substr($s, 0x28);
        // } elsif ($type == 0xf6) { # QGROUP_RELATION
        //     printf("qgroup_relation");
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
        // } elsif ($type == 0xf9) { # DEV_STATS
        //     print "dev_stats";
        //
        //     while (length($s) > 0) {
        //         printf(" %x", unpack("Q", $s));
        //         $s = substr($s, 8);
        //     }
        // } elsif ($type == 0xfb) { # UUID_SUBVOL
        //     print "uuid_subvol";
        //
        //     while (length($s) > 0) {
        //         printf(" %x", unpack("Q", $s));
        //         $s = substr($s, 8);
        //     }
        // } elsif ($type == 0xfc) { # UUID_REC_SUBVOL
        //     print "uuid_rec_subvol";
        //
        //     while (length($s) > 0) {
        //         printf(" %x", unpack("Q", $s));
        //         $s = substr($s, 8);
        //     }
        // } elsif ($type == 0 && $id == 0xfffffffffffffff5) { # free space
        //     @b = unpack("QCQQQQ", $s);
        //     $s = substr($s, 0x29);
        //
        //     printf("free_space key=(%x,%x,%x) gen=%x num_entries=%x num_bitmaps=%x", @b);
        // } else {

        default:
            cerr << format("ERROR - unknown type {} (size {:x})", key.type, s.size()) << endl;

            cout << format("unknown (size={:x})", s.size());
            unrecog = true;
    }

    if (!unrecog && !s.empty())
        cout << format(" (left={:x})", s.size());

    cout << endl;
}

static void dump_tree(ifstream& f, uint64_t addr, string_view pref, const map<uint64_t, chunk>& chunks) {
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

    // $treenum = $headbits[6];
    //
    if (h.level == 0) {
        auto items = span((btrfs::item*)((uint8_t*)&h + sizeof(btrfs::header)), h.nritems);

        for (const auto& it : items) {
            // FIXME - don't translate key type here
            cout << format("{}{}\n", pref, it.key);

            auto item = span((uint8_t*)tree.data() + sizeof(btrfs::header) + it.offset, it.size);

            dump_item(item, pref, it.key);

    //         if ($treenum == 3 && $ihb[1] == 0xe4) {
    //             my @b = unpack("QQQQVVVvv", $item);
    //             my $stripes = substr($item, 48);
    //             my %obj;
    //
    //             my $numstripes = $b[7];
    //
    //             $obj{'offset'} = $ihb[2];
    //             $obj{'size'} = $b[0];
    //             $obj{'type'} = $b[3];
    //             $obj{'num_stripes'} = $b[7];
    //             $obj{'stripe_len'} = $b[2];
    //             $obj{'sub_stripes'} = $b[8];
    //
    //             for (my $i = 0; $i < $numstripes; $i++) {
    //                 my @cis = unpack("QQa16", $stripes);
    //                 $stripes = substr($stripes, 32);
    //
    //                 $obj{'stripes'}[$i]{'physoffset'} = $cis[1];
    //                 $obj{'stripes'}[$i]{'devid'} = $cis[0];
    //             }
    //
    //             push @l2p, \%obj;
    //
    //             #print Dumper(@l2p);
    //         }
    //
    //         if ($ihb[1] == 0x84) {
    //             if ($treenum == 1) {
    //                 $roots{$ihb[0]} = unpack("x176Q", $item);
    //             } elsif ($treenum == 0xfffffffffffffffa && $ihb[0] == 0xfffffffffffffffa) {
    //                 $logroots{$ihb[2]} = unpack("x176Q", $item);
    //             }
    //         }
        }
    } else {
    //     for (my $i = 0; $i < $numitems; $i++) {
    //         my $itemhead = substr($tree, 0x65 + ($i * 0x21), 0x21);
    //
    //         my @ihb = unpack("QCQQQ", $itemhead);
    //
    //         print $pref;
    //         printf("%x,%x,%x block=%x gen=%x\n", $ihb[0], $ihb[1], $ihb[2], $ihb[3], $ihb[4]);
    //
    //         dump_tree($ihb[3], " " . $pref, $bs);
    //     }
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

static void dump(const filesystem::path& fn) {
    ifstream f(fn);

    if (f.fail())
        throw formatted_error("Failed to open {}", fn.string()); // FIXME - include why

    read_superblock(f);

    // FIXME - multiple devices (including for SYSTEM chunks)

    auto sys_chunks = load_sys_chunks();

    cout << "CHUNK:" << endl;
    dump_tree(f, sb.chunk_root, "", sys_chunks);
    cout << endl;

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
