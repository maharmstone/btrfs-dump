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
    // my $numitems = $headbits[7];
    //
    if (h.level == 0) {
    //     my $headaddr = tell($f);
    //     for (my $i = 0; $i < $numitems; $i++) {
    //         #read($f, my $itemhead, 0x19);
    //         my $itemhead = substr($tree, 0x65 + ($i * 0x19), 0x19);
    //
    //         my @ihb = unpack("QCQVV", $itemhead);
    //
    //         #print Dumper(@ihb)."\n";
    //         print $pref;
    //         printf("%x,%x,%x\n", $ihb[0], $ihb[1], $ihb[2]);
    //
    //         my $item = substr($tree, 0x65 + $ihb[3], $ihb[4]);
    //         dump_item($ihb[1], $item, $pref, $ihb[0], $ihb[2]);
    //
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
    //     }
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
