btrfs-dump
==========

`btrfs-dump` is a metadata dumper for btrfs, which produces compact text output.
The chief advantage over `btrfs inspect-internal dump-tree` is that is easily
diff-able, so you can do for instance:

```shell
# ./btrfs-dump -t 5 test > before.txt
# mount -o noatime test /root/temp
# touch /root/temp/file
# umount /root/temp
# ./btrfs-dump -t 5 test > after.txt
# diff -u before.txt after.txt
```

and get:

```diff
--- bef.txt     2025-09-18 18:34:18.336661321 +0100
+++ aft.txt     2025-09-18 18:34:31.176590220 +0100
@@ -1,4 +1,4 @@
-header csum=8cffdf1f fsid=11ca5960-0c9f-4536-9de0-765251e106b2 bytenr=1d00000 flags=written,mixed_backref chunk_tree_uuid=68b52de7-e2bd-4fcf-bf3d-5c9f6a0995eb generation=b owner=5 nritems=6 level=0 physical=test,2500000;test,5830000
+header csum=27acbb5d fsid=11ca5960-0c9f-4536-9de0-765251e106b2 bytenr=1d04000 flags=written,mixed_backref chunk_tree_uuid=68b52de7-e2bd-4fcf-bf3d-5c9f6a0995eb generation=c owner=5 nritems=6 level=0 physical=test,2504000;test,5834000
 100,1,0
 inode_item generation=3 transid=b size=8 nbytes=4000 block_group=0 nlink=1 uid=0 gid=0 mode=40755 rdev=0 flags=0 sequence=2 atime=2025-09-18T17:30:39 ctime=2025-09-18T17:30:02 mtime=2025-09-18T17:30:02 otime=2025-09-18T17:29:32
 100,c,100
@@ -8,6 +8,6 @@
 100,60,2
 dir_index location=101,1,0 transid=a data_len=0 name_len=4 type=reg_file name=file
 101,1,0
-inode_item generation=a transid=b size=0 nbytes=0 block_group=0 nlink=1 uid=0 gid=0 mode=100644 rdev=0 flags=0 sequence=1a atime=2025-09-18T17:30:39 ctime=2025-09-18T17:30:39 mtime=2025-09-18T17:30:39 otime=2025-09-18T17:30:02
+inode_item generation=a transid=c size=0 nbytes=0 block_group=0 nlink=1 uid=0 gid=0 mode=100644 rdev=0 flags=0 sequence=1b atime=2025-09-18T17:34:26 ctime=2025-09-18T17:34:26 mtime=2025-09-18T17:34:26 otime=2025-09-18T17:30:02
 101,c,100
 inode_ref index=2 name_len=4 name=file
```

So you can see that the following happened by touch-ing a file...
* The atime, ctime, and mtime of the inode_item for inode 101 were updated
* The sequence value of the inode_item was increased by one
* The transid value of the inode_item was increased
* The virtual address ("bytenr" in "header") of the tree block changed (i.e. because it was COWed)
* The disk location ("physical") of the block changed (from 2500000 and 5830000 in test to 2504000 and 5834000 - two copies because we have DUP metadata)
* And finally that the csum value of the header changed, because the contents of the block changed

Note that unlike `btrfs-progs` and `dmesg` virtually all the nubers are in hex,
because it's easier to work with. The exceptions are the dates and times
(obviously) and the "mode" of inode_item, which is in its traditional octal
form.

The supported output should be completely comprehensive: if you find something
valid that `btrfs-dump` fails to understand, please open an issue.

Usage
-----

`btrfs-dump [option] <device> [<device>...]`

Options:

* `-t|--tree <tree_id>`: only output the specified tree. Values can be decimal,
hexadecimal (with a leading "0x"), or the same strings that `btrfs-progs`
supports (e.g. `-t fs` as a synonym for `-t 5`)

* `-p|--physical`: also print the physical addresses of a block in the tree
header. This can be useful for feeding to `dd`, or if you have the image open
in a hex editor.

If you only give one device for a multi-device filesystem, it will use
`libblkid` to try and find the other devices - or you can always specify them
manually.

Compilation
-----------

This uses C++ modules, so you will need a recent version of CMake and GCC. You
will also need `libblkid`.

```shell
$ mkdir build
$ cd build
$ cmake -GNinja ..
$ ninja
```

Changelog
---------

* 20260123:
    * Added `--physical` option
    * Added definitions for experimental incompat feature REMAP_TREE

* 20250918: Initial release
