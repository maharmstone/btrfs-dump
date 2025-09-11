#include <iostream>
#include <filesystem>
#include <fstream>

import cxxbtrfs;
import formatted_error;

using namespace std;

static void dump(const filesystem::path& fn) {
    ifstream f(fn);

    if (f.fail())
        throw formatted_error("Failed to open {}", fn.string()); // FIXME - include why

    // FIXME
}

int main() {
    // FIXME - solicit filename

    try {
        dump("test");
    } catch (const exception& e) {
        cerr << "Exception: " << e.what() << endl;
    }
}
