#include <iostream>
#include <string>

#include "OMNI.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <command> [options]"
                  << std::endl;
        return 1;
    }

    std::string command = argv[1];
    cae::OMNI omni;

    if (command == "put") {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " put <omni.yaml>"
                      << std::endl;
            return 1;
        }
        return omni.Put(argv[2]);

    } else if (command == "get") {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " get <buffer>"
                      << std::endl;
            return 1;
        }
        return omni.Get(argv[2]);

    } else if (command == "ls") {
        std::cout << "connecting runtime" << std::endl;
        return omni.List();

    } else {
        std::cerr << "Error: invalid command - " << command << std::endl;
        return 1;
    }

    return 0;
}
