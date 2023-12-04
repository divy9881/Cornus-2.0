#!/bin/bash

cd $HOME
# Clone the project
git clone https://github.com/cpp-redis/cpp_redis.git
# Go inside the project directory
cd cpp_redis
# Reset to a particular commit
git reset --hard ab5ea8638bc51e3d407b0045aceb5c5fd3218aa0
# Get tacopie submodule
git submodule update --init
# Create a build directory and move into it
mkdir -p build && cd build
# Generate the Makefile using CMake
cmake .. -DCMAKE_BUILD_TYPE=Release
# Build the library
make
# Install the library
sudo make install
