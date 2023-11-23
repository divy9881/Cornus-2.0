#TODO: use clang++ for arm64
CC=g++
PROTOC=protoc
CPPF = `pkg-config --cflags protobuf grpc`

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./transport/ ./proto/ ./utils/ 
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

CFLAGS = -Wno-unknown-pragmas -Wall -g -std=c++17
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./transport -I./proto -I./utils -I$(HOME)/cpp_redis/includes -I$(HOME)/cpp_redis/tacopie/includes/
# for azure storage
# INCLUDE += -I/home/kanwu/vcpkg/installed/x64-linux/include

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Og -g -fpermissive

LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++11 -Og -ljemalloc
LDLIB = -lssl -lcrypto -lre2 -lcares

# for redis storage & grpc
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl\
           -lz -laddress_sorting -lutf8_range_lib -lutf8_validity -lutf8_range -lupb_textformat_lib -lupb_json_lib -lupb_collections_lib -lupb -lutf8_range -lcpp_redis -ltacopie -lssl -lcrypto -lre2 -lcares
# for azure storage
# LDFLAGS += -lcpprest -lboost_system -lboost_log -lboost_log_setup -lboost_thread -lboost_serialization\
#            -luuid -lxml2 -lz -llzma -lssl -lcrypto -lpthread -ldl
# LDFLAGS += -L/home/kanwu/vcpkg/installed/x64-linux/lib -lazurestorage\

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

rundb: TARGET=rundb
rundb: $(OBJS)
		$(CC) -no-pie -o $@ $^ $(LDFLAGS) $(LDLIBS)

runstorage: TARGET=runstorage
runstorage: $(OBJS)
		$(CC) -no-pie -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
		$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
		$(CC) $(CPPF) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb runstorage $(OBJS) $(DEPS)
