# Local build without cmake:  make -C cpp test   (CMakeLists.txt is the build for the Linux servers)
CXX      ?= clang++
CXXFLAGS ?= -std=c++20 -O2 -Wall -Wextra -Wno-unused-parameter -pthread
INC      := -I.
B        := build

CORE := core/ini.cpp core/tz.cpp core/market.cpp core/mirror_ring.cpp core/process_config.cpp core/event_bus.cpp core/plugin.cpp core/app.cpp core/sql.cpp core/statedb.cpp
DATA := data/ltt_parser.cpp data/token_index.cpp data/candle_aggregator.cpp data/master.cpp
# libpq's header lives under a "postgresql" subdir on Debian/Ubuntu (/usr/include/postgresql/libpq-fe.h), not on the
# plain include path; sqlite3.h usually is on the plain path already, so only the extra -I is needed here.
MASTER_INC  := -I/usr/include/postgresql -I/usr/local/include/postgresql
MASTER_LIBS := -lsqlite3 -lpq

.PHONY: all test parity master-parity tsan sdk sdk-test sdk-check clean
all: $(B)/test_core $(B)/candles_replay $(B)/master_dump $(B)/master_build sdk

# strategy plugins: C++17, hidden visibility, no algod code linked (the SDK's build rules)
SO_FLAGS := -std=c++17 -O3 -fPIC -fvisibility=hidden -fvisibility-inlines-hidden -Wall -Wextra -Wno-unused-parameter -shared -Isdk
sdk: $(B)/libNiftySmaCross.so $(B)/libBasketRsiSingle.so $(B)/mock_host

$(B)/libNiftySmaCross.so: sdk/examples/NiftySmaCross/NiftySmaCross.cpp sdk/AlgoSDK.h sdk/AlgoUtil.h | $(B)
	$(CXX) $(SO_FLAGS) $< -o $@

$(B)/libBasketRsiSingle.so: sdk/examples/BasketRsiSingle/BasketRsiSingle.cpp sdk/AlgoSDK.h sdk/AlgoUtil.h | $(B)
	$(CXX) $(SO_FLAGS) $< -o $@

$(B)/mock_host: sdk/tests/mock_host.cpp core/ini.cpp sdk/AlgoSDK.h | $(B)
	$(CXX) $(CXXFLAGS) $(INC) -Isdk core/ini.cpp sdk/tests/mock_host.cpp -o $@

sdk-test: sdk sdk-check
	./$(B)/mock_host $(B)/libNiftySmaCross.so sdk/examples/NiftySmaCross/nifty_sma.ini

# every strategy .so: exactly the three factory exports, and no writable global data (all state must be per instance)
sdk-check: $(B)/libNiftySmaCross.so $(B)/libBasketRsiSingle.so
	./tools/check_plugin.sh $^

$(B):
	mkdir -p $(B)

$(B)/test_core: $(CORE) $(DATA) tests/test_core.cpp $(wildcard core/*.h data/*.h) | $(B)
	$(CXX) $(CXXFLAGS) $(INC) $(MASTER_INC) $(CORE) $(DATA) tests/test_core.cpp $(MASTER_LIBS) -o $@

$(B)/candles_replay: $(CORE) $(DATA) tools/candles_replay.cpp $(wildcard core/*.h data/*.h) | $(B)
	$(CXX) $(CXXFLAGS) $(INC) $(MASTER_INC) $(CORE) $(DATA) tools/candles_replay.cpp $(MASTER_LIBS) -o $@

$(B)/master_dump: $(CORE) $(DATA) tools/master_dump.cpp $(wildcard core/*.h data/*.h) | $(B)
	$(CXX) $(CXXFLAGS) $(INC) $(MASTER_INC) $(CORE) $(DATA) tools/master_dump.cpp $(MASTER_LIBS) -o $@

$(B)/master_build: $(CORE) $(DATA) tools/master_build.cpp $(wildcard core/*.h data/*.h) | $(B)
	$(CXX) $(CXXFLAGS) $(INC) $(MASTER_INC) $(CORE) $(DATA) tools/master_build.cpp $(MASTER_LIBS) -o $@

$(B)/test_core_tsan: $(CORE) $(DATA) tests/test_core.cpp | $(B)
	$(CXX) -std=c++20 -O1 -g -pthread -fsanitize=thread $(INC) $(MASTER_INC) $(CORE) $(DATA) tests/test_core.cpp $(MASTER_LIBS) -o $@

test: $(B)/test_core
	./$(B)/test_core

parity: $(B)/candles_replay
	cd .. && python3 cpp/tests/parity/candles_parity.py --bin cpp/$(B)/candles_replay

master-parity: $(B)/master_dump
	cd .. && python3 cpp/tests/parity/master_parity.py --bin cpp/$(B)/master_dump

tsan: $(B)/test_core_tsan
	./$(B)/test_core_tsan

clean:
	rm -rf $(B)
