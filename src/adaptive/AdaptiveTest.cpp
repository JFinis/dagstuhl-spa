#include "ChunkedTable.h"
#include "Insert.h"
#include "Scan.h"
#include <iostream>

template<
        class result_t   = std::chrono::milliseconds,
        class clock_t    = std::chrono::steady_clock,
        class duration_t = std::chrono::milliseconds
>
auto since(std::chrono::time_point<clock_t, duration_t> const &start) {
    return std::chrono::duration_cast<result_t>(clock_t::now() - start);
}


template<typename Lambda>
void benchmark(size_t queryId, std::string_view name, Lambda &&lambda, ChunkedTable &t) {
    if (logLevel >= 1) std::cout << name << std::endl;
    auto start = std::chrono::steady_clock::now();

    lambda();

    auto timeMs = since(start).count();
    //std::cout << name << " Elapsed(ms)=" << timeMs<< std::endl;
    std::cout << t.costModel.baseQuotaPerFile << ";" << queryId << ";" << timeMs << std::endl;
    if (logLevel >= 1) std::cout << "---------------------" << std::endl;
}

struct Target {
    ScanStats stats;
    value_t v1 = 0, v2 = 0;

    void operator()(const File &g, size_t i) {
        v1 += g.columnChunks[0].values[i];
        v2 += g.columnChunks[1].values[i];
    }
};

struct Predicate {
    size_t colId = 0;
    value_t equalsValue = 5;

    bool operator()(value_t v) const {
        return v == equalsValue;
    }

    bool intersectsMinMax(const ColumnChunk &c) {
        return c.min <= equalsValue && c.max >= equalsValue;
    }

    Predicate(size_t colId, value_t equalsValue) : colId{colId}, equalsValue{equalsValue} {}
};

int main() {
    for (int baseQuota: {0, 1, 4, 16, 64, 256}) {
        CostModel costModel;
        costModel.baseQuotaPerFile = baseQuota;
        ChunkedTable t{costModel, 2, 100'000};
        RandomGenerator c1gen{0, 1'000'000'000};
        RandomGenerator c2gen{0, 1'000'000};
        constexpr unsigned numFiles = 1024;
        if (logLevel >= 1)
            std::cout << "Creating table with " << numFiles << " files, each having " << t.chunkSize << " rows..."
                      << std::endl;
        for (unsigned i = 0; i < numFiles; ++i) {
            insertRowGroup(t, c1gen, c2gen);
        }

        Random random{1'000'000};
        Random r2{1'000'000'000};

        t.savings = 0;
        for (int i = 0; i < 1000; ++i) {
            Target target;
            Predicate predicate(0, r2.next());
            if (logLevel >= 1) {
                std::cout << "WHERE c1 = " << predicate.equalsValue << std::endl;
            }
            benchmark(i, std::string{"Query "} + std::to_string(i), [&]() {
                scan<true>(t, random, target, predicate);
            }, t);
            if (logLevel >= 1) {
                std::cout << "Result: " << target.v1 << ", " << target.v2 << std::endl;
                const auto &stats = target.stats;
                std::cout << "Stats: Scanned: " << stats.rowGroupsScanned << ", Matched: " << stats.rowGroupsMatched
                          << ", Cost: " << stats.cost << std::endl;
                std::cout << "Indexes: Created: " << stats.indexesCreated << ", Combined: " << stats.indexesCombined
                          << ", Merged: " << stats.indexesMerged << std::endl;
            }
        }
    }
}