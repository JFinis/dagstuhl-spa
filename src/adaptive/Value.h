#pragma once
#include <cstdint>

constexpr int logLevel = 0;

using value_t = int64_t;


struct ScanStats {
    size_t rowGroupsScanned = 0;
    size_t rowGroupsMatched = 0;
    size_t cost = 0;
    size_t indexesCreated = 0;
    size_t indexesCombined = 0;
    size_t indexesMerged = 0;
};