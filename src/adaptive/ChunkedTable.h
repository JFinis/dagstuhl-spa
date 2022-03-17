#pragma once
#include <cstdint>
#include <vector>
#include "Value.h"
#include "Index.h"

struct ColumnChunk {
    std::shared_ptr<Index> index;
    std::vector<value_t> values;
    value_t min;
    value_t max;
    ColumnChunk(std::vector<value_t> values) : values(std::move(values)) {
        auto [minIt, maxIt] = std::minmax_element(this->values.begin(), this->values.end());
        min = *minIt;
        max = *maxIt;
    }
};

struct File {
    std::vector<ColumnChunk> columnChunks;
};

struct CostModel {
//    int64_t minSavings = 1000;
    int64_t baseQuotaPerFile = 10;
    int64_t costForIndexBuild = 1000;
    int64_t costForIndexGrow = 1000;
    int64_t savingsPerIndexUse = 100;
};

struct ChunkedTable {
    CostModel costModel;
    int64_t savings = 0;
    size_t numColumns;
    size_t chunkSize;

    ChunkedTable(CostModel costModel, size_t numColumns, size_t chunkSize) : costModel(costModel), numColumns(numColumns), chunkSize(chunkSize) {}

    std::vector<File> files;

    bool growIndex(size_t fileId, size_t colId, ScanStats& stats);
};


bool ChunkedTable::growIndex(size_t fileId, size_t colId, ScanStats& stats) {
    const auto& index = files[fileId].columnChunks[colId].index;
    if (files[fileId].columnChunks[colId].index->mergeSome()) {
        savings -= costModel.costForIndexGrow;
        stats.indexesMerged++;
        return true;
    }

    // Merging the index in the file, grow by merging with others
    constexpr size_t fanOut = 2;
    size_t indexSize = index->getNumFilesSpanned();
    size_t indexStart = index->getFirstIndexedFile();
    size_t bucketSize = indexSize * fanOut;
    size_t bucketStart = fileId / bucketSize * bucketSize;
    size_t bucketEnd = std::min(bucketStart + bucketSize, files.size());
    if (bucketStart == indexStart && bucketEnd == bucketStart + indexSize) {
        // We cannot grow, as we're at the end of the file
        return false;
    }

    if (logLevel >= 2) {
        std::cout << "Combining indexes in [" << bucketStart << "," << bucketEnd << "] (size " << bucketSize << ")" << std::endl;
    }
    for (size_t i = bucketStart; i < bucketEnd; i += indexSize) {
        if (i == indexStart) {
            continue;
        }
        auto& otherIndex = files[i].columnChunks[colId].index;
        // A file to be merged with doesn't have an index, yet. Build that index and call it a day
        if (!otherIndex) {
            otherIndex = make_shared<Index>(files[i].columnChunks[colId].values, i);
            stats.indexesCreated++;
            savings -= costModel.costForIndexBuild;
            return true;
        }
        // A neighboring index isn't big enough, yet.
        if (otherIndex->getNumFilesSpanned() < indexSize) {
            // Grow the neighbor index first, if possible
            if (growIndex(i, colId, stats)) {
                return true;
            }
            // (if we're here, then the other index couldn't be grown, as we're at the end of the table)
        } else if (otherIndex->getNumFilesSpanned() > indexSize) {
            // should never happen...
            throw;
        }
    }

    // If we arrive here, we can merge the indexes, so collect them
    std::vector<std::shared_ptr<Index>> indexes;
    for (size_t i = bucketStart; i < bucketEnd; i += indexSize) {
        indexes.push_back(files[i].columnChunks[colId].index);
    }
    std::shared_ptr<Index> combinedIndex = std::make_shared<Index>(move(indexes));
    stats.indexesCombined++;
    for (size_t i = bucketStart; i < bucketEnd; ++i) {
        files[i].columnChunks[colId].index = combinedIndex;
    }
    // Do a bit of work
    if (combinedIndex->mergeSome()) {
        savings -= costModel.costForIndexGrow;
        stats.indexesMerged++;
    }

    return true;
}