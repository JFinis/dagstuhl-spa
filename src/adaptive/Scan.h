#pragma once
#include "ChunkedTable.h"
#include <iostream>


struct Random {

    std::mt19937 rng;
    std::uniform_int_distribution<ssize_t> uni;

    Random(ssize_t max) : rng(1337), uni(0, max) {}

    bool operator()(ssize_t s) {
        return uni(rng) < s;
    }

    ssize_t next() {
        return uni(rng);
    }
};


template<typename Projection, typename Predicate>
bool scanFile(ChunkedTable& t, size_t fileId, Projection&& p, Predicate&& predicate){
    auto f = t.files[fileId];
    const ColumnChunk& c = f.columnChunks[predicate.colId];
    uint64_t hits = 0;
    for (size_t i = 0; i < t.chunkSize; ++i) {
        value_t v = c.values[i];
//        std::cout << "VALUE " << v << std::endl;
        if (predicate(v)) {
//            std::cout << "MATCH " << v << " AGAINST " << predicate.equalsValue << std::endl;
            ++hits;
            p(f, i);
        }
    }
    p.stats.cost++;
    p.stats.rowGroupsScanned++;
    if (hits > 0) {
        p.stats.rowGroupsMatched++;
    }
    return hits > 0;
}

template<bool useIndexing, typename Random, typename Projection, typename Predicate>
void scan(ChunkedTable& t, Random& random, Projection&& p, Predicate&& predicate){
    std::vector<size_t> indexResultVector;
    t.savings = t.files.size() * t.costModel.baseQuotaPerFile;
    for (size_t r = 0; r < t.files.size(); ++r) {
        File& f = t.files[r];
        ColumnChunk& c = f.columnChunks[predicate.colId];
        if (useIndexing && c.index) {
            indexResultVector.clear();
            c.index->probe(predicate.equalsValue, indexResultVector);
            p.stats.cost++;
            for (size_t fileId : indexResultVector) {
                scanFile(t, fileId, p, predicate);
            }
            r = c.index->getLastIndexedFile();
            t.savings += t.costModel.savingsPerIndexUse;

            // We saved, consider growing...
            if (random(t.savings)) {
                if (t.growIndex(r, predicate.colId, p.stats)) {
                }
            }

        } else {
            if (!predicate.intersectsMinMax(c)) {
                continue;
            }
            if (!scanFile(t, r, p, predicate)) {
                 // Might be good to build an index...
                 if (useIndexing && random(t.savings)) {
                     c.index = std::make_shared<Index>(c.values, r);
                     p.stats.indexesCreated++;
                     t.savings = t.savings - t.costModel.costForIndexBuild;
                 }
            }

        }
    }
}
