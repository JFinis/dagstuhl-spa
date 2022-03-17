#pragma once
#include "ChunkedTable.h"
#include <cassert>
#include <random>
#include <iostream>



struct RandomGenerator {
    value_t min;
    value_t max;
    std::mt19937 rng = std::mt19937(1337);

    std::vector<value_t> operator()(size_t chunkSize) {
        std::vector<value_t> result(chunkSize);
        std::uniform_int_distribution<value_t> uni(min, max);
        for (value_t& v : result) {
            v = uni(rng);
        }
        return result;
    }
 };

void insertRecursive(ChunkedTable& t){
}

template<typename G1, typename... Generator>
void insertRecursive(ChunkedTable& t, G1&& generator, Generator&&... otherGenerators){
    t.files.back().columnChunks.emplace_back(generator(t.chunkSize));
    insertRecursive(t, std::forward<Generator>(otherGenerators)...);
}

template<typename... Generator>
void insertRowGroup(ChunkedTable& t, Generator&&... generators){
    assert(sizeof...(Generator) == t.numColumns);
    t.files.emplace_back();
    insertRecursive(t, std::forward<Generator>(generators)...);
}
