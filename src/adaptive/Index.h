#pragma once
#include "Value.h"
#include <compare>
#include <iostream>

struct IndexEntry {
    /// The value
    value_t value;
    /// The filw id
    size_t fileId;

    auto operator<=>(const IndexEntry& other) const = default;
};



class Index {
    using IndexPos = typename std::vector<IndexEntry>::iterator;
    struct IndexToMerge {
        std::shared_ptr<Index> index;
        IndexPos mergePos;

        IndexToMerge(std::shared_ptr<Index> index) : index(index), mergePos(index->index.begin()) {}
    };

    size_t firstFile, lastFile;
    std::vector<IndexEntry> index;
    std::vector<IndexToMerge> indexesToMerge;

public:
    void probe(value_t v, std::vector<std::size_t>& result) const {
        auto [begin, end] = std::equal_range(index.begin(), index.end(), IndexEntry{v,0}, [](IndexEntry e1, IndexEntry e2){ return e1.value < e2.value; });
        for (auto it = begin; it != end; ++it) {
            result.push_back(it->fileId);
        }
        if (indexesToMerge.size()) {
            for (const auto& index : indexesToMerge) {
                index.index->probe(v, result);
            }
            // Remove duplicates
            sort( result.begin(), result.end() );
            result.erase( unique( result.begin(), result.end() ), result.end() );
        }
    }

    size_t getFirstIndexedFile() const {
        return firstFile;
    }
    size_t getLastIndexedFile() const {
        return lastFile;
    }
    size_t getNumFilesSpanned() const {
        return lastFile - firstFile + 1;
    }

    Index(const std::vector<value_t>& values, size_t fileId) : firstFile(fileId), lastFile(fileId) {
        index.reserve(values.size());
        for (value_t v : values) {
            index.push_back({v, fileId});
        }
        std::sort(index.begin(), index.end());
        if (logLevel >= 2) {
            std::cout << "Created index " << firstFile << " (size " << getNumFilesSpanned() << ")" << std::endl;
        }
    }

    Index(std::vector<std::shared_ptr<Index>> indexesToMerge) : firstFile(indexesToMerge.front()->firstFile), lastFile(indexesToMerge.back()->lastFile) {
        for (const auto& index : indexesToMerge) {
            this->indexesToMerge.emplace_back(index);
        }
        size_t combinedSize = 0;
        for (const auto& i : this->indexesToMerge) {
            combinedSize += i.index->index.size();
        }
        index.reserve(combinedSize);
    }

    // Copied from std::merge :)
    template<class InputIt1, class InputIt2, class OutputIt>
    bool merge(InputIt1& first1, InputIt1 last1,
                   InputIt2& first2, InputIt2 last2,
                   OutputIt d_first, ssize_t& mergeLimit)
    {
        for (; first1 != last1 && mergeLimit != 0; ++d_first) {
            if (first2 == last2) {
                ssize_t num = std::min(last1-first1,mergeLimit);
                std::copy_n(first1, num, d_first);
                first1 += num;
                return first1 == last1;
            }
            --mergeLimit;
            if (*first2 < *first1) {
                *d_first = *first2;
                ++first2;
            } else {
                *d_first = *first1;
                ++first1;
            }
        }
        ssize_t num = std::min(last2-first2,mergeLimit);
        std::copy_n(first2, num, d_first);
        first2 += num;
        return first2 == last2 && first1 == last1;
    }

    bool mergeSome(){
        // For now, we merge everything
        if (indexesToMerge.empty()) {
            return false;
        }
        assert(indexesToMerge.size() == 2);
        constexpr size_t mergeLimit = 1'600'000;
        ssize_t numToMerge = mergeLimit;



        if(merge(indexesToMerge[0].mergePos, indexesToMerge[0].index->index.end(),
                   indexesToMerge[1].mergePos, indexesToMerge[1].index->index.end(),
                   std::back_inserter(index), numToMerge)) {
            indexesToMerge.clear();
        }
        if (logLevel >= 2) {
            std::cout << "Merged " << (mergeLimit - numToMerge) << " in index " << firstFile << " (size " << getNumFilesSpanned() << ")" << std::endl;
        }

        return true;
    }

};

