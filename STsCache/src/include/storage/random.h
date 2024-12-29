#ifndef SEMANTIC_CACHE_RANDOM_H
#define SEMANTIC_CACHE_RANDOM_H
#include <cstdint>

class Random {

public:
    explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
        if (seed == 0 || seed == 2147483647L) {
            seed = 1;
        }
    }

    uint32_t Next() {
        static const uint32_t M = 2147483647L;
        static const uint64_t A = 16807;

        uint64_t product = seed * A;

        seed = static_cast<uint32_t>((product >> 31) + (product & M));
        if (seed > M) {
            seed -= M;
        }
        return seed;
    }

    uint32_t Uniform(int n) { return (Next() % n); }

    bool OneIn(int n) { return (Next() % n) == 0; }

    uint32_t Skewed(int max_log) {
        return Uniform(1 << Uniform(max_log + 1));
    }


private:
    uint32_t seed;
};

#endif
