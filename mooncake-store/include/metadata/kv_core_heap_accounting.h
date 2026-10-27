#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace mooncake {

// Only allocations explicitly using these types belong to the metric. In
// particular, storage payloads, segment allocators and RPC DTOs do not.
class CoreHeapAccount {
   public:
    void Add(size_t bytes) noexcept {
        bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void Sub(size_t bytes) noexcept {
        [[maybe_unused]] const auto previous =
            bytes_.fetch_sub(bytes, std::memory_order_relaxed);
        assert(previous >= bytes);
    }

    uint64_t Load() const noexcept {
        return bytes_.load(std::memory_order_relaxed);
    }

   private:
    std::atomic<uint64_t> bytes_{0};
};

// Trivial process-lifetime state: callbacks and retired services may free
// allocations after their original MasterService has stopped serving.
inline CoreHeapAccount& GetMasterCoreHeapAccount() noexcept {
    static CoreHeapAccount account;
    return account;
}

template <typename T>
class CoreHeapAllocator {
   public:
    using value_type = T;
    using is_always_equal = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;

    CoreHeapAllocator() noexcept = default;
    template <typename U>
    CoreHeapAllocator(const CoreHeapAllocator<U>&) noexcept {}

    [[nodiscard]] T* allocate(size_t count) {
        // std::allocator checks the size multiplication before allocating.
        T* pointer = std::allocator<T>{}.allocate(count);
        GetMasterCoreHeapAccount().Add(count * sizeof(T));
        return pointer;
    }

    void deallocate(T* pointer, size_t count) noexcept {
        std::allocator<T>{}.deallocate(pointer, count);
        GetMasterCoreHeapAccount().Sub(count * sizeof(T));
    }

    template <typename U>
    bool operator==(const CoreHeapAllocator<U>&) const noexcept {
        return true;
    }
};

template <typename T>
using CoreVector = std::vector<T, CoreHeapAllocator<T>>;

// Keep allocation-bearing strings internal. Conversion to std::string creates
// an ordinary, untracked wire/query value, never another tracked key.
class CoreString : public std::basic_string<char, std::char_traits<char>,
                                            CoreHeapAllocator<char>> {
    using Base = std::basic_string<char, std::char_traits<char>,
                                   CoreHeapAllocator<char>>;

   public:
    using Base::Base;
    using Base::operator=;
    CoreString() = default;
    CoreString(const std::string& value) : Base(value.data(), value.size()) {}
    CoreString(std::string_view value) : Base(value.data(), value.size()) {}

    operator std::string() const { return {data(), size()}; }

    friend bool operator==(const CoreString& left,
                           const CoreString& right) noexcept {
        return std::string_view(left) == std::string_view(right);
    }
    friend bool operator==(const CoreString& left,
                           std::string_view right) noexcept {
        return std::string_view(left) == right;
    }
    friend bool operator==(const CoreString& left,
                           const std::string& right) noexcept {
        return std::string_view(left) == std::string_view(right);
    }
    friend bool operator==(const CoreString& left, const char* right) noexcept {
        return std::string_view(left) == right;
    }
};

struct CoreStringHash {
    using is_transparent = void;
    size_t operator()(std::string_view value) const noexcept {
        return std::hash<std::string_view>{}(value);
    }
};

struct CoreStringEqual {
    using is_transparent = void;
    bool operator()(std::string_view left,
                    std::string_view right) const noexcept {
        return left == right;
    }
};

template <typename T>
struct CoreHeapDeleter {
    void operator()(T* pointer) const noexcept {
        std::destroy_at(pointer);
        CoreHeapAllocator<T>{}.deallocate(pointer, 1);
    }
};

template <typename T>
using CoreUniquePtr = std::unique_ptr<T, CoreHeapDeleter<T>>;

template <typename T, typename... Args>
CoreUniquePtr<T> MakeCoreUnique(Args&&... args) {
    CoreHeapAllocator<T> allocator;
    T* pointer = allocator.allocate(1);
    try {
        std::construct_at(pointer, std::forward<Args>(args)...);
    } catch (...) {
        allocator.deallocate(pointer, 1);
        throw;
    }
    return CoreUniquePtr<T>(pointer);
}

template <typename T, typename... Args>
std::shared_ptr<T> MakeCoreShared(Args&&... args) {
    return std::allocate_shared<T>(CoreHeapAllocator<T>{},
                                   std::forward<Args>(args)...);
}

}  // namespace mooncake
