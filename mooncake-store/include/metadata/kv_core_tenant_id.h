#pragma once

#include "metadata/kv_core_heap_accounting.h"
#include "tenant_id.h"

namespace mooncake {

// The persistent, allocation-tracked representation; request TenantId objects
// and auxiliary tenant indexes continue to use the public representation.
class CoreTenantId {
   public:
    CoreTenantId() : value_(TenantId::kDefaultValue) {}
    CoreTenantId(const TenantId& tenant) : value_(tenant.value()) {}

    const CoreString& value() const noexcept { return value_; }
    bool IsDefault() const noexcept {
        return value_ == TenantId::kDefaultValue;
    }
    operator TenantId() const { return TenantId(std::string(value_)); }

    std::string MakeScopedKey(std::string_view key) const {
        return TenantId(*this).MakeScopedKey(key);
    }

   private:
    CoreString value_;
};

struct CoreTenantIdHash {
    using is_transparent = void;
    template <typename T>
    size_t operator()(const T& tenant) const noexcept {
        return CoreStringHash{}(std::string_view(tenant.value()));
    }
};

struct CoreTenantIdEqual {
    using is_transparent = void;
    template <typename L, typename R>
    bool operator()(const L& left, const R& right) const noexcept {
        return std::string_view(left.value()) ==
               std::string_view(right.value());
    }
};

}  // namespace mooncake
