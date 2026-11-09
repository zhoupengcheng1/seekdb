/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
struct ObFTDictHub::CachedDictionary
{
  explicit CachedDictionary(const ObFTDictDesc &desc)
      : allocator_("IKSharedDict"), container_(allocator_), dictionary_(allocator_, &container_, desc) {}
  ObArenaAllocator allocator_;
  ObFTCacheRangeContainer container_;
  ObFTRangeDict dictionary_;
};

ObFTDictHub::~ObFTDictHub() { destroy(); }

int ObFTDictHub::get_builtin_dictionary(const ObFTDictDesc &desc, ObIFTDict *&dictionary)
{
  int ret = OB_SUCCESS;
  int slot = -1;
  switch (desc.type_) {
    case ObFTDictType::DICT_IK_MAIN: slot = 0; break;
    case ObFTDictType::DICT_IK_QUAN: slot = 1; break;
    case ObFTDictType::DICT_IK_STOP: slot = 2; break;
    default: return OB_NOT_SUPPORTED;
  }
  if (!is_inited_) {
    return OB_NOT_INIT;
  }
  CachedDictionary *cached = builtin_[slot].load(std::memory_order_acquire);
  if (cached == nullptr) {
    std::lock_guard<std::mutex> guard(builtin_mutex_);
    cached = builtin_[slot].load(std::memory_order_relaxed);
    if (cached == nullptr) {
      if (OB_ISNULL(cached = OB_NEW(CachedDictionary, "IKSharedDict", desc))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ret = load_cache(desc, cached->container_);
        if (ret == OB_ENTRY_NOT_EXIST) {
          ret = build_cache(desc, cached->container_);
        }
        if (OB_SUCC(ret)) {
          ret = cached->dictionary_.init();
        }
        if (OB_SUCC(ret)) {
          builtin_[slot].store(cached, std::memory_order_release);
        } else {
          OB_DELETE(CachedDictionary, "IKSharedDict", cached);
          cached = nullptr;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    dictionary = &cached->dictionary_;
  }
  return ret;
}

int ObFTDictHub::init()
{
  static constexpr int K_MAX_DICT_BUCKET = 128; // for now, only built-in dicts.
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  for (auto &entry : builtin_) {
    CachedDictionary *cached = entry.exchange(nullptr, std::memory_order_acq_rel);
    if (cached != nullptr) {
      OB_DELETE(CachedDictionary, "IKSharedDict", cached);
    }
  }
  return ret;
}
int ObFTDictHub::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());

    // try if valid with no recursive lock
    if (OB_FAIL(get_dict_info(key, info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
        ret = OB_ENTRY_NOT_EXIST;
      } else {
      }
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
        } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
        } else if (OB_FAIL(put_dict_info(key, info))) {
        }
      }
    }
  }
  return ret;
}

int ObFTDictHub::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    {
      ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
      if (OB_FAIL(get_dict_info(key, info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // dict not exist, make new one, by caller
          ret = OB_ENTRY_NOT_EXIST;
        } else {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
      } else {
      }
    }
  }

  return ret;
}


int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
