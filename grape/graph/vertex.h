/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPE_GRAPH_VERTEX_H_
#define GRAPE_GRAPH_VERTEX_H_

#include <utility>

#include "grape/config.h"
#include "grape/types.h"

namespace grape {

namespace internal {

class InArchive;
class OutArchive;

/**
 * @brief Vertex representation.
 *
 * @tparam VID_T
 * @tparam VDATA_T
 */
template <typename VID_T, typename VDATA_T>
struct Vertex {
  DEV_HOST Vertex() {}

  DEV_HOST explicit Vertex(const VID_T& vid) : vid(vid), vdata(), min_iter(0), secret(0) {}
  DEV_HOST Vertex(const VID_T& vid, const VDATA_T& vdata)
      : vid(vid), vdata(vdata), min_iter(0), secret(0) {}
  DEV_HOST Vertex(const VID_T& vid, VDATA_T&& vdata)
      : vid(vid), vdata(std::move(vdata)), min_iter(0), secret(0) {}

  DEV_HOST Vertex(const VID_T& vid, const VDATA_T& vdata, const int32_t min_iter, const int32_t secret)
      : vid(vid), vdata(vdata), min_iter(min_iter), secret(secret) {}
  DEV_HOST Vertex(const VID_T& vid, VDATA_T&& vdata, const int32_t min_iter, const int32_t secret)
      : vid(vid), vdata(std::move(vdata)), min_iter(min_iter), secret(secret) {}

  DEV_HOST Vertex(const Vertex& vert)
      : vid(vert.vid), vdata(vert.vdata), min_iter(vert.min_iter), secret(vert.secret) {}
  DEV_HOST Vertex(Vertex&& vert) noexcept
      : vid(vert.vid), vdata(std::move(vert.vdata)), min_iter(vert.min_iter), secret(vert.secret) {}

  DEV_HOST ~Vertex() {}

  DEV_HOST Vertex& operator=(const Vertex& rhs) {
    if (this == &rhs) {
      return *this;
    }
    vid = rhs.vid;
    vdata = rhs.vdata;
    min_iter = rhs.min_iter;
    secret = rhs.secret;
    return *this;
  }

  VID_T vid;
  VDATA_T vdata;
  int32_t min_iter;
  int32_t secret;
};

/**
 * @brief Partial specialization for Vertex with EmptyType on vertex_data.
 *
 */
template <typename VID_T>
struct Vertex<VID_T, EmptyType> {
  DEV_HOST Vertex() {}

  DEV_HOST explicit Vertex(const VID_T& vid) : vid(vid), secret(0) {}
  DEV_HOST Vertex(const VID_T& vid, const EmptyType&)
      : vid(vid),min_iter(0), secret(0) {}
  DEV_HOST Vertex(const VID_T& vid, const EmptyType&,int32_t min_iter, int32_t secret)
      : vid(vid),min_iter(min_iter), secret(secret) {}
  DEV_HOST Vertex(const Vertex& vert) : vid(vert.vid),min_iter(vert.min_iter) , secret(vert.secret) {}

  DEV_HOST ~Vertex() {}

  DEV_HOST Vertex& operator=(const Vertex& rhs) {
    if (this == &rhs) {
      return *this;
    }
    vid = rhs.vid;
    secret = rhs.secret;
    min_iter = rhs.min_iter;
    return *this;
  }

  union {
    VID_T vid;
    EmptyType vdata;
  };
  int32_t min_iter;
  int32_t secret;
};

}  // namespace internal

template <typename VID_T, typename VDATA_T>
InArchive& operator<<(InArchive& archive,
                      const internal::Vertex<VID_T, VDATA_T>& v) {
  archive << v.vid << v.vdata;
  return archive;
}

template <typename VID_T, typename VDATA_T>
OutArchive& operator>>(OutArchive& archive,
                       internal::Vertex<VID_T, VDATA_T>& v) {
  archive >> v.vid >> v.vdata;
  return archive;
}

template <typename VID_T>
InArchive& operator<<(InArchive& archive,
                      const internal::Vertex<VID_T, EmptyType>& v) {
  archive << v.vid;
  return archive;
}

template <typename VID_T>
OutArchive& operator>>(OutArchive& archive,
                       internal::Vertex<VID_T, EmptyType>& v) {
  archive >> v.vid;
  return archive;
}

}  // namespace grape

#endif  // GRAPE_GRAPH_VERTEX_H_
