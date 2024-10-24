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

#ifndef EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_CONTEXT_H_

#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <vector>
#include <string>

#include <grape/grape.h>

namespace grape {

/**
 * @brief Context for the parallel version of DrugRecommendation.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class DrugRecommendationContext : public VertexDataContext<FRAG_T, typename FRAG_T::oid_t> {

public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;


  using label_t = oid_t;
  explicit DrugRecommendationContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, typename FRAG_T::oid_t>(fragment, true),
        labels(this->data()) {
  }

  void Init(ParallelMessageManager& messages, int patientId ) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    Vertex<vid_t> patientVertex;
    frag.GetVertex(patientId, patientVertex);
    active.Init(inner_vertices);
    active[patientVertex] = true;
    step = 0;

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << labels[v] << std::endl;
    }
    ostream.close();//
  }


  typename FRAG_T::template vertex_array_t<label_t>& labels;
  typename FRAG_T::template inner_vertex_array_t<bool> active;
  std::ofstream ostream;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

  int step = 0;
  int max_round = 4;

};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_CONTEXT_H_
