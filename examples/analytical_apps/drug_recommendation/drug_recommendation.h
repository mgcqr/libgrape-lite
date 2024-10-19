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

#ifndef EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_H_
#define EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_H_

#include <grape/grape.h>

#include "drug_recommendation/drug_recommendation_context.h"

namespace grape {

/**
 * @brief DrugRecommendation application
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class DrugRecommendation : public ParallelAppBase<FRAG_T, DrugRecommendationContext<FRAG_T>>,
             public ParallelEngine {
  INSTALL_PARALLEL_WORKER(DrugRecommendation<FRAG_T>, DrugRecommendationContext<FRAG_T>, FRAG_T)

 private:
  using label_t = typename context_t::label_t;
  using vid_t = typename context_t::vid_t;
  std::vector<int> vertex_class_step;
  /**
   * wuyufei
   *
   *
   * @param frag
   * @param ctx
   * @param messages
   */
  void printLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages){
    auto inner_vertices = frag.InnerVertices();
    ctx.ostream << "current label\n";
    for(auto v : inner_vertices ){
      ctx.ostream << "v" << frag.GetId(v) << " : " << ctx.labels[v] << std::endl;
    }
    ctx.ostream << "active \n";
    for(auto v : inner_vertices ){
      ctx.ostream << "v" << frag.GetId(v) << " : " << ctx.active[v] << std::endl;
    }

  }


  void PropagateLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages) {
    ctx.ostream << "PropagateLabel" << std::endl;
#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    auto inner_vertices = frag.InnerVertices();

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // touch neighbor and send messages in parallel
    ForEach(inner_vertices,
            [&frag, &ctx, &messages, this](int tid, vertex_t v) {
              ctx.ostream << "=== printState ===\n";
              ctx.ostream << "current vertex:" << frag.GetId(v) << std::endl;
              if(frag.GetData(v) != vertex_class_step[ctx.step - 1]) {
                ctx.ostream << frag.GetData(v) << "!=" << vertex_class_step[ctx.step - 1] << std::endl;
                return;
              }

              if (ctx.active[v] == false) {
                ctx.ostream << "non active \n";
                return;
              }


              printLabel(frag, ctx, messages);


              auto& channel_0 = messages.Channels()[0];
              auto es = frag.GetOutgoingAdjList(v);
              if (ctx.step < 3) {
                for(auto e : es) {
                  vertex_t u = e.get_neighbor();
                  if (frag.IsInnerVertex(u)) {
                    ctx.active[e.get_neighbor()] = true;
                    ctx.labels[e.get_neighbor()] = 1;
                  } else if (frag.IsOuterVertex(u)) {
                    channel_0.SyncStateOnOuterVertex<fragment_t, label_t>(frag, u, 1);
                  }
                }
                // messages.SendMsgThroughOEdges<fragment_t, label_t>(
                //    frag, v, 1, tid);
              } else if (ctx.step == 3) {//step = 3
                for(auto e : es) {
                  ctx.active[e.get_neighbor()] = true;
                  vertex_t u = e.get_neighbor();
                  int weight = static_cast<int>(e.get_data());
                  if (frag.IsInnerVertex(u)) {
                    ctx.labels[u] += weight;
                  }else if (frag.IsOuterVertex(u)){
                    channel_0.SyncStateOnOuterVertex<fragment_t, label_t>(frag, u, weight);
                  }

                }
              } else if (ctx.step == 4) {

              }
              ctx.active[v] = false;


            });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif



#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

 public:
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_class_step.emplace_back(0);
    vertex_class_step.emplace_back(1);
    vertex_class_step.emplace_back(2);
    vertex_class_step.emplace_back(3);
    ctx.ostream.open("log" + std::to_string(frag.fid()));
    ctx.ostream << "============ PEval ================\n";
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num());

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }


    ForEach(inner_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      int vlabel = frag.GetData(v);
      int tar = vertex_class_step[ctx.step - 1];
      if (vlabel == tar){//标签过滤逻辑
        ctx.labels[v] = 1;
        ctx.active[v] = true;
      } else {
        ctx.labels[v] = 0;
      }
    });
    ForEach(outer_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      if (frag.GetData(v) == vertex_class_step[ctx.step - 1]){
        ctx.labels[v] = 1;
        ctx.active[v] = true;
      }else {
        ctx.labels[v] = 0;
      }
    });
    printLabel(frag, ctx, messages);//wuyufei
    PropagateLabel(frag, ctx, messages);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ++ctx.step;
    ctx.ostream << "=============== IncEval step:"<< ctx.step << " ==================\n";

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    // receive messages and set labels
    {
      messages.ParallelProcess<fragment_t, label_t>(
          thread_num(), frag, [&ctx, &frag, this](int tid, vertex_t u, const label_t& msg) {
            // ctx.labels[u] = msg;
            ctx.ostream << "message: " << frag.GetId(u) << " " << msg << std::endl;
            if (frag.GetData(u) != vertex_class_step[ctx.step - 1])
              return;
            ctx.active[u] = true;

            if (ctx.step < 4) {
              ctx.labels[u] = msg;
            }else if(ctx.step == 4) {
              ctx.labels[u] += msg;
            }
          });
    }

    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
#endif
    printLabel(frag, ctx, messages);//wuyufei
    PropagateLabel(frag, ctx, messages);
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_DRUG_RECOMMENDATION_H_
