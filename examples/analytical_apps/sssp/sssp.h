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

#ifndef EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_
#define EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_

#include <grape/grape.h>

#include "sssp/sssp_context.h"

namespace grape {

/**
 * @brief SSSP application, determines the length of the shortest paths from a
 * given source vertex to all other vertices in graphs, which can work
 * on both directed and undirected graph.
 *
 * This version of SSSP inherits ParallelAppBase. Messages can be sent in
 * parallel with the evaluation process. This strategy improves the performance
 * by overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSP : public ParallelAppBase<FRAG_T, SSSPContext<FRAG_T>>,
             public ParallelEngine {

 public:
  // specialize the templated worker.
  INSTALL_PARALLEL_WORKER(SSSP<FRAG_T>, SSSPContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  /**
   * @brief Partial evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   * @param messages
   */
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    ctx.ostream.open("log" + std::to_string(frag.fid()) + ".txt");
    messages.InitChannels(thread_num());

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.next_modified.ParallelClear(GetThreadPool());

    // Get the channel. Messages assigned to this channel will be sent by the
    // message manager in parallel with the evaluation process.
    // auto& channel_0 = messages.Channels()[0];
    if (native_source) {
      ctx.ostream << "v" << frag.GetId(source) << ": " << frag.GetData(source) << " p" << frag.GetSecret(source) << std::endl;
      //private_count
      if (frag.GetSecret(source)) {
        ++ctx.private_count;
        ctx.ostream << 1 << std::endl;
      }else {
        ctx.ostream << 0 << std::endl;
      }

      ctx.partial_result[source] = 0;
      auto es = frag.GetOutgoingAdjList(source);
      for (auto& e : es) {
        vertex_t v = e.get_neighbor();
        ctx.ostream << "out v" << frag.GetId(v) << ": " << ctx.partial_result[v] << std::endl;
        if(frag.GetSecret(v)) {
          ctx.private_potential_result.insert_or_update_min(frag.GetId(v), static_cast<double>(e.get_data()));
        }else {
          ctx.partial_result[v] = std::min(ctx.partial_result[v], static_cast<double>(e.get_data()));
          // if (frag.IsOuterVertex(v)) {
          //   // put the message to the channel.
          //   channel_0.SyncStateOnOuterVertex<fragment_t, double>(
          //       frag, v, ctx.partial_result[v]);
          // } else {
          //   ctx.next_modified.Insert(v);
          // }
          ctx.next_modified.Insert(v);
        }

      }
      processPrivate(frag, ctx, messages);
    }
    ctx.ostream << "sending msg" << std::endl;
    auto outer_vertices = frag.OuterVertices();
    ForEach(ctx.next_modified, outer_vertices,
            [&messages, &frag, &ctx](int tid, vertex_t v) {
              ctx.ostream << "sending v" << frag.GetId(v) << ": " << ctx.partial_result[v] << std::endl;
              messages.Channels()[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    messages.ForceContinue();

    ctx.next_modified.Swap(ctx.curr_modified);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  /**
   * @brief Incremental evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   * @param messages
   */
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ctx.ostream << "==================== IncEval ====================" << std::endl;
    auto inner_vertices = frag.InnerVertices();

    auto& channels = messages.Channels();

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    ctx.next_modified.ParallelClear(GetThreadPool());

    // parallel process and reduce the received messages
    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, double msg) {
          if (ctx.partial_result[u] > msg) {
            atomic_min(ctx.partial_result[u], msg);
            ctx.curr_modified.Insert(u);
          }
        });
    ctx.ostream << "msg receive" << std::endl;

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // incremental evaluation.
    ctx.private_count_iter = 0;
    ForEach(ctx.curr_modified, inner_vertices,
            [&frag, &ctx](int tid, vertex_t v) {
              double distv = ctx.partial_result[v];
              ctx.ostream << "v" << frag.GetId(v) << ": " << ctx.partial_result[v]<< " p" << frag.GetSecret(v) << std::endl;
              if (frag.GetSecret(v)) {
                ++ctx.private_count;
                ++ctx.private_count_iter;
                // ctx.ostream << "counting v" << frag.GetId(v) << std::endl;
              }
              auto es = frag.GetOutgoingAdjList(v);
              for (auto& e : es) {
                vertex_t u = e.get_neighbor();
                double ndistu = distv + e.get_data();
                if (frag.GetSecret(u)){
                  // if (ndistu == conn->min(ndistu, ctx.partial_result[u])) {
                  //   atomic_min(ctx.partial_result[u], ndistu);
                  //   ctx.next_modified.Insert(u);
                  // }
                  ctx.private_potential_result.insert_or_update_min(frag.GetId(u), ndistu);
                } else {
                  if (ndistu < ctx.partial_result[u]) {
                    atomic_min(ctx.partial_result[u], ndistu);
                    ctx.next_modified.Insert(u);
                  }
                }
              }
            });
    processPrivate(frag, ctx, messages);

    // ctx.ostream << "iter_count: " << ctx.private_count_iter << std::endl;
    ctx.ostream << ctx.private_count_iter << std::endl;
    // put messages into channels corresponding to the destination fragments.

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif
    auto outer_vertices = frag.OuterVertices();
    ForEach(ctx.next_modified, outer_vertices,
            [&channels, &frag, &ctx](int tid, vertex_t v) {
              channels[tid].SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.partial_result[v]);
            });

    if (!ctx.next_modified.PartialEmpty(
            frag.Vertices().begin_value(),
            frag.Vertices().begin_value() + frag.GetInnerVerticesNum())) {
      messages.ForceContinue();
    }

    ctx.next_modified.Swap(ctx.curr_modified);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
private:
  void processPrivate(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    ctx.ostream << "===processPrivate===" << std::endl;
    auto conn = ctx.connection_pool.acquire();

    //compare and get active
    conn->resetSharedMemory();
    ctx.ostream << "resetSharedMemory" << std::endl;
    uint64_t index = 0;
    uint64_t size = 0;
    auto current = static_cast<double*>(conn->sssp_get_current_buffer(size));
    auto target = static_cast<double*>(conn->sssp_get_target_buffer(size));
    ctx.ostream << "getBuffer" << std::endl;
    for (auto vid : ctx.private_potential_result.keys()) {
      vertex_t v;
      frag.GetVertex(vid, v);
      ctx.ostream << "process current v" << vid << ": " << ctx.partial_result[v] << std::endl;
      current[index] = ctx.partial_result[v];

      ctx.ostream << "process target v" << vid << ": " << ctx.private_potential_result.get(vid).value() << std::endl;
      target[index] = ctx.private_potential_result.get(vid).value();
      index ++;

    }
    ctx.ostream << "write data" << std::endl;
    conn->sssp_compare();
    ctx.ostream << "compare" << std::endl;

    //send message
    index = 0;
    for (auto vid : ctx.private_potential_result.keys()) {
      vertex_t v;
      frag.GetVertex(vid, v);
      ctx.ostream << "editing v" << vid << ": " << ctx.partial_result[v] << std::endl;
      if (target[index] > 0) {
        ctx.ostream << "changed\n";
        ctx.partial_result[v] = current[index];
        ctx.next_modified.Insert(v);
      }
      index ++;
    }
    ctx.ostream << "modify partial_result" << std::endl;
    ctx.private_potential_result.clear();
    ctx.connection_pool.release(conn);
    //next_modify active list
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_H_
