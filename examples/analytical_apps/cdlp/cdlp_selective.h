//
// Created by Yufei on 2024/7/19.
//

#ifndef LIBGRAPE_LITE_CDLP_SELECTIVE_H
#define LIBGRAPE_LITE_CDLP_SELECTIVE_H


#include <grape/grape.h>
#include <set>

#include <iostream>  //wuyufei
#include "cdlp/cdlp_selective_context.h"
#include "cdlp/cdlp_utils.h"

#include "TEE_connection.h"

namespace grape {

/**
 * @brief An implementation of CDLP(Community detection using label
 * propagation), the version in LDBC, which only works on the undirected graph.
 *
 * This version of CDLP inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class CDLPSelective : public ParallelAppBase<FRAG_T, CDLPSelectiveContext<FRAG_T>>,
             public ParallelEngine {
  INSTALL_PARALLEL_WORKER(CDLPSelective<FRAG_T>, CDLPSelectiveContext<FRAG_T>, FRAG_T)

 private:
  using label_t = typename context_t::label_t;
  using vid_t = typename context_t::vid_t;

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
    std::cout << "current label\n";
    for(auto v : inner_vertices ){
      std::cout << "v" << frag.GetId(v) << " : " << ctx.labels[v] << std::endl;
    }
    std::cout << "valid label count :" << ctx.verticesWithValidLabel.Count() << std::endl;
  }

  void PropagateLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages) {
    std::cout << "PropagateLabel" << std::endl;
#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    auto inner_vertices = frag.InnerVertices();
    typename FRAG_T::template inner_vertex_array_t<label_t> new_ilabels;
    new_ilabels.Init(inner_vertices);

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // touch neighbor and send messages in parallel
    ForEach(inner_vertices,
            [&frag, &ctx, &new_ilabels, &messages](int tid, vertex_t v) {
              auto es = frag.GetOutgoingAdjList(v);
              if (es.Empty()) {
                ctx.changed[v] = false;
              } else {
                label_t new_label =
                    update_label_fast_selected<label_t, context_t, vid_t,
                                               fragment_t>(
                        es, ctx.labels, ctx.labels[v], ctx, frag);//wuyufei

                if (ctx.labels[v] != new_label) {
                  std::cout << "Change v" << frag.GetId(v) << " " << ctx.labels[v] << " -> " << new_label << std::endl;
                  new_ilabels[v] = new_label;
                  ctx.changed[v] = true;
                  messages.SendMsgThroughOEdges<fragment_t, label_t>(
                      frag, v, new_label, tid);
                } else {
                  ctx.changed[v] = false;
                }
              }
            });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    for (auto v : inner_vertices) {
      if (ctx.changed[v]) {
        ctx.labels[v] = new_ilabels[v];
      }
    }

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
    std::cout << "============ PEval ================\n";
    
    TEE_connection conn;
    conn.increse();

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num());

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }

#ifdef GID_AS_LABEL
    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.labels[v] = frag.GetInnerVertexGid(v);
    });
    ForEach(outer_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.labels[v] = frag.GetOuterVertexGid(v);
    });
#else
    ctx.verticesWithValidLabel.ParallelClear(GetThreadPool());
    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      if (frag.GetData(v) == 1){//标签过滤逻辑
        ctx.verticesWithValidLabel.Insert(v);
      }
      ctx.labels[v] = frag.GetInnerVertexId(v);
    });
    ForEach(outer_vertices, [&frag, &ctx](int tid, vertex_t v) {
      if (frag.GetData(v) == 1){
        ctx.verticesWithValidLabel.Insert(v);
      }
      ctx.labels[v] = frag.GetOuterVertexId(v);
    });
#endif
    printLabel(frag, ctx, messages);//wuyufei
    PropagateLabel(frag, ctx, messages);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    std::cout << "=============== IncEval round "<< ctx.step << " ==================\n";
    ++ctx.step;

#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif

    // receive messages and set labels
    {
      messages.ParallelProcess<fragment_t, label_t>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, const label_t& msg) {
            ctx.labels[u] = msg;
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


#endif  // LIBGRAPE_LITE_CDLP_SELECTIVE_H



