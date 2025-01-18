//
// Created by Yufei on 2024/7/19.
//

#ifndef LIBGRAPE_LITE_JACCARD_H
#define LIBGRAPE_LITE_JACCARD_H


#include <grape/grape.h>
#include <set>

#include "jaccard/jaccard_context.h"
#include "cdlp/cdlp_utils.h"

#include "TEE/TEE_connection.h"

namespace grape {

/**
 * @brief An implementation of  CDLP(Community detection using label
 * propagation), the version in LDBC, which only works on the undirected graph.
 *
 * This version of CDLP inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class Jaccard : public ParallelAppBase<FRAG_T, JaccardContext<FRAG_T>>,
             public ParallelEngine {
  INSTALL_PARALLEL_WORKER(Jaccard<FRAG_T>, JaccardContext<FRAG_T>, FRAG_T)

 private:
  using label_t = typename context_t::label_t;
  using vid_t = typename context_t::vid_t;

  void printSet(context_t& ctx, std::set<label_t>& set) {
    for (auto l : set) {
      ctx.ostream << l << " ";
    }
  }

  void printLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages){
    auto inner_vertices = frag.InnerVertices();
    ctx.ostream << "current label\n";
    for(auto v : inner_vertices ){
      ctx.ostream << "v" << frag.GetId(v) << " : ";
      printSet(ctx,ctx.labels[v]);
      ctx.ostream << std::endl;
    }
  }

  void PropagateLabel(const fragment_t& frag, context_t& ctx,
                      message_manager_t& messages) {
    ctx.ostream << "PropagateLabel" << std::endl;

    auto& channels = messages.Channels();
    // auto inner_vertices = frag.InnerVertices();


    ctx.next_modified.ParallelClear(GetThreadPool());
    ForEach(ctx.curr_modified,
            [&frag, &ctx, &channels](int tid, vertex_t v) {
              ctx.ostream << "v" << frag.GetId(v) << " sending to ";
              auto es = frag.GetOutgoingAdjList(v);
              for (auto& e : es) {
                vertex_t u = e.get_neighbor();
                ctx.ostream << "v" << frag.GetId(u) << " ";
                if (frag.IsOuterVertex(u)) {
                  // put the message to the channel.
                  channels[tid].SyncStateOnOuterVertex<fragment_t, std::set<label_t>>(
                        frag, u, ctx.labels[v]);
                } else {
                  auto origin_size = ctx.labels[u].size();
                  ctx.labels[u].insert(ctx.labels[v].begin(), ctx.labels[v].end());
                  if (origin_size < ctx.labels[u].size()) {
                    ctx.next_modified.Insert(u);
                  }
                }
              }
              ctx.ostream << std::endl;
            });
    ctx.next_modified.Swap(ctx.curr_modified);
  }

 public:
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    ctx.ostream.open("log" + std::to_string(frag.fid()) + ".txt");
    auto inner_vertices = frag.InnerVertices();
    // auto outer_vertices = frag.OuterVertices();

    messages.InitChannels(thread_num());

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }
    ctx.ostream << "============ PEval round "<< ctx.step <<"================\n";


    ctx.curr_modified.ParallelClear(GetThreadPool());
    ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t v) {
      ctx.ostream << "v" << frag.GetId(v) << ": " << frag.GetData(v) << " p" << frag.GetSecret(v) << std::endl;
      if (frag.GetData(v) == 1){//标签过滤逻辑
        // ctx.verticesWithValidLabel.Insert(v);
        ctx.labels[v].insert(frag.GetInnerVertexId(v));
        ctx.curr_modified.Insert(v);
      }
    });

    printLabel(frag, ctx, messages);//wuyufei
    PropagateLabel(frag, ctx, messages);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    {
      messages.ParallelProcess<fragment_t, std::set<label_t>>(
          thread_num(), frag, [&ctx, &frag](int tid, vertex_t u, const std::set<label_t>& msg) {
            ctx.ostream << "v" << frag.GetId(u) << " receiving msg ";
            for (auto l : msg) {
              ctx.ostream << l << " ";
            }
            ctx.ostream << std::endl;
            auto origin_size = ctx.labels[u].size();
            ctx.labels[u].insert(msg.begin(), msg.end());
            if(ctx.labels[u].size() > origin_size)
              ctx.curr_modified.Insert(u);
          });
    }


    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    } else {
      messages.ForceContinue();
    }
    ctx.ostream << "=============== IncEval round "<< ctx.step << " ==================\n";
    printLabel(frag, ctx, messages);//wuyufei
    PropagateLabel(frag, ctx, messages);
  }
};
}  // namespace grape


#endif  // LIBGRAPE_LITE_JACCARD_H



