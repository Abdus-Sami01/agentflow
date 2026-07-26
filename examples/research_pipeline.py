from agentflow import MetricsCollector, WorkflowBuilder, workflow_to_text, to_ascii

DOCS = {
    "doc1": "Transformers use self-attention. Attention scales quadratically with sequence length.",
    "doc2": "Sparse attention reduces cost to near-linear. FlashAttention optimizes memory access.",
    "doc3": "Quadratic cost limits context windows. Sparse patterns trade accuracy for speed.",
}


def fetch_docs():
    return list(DOCS.values())


def extract_claims(text):
    return [s.strip() for s in text.split(".") if s.strip()]


def score_claim(claim, context):
    keywords = {"attention", "sparse", "quadratic", "linear", "memory"}
    hits = sum(1 for k in keywords if k in claim.lower())
    return {"claim": claim, "score": hits}


def rank(inputs):
    scored = next(iter(inputs.values()))
    ranked = sorted(scored, key=lambda c: c["score"], reverse=True)
    return ranked[:5]


def summarize(inputs):
    top = next(iter(inputs.values()))
    lines = [f"{i+1}. [{c['score']}] {c['claim']}" for i, c in enumerate(top)]
    return "Top claims:\n" + "\n".join(lines)


def main():
    metrics = MetricsCollector()

    wf = (
        WorkflowBuilder("research")
        .config(max_parallel=4)
        .tool("fetch", fetch_docs)
        .transform("flatten", lambda i: [c for d in next(iter(i.values())) for c in extract_claims(d)])
        .foreach("score", score_claim, max_parallel=4)
        .transform("rank", rank)
        .transform("summary", summarize)
        .edge("fetch", "flatten")
        .edge("flatten", "score")
        .edge("score", "rank")
        .edge("rank", "summary")
        .terminal("summary")
        .with_hooks(metrics.as_hooks())
    )

    print(to_ascii(wf.dag))
    result = wf.run()
    print(workflow_to_text(result))
    print()
    print(result.final_output)


if __name__ == "__main__":
    main()
