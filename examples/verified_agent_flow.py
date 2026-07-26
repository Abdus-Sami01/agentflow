from agentflow import SharedContext, WorkflowBuilder, workflow_to_text

TABLES = {"orders", "customers", "products"}
BLOCKED_SQL = ("drop", "delete", "truncate", "alter", "update", "insert")

ATTEMPTS = [
    "DROP TABLE orders",
    "SELECT * FROM secrets",
    "SELECT region, SUM(total) FROM orders GROUP BY region",
]


def propose_query(inputs, context, violations):
    idx = min(len(violations), len(ATTEMPTS) - 1)
    return ATTEMPTS[idx]


def verify_query(sql, context):
    lowered = sql.lower()

    for word in BLOCKED_SQL:
        if lowered.startswith(word) or f" {word} " in lowered:
            return False, f"destructive statement {word.upper()!r} is not permitted"

    if not lowered.startswith("select"):
        return False, "only SELECT statements are allowed"

    referenced = {t for t in TABLES if t in lowered}
    if not referenced:
        return False, f"query must reference a known table: {sorted(TABLES)}"

    return True, ""


def run_query(sql, context):
    return [{"region": "north", "total": 5200}, {"region": "south", "total": 3100}]


def main():
    wf = (
        WorkflowBuilder("verified-sql")
        .agent("author_sql", propose_query, verify_query, execute_fn=run_query, max_attempts=5)
        .transform("format", lambda i: "\n".join(
            f"{r['region']}: {r['total']}" for r in next(iter(i.values()))
        ))
        .edge("author_sql", "format")
        .terminal("format")
    )

    result = wf.run()
    print(workflow_to_text(result))

    meta = result.results["author_sql"].output.metadata
    print(f"Attempts: {meta['attempts']}, rejected: {meta['rejections']}")
    for v in meta["violations"]:
        print(f"  rejected: {v}")
    print()
    print(result.final_output)


if __name__ == "__main__":
    main()
