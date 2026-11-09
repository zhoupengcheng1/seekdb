"""Compare full MATCH result sets and scores across two benchmark instances."""
import argparse
import subprocess


def queries():
    # Exercise both sides of the small-merge crossover and larger loser trees.
    words = ["database", "performance", "audit", "security", "training", "client",
             "search", "index", "query", "storage", "transaction", "memory",
             "service", "network", "system", "document"]
    for size in (1, 2, 3, 4, 7, 8, 9, 12, 16):
        text = " ".join(words[:size])
        match = f"MATCH(title_en, content_en) AGAINST('{text}')"
        for suffix in ("ORDER BY id", "ORDER BY score DESC,id LIMIT 31"):
            yield f"SELECT id,ROUND({match},12) score FROM fts_large_bench.docs WHERE {match} {suffix};"
    for text in ("全文索引 分词器", "OceanBase 稳定 database", "数据库 查询 优化 内存 索引 分词 检索 存储"):
        match = f"MATCH(title, content) AGAINST('{text}')"
        yield f"SELECT id,ROUND({match},12) FROM fts_large_bench.docs WHERE {match} ORDER BY id;"


def run(client, port, sql):
    return subprocess.check_output([client, "-h127.0.0.1", f"-P{port}", "-uroot", "-N", "-B",
                                    "--default-character-set=utf8mb4"], input=sql,
                                   text=True, encoding="utf-8", timeout=180)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--reference-port", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    cases = list(queries())
    sql = "SET NAMES utf8mb4;\n" + "\n".join(cases) + "\n"
    reference = run(args.client, args.reference_port, sql)
    candidate = run(args.client, args.port, sql)
    assert reference and reference == candidate, "MATCH row IDs, ranks or scores differ"
    print(f"{len(cases)} full result/ranking comparisons passed ({len(reference.splitlines())} rows)")
