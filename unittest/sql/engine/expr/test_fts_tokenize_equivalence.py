"""Compare complete TOKENIZE outputs from reference and candidate instances.

Both instances must use the same settings and dictionary. Run with --client,
--reference-port and --port. No benchmark data, timer or scoring code is changed.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import random
import subprocess


def statements():
    rng = random.Random(73129)
    alphabet = "数据库全文检索一二三十百千万abcXYZ019 １２３，。！？-_@.日本語한국어éßΩЖ😀𠀀"
    texts = ["", " ", "a", "，。！", "Alpha ALPHA alpha", "数据库数据库",
             "数据库 abc １２３，😀" * 300]
    texts += ["".join(rng.choice(alphabet) for _ in range(rng.randrange(1, 180)))
              for _ in range(160)]
    for text in texts:
        literal = "CONVERT(X'" + text.encode().hex() + "' USING utf8mb4)"
        for charset in ("utf8mb4", "utf16", "utf16le"):
            value = f"CONVERT({literal} USING {charset})"
            for parser in ("ik", "beng", "space", "ngram"):
                yield f"SELECT TOKENIZE({value}, '{parser}', '[{{\"output\":\"all\"}}]');"


def run(client, port, sql):
    result = subprocess.run(
        [client, "-h127.0.0.1", f"-P{port}", "-uroot", "-N", "-B", "--raw",
         "--default-character-set=utf8mb4", "--force"],
        input=sql, text=True, encoding="utf-8", capture_output=True, timeout=180,
    )
    return result.stdout, result.stderr


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--reference-port", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    cases = list(statements())
    sql = "SET NAMES utf8mb4;\n" + "\n".join(cases) + "\n"
    before = run(args.client, args.reference_port, sql)
    assert before[0], "Reference instance produced no results"
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run, args.client, args.port, sql) for _ in range(args.workers)]
        for future in futures:
            assert before == future.result(), "TOKENIZE results or error diagnostics differ"
    print(f"{len(cases)} TOKENIZE cases match across {args.workers} concurrent clients, "
          "including complete frequencies and errors")
