"""Exercise file SQL against a disposable seekdb instance.

Pass a writable directory permitted by secure_file_priv. The fixture creates
its own temporary subdirectory and runs as the instance's root test user.
"""

import argparse
from pathlib import Path
import subprocess
import tempfile


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def run(client, port, directory):
    args = [client, "-h127.0.0.1", f"-P{port}", "-uroot", "-Dtest", "-N", "-B", "--raw"]

    def sql(statement, *, enabled=True, error=None):
        prefix = "SET SESSION enable_file_sql=ON; " if enabled else ""
        result = subprocess.run(
            [*args, "-e", prefix + statement], capture_output=True, text=True, timeout=30,
        )
        if error is not None:
            assert result.returncode != 0 and error in result.stderr, result
        else:
            assert result.returncode == 0, result.stderr
        return result.stdout.rstrip("\n")

    with tempfile.TemporaryDirectory(dir=directory, prefix="file-sql-regression-") as path:
        root = Path(path)
        csv = root / "left.csv"
        jsonl = root / "right.jsonl"
        csv.write_bytes('id;note\r\n1;"alpha;first"\r\n2;"第二行"\r\n3;NULL\r\n'.encode())
        jsonl.write_text('{"id":1,"group_name":"x"}\n{"id":3,"group_name":"y"}\n', encoding="utf-8")
        left = f"read_csv({literal(csv)}, delim=';')"
        right = f"read_ndjson({literal(jsonl)})"
        sql(f"SELECT COUNT(*) FROM {left}", enabled=False, error="enable_file_sql")
        assert sql(f"SELECT a.id,b.group_name FROM {left} a JOIN {right} b ON a.id=b.id ORDER BY a.id") == "1\tx\n3\ty"
        assert sql(f"SELECT a.id,b.group_name FROM {left} a LEFT JOIN {right} b ON a.id=b.id ORDER BY a.id") == "1\tx\n2\tNULL\n3\ty"
        assert sql(f"SELECT /*+ LEADING(a b) USE_NL(b) */ a.id,b.group_name FROM {left} a "
                   f"JOIN {right} b ON a.id=b.id ORDER BY a.id") == "1\tx\n3\ty"
        assert sql(f"SELECT COUNT(*),SUM(id),MIN(note) FROM {left} WHERE id>1 HAVING COUNT(*)=2") == "2\t5\t第二行"

        # Repeated reads must bind the current schema and preserve type boundaries.
        types = root / "types.csv"
        types.write_text("limit_int,wide,empty,d\n9223372036854775807,1,,2024-02-29\n"
                         "-9223372036854775808,2.5,,2024-03-01\n", encoding="utf-8")
        assert sql(f"SELECT MIN(limit_int),MAX(limit_int),SUM(wide),COUNT(empty) FROM read_csv({literal(types)})") == (
            "-9223372036854775808\t9223372036854775807\t3.5\t0"
        )
        types.write_text("changed\n11\n", encoding="utf-8")
        assert sql(f"SELECT * FROM read_csv({literal(types)})") == "11"

        malformed = root / "malformed.csv"
        malformed.write_text('id,name\n1,"unclosed\n', encoding="utf-8")
        sql(f"SELECT * FROM read_csv({literal(malformed)})", error="unterminated CSV quote")
        nested = root / "nested.jsonl"
        nested.write_text('{"a":{"b":1}}\n', encoding="utf-8")
        sql(f"SELECT * FROM read_ndjson({literal(nested)})", error="flat JSON object")
        oversized = root / "large.csv"
        oversized.write_text("value\n" + "x" * (1024 * 1024 + 10), encoding="utf-8")
        sql(f"SELECT COUNT(*) FROM read_csv({literal(oversized)})", error="exceeds 1 MiB")
        sql(f"SELECT * FROM read_csv({literal(root / '..' / 'outside.csv')})", error="parent path")

        alias = root / "link.csv"
        alias.symlink_to(csv)
        sql(f"SELECT * FROM read_csv({literal(alias)})", error="ERROR 1235")
        assert sql(f"SELECT COUNT(*) FROM glob({literal(root / 'link*')})") == "0"

        output = root / "export.csv"
        copy = f"COPY (SELECT id,note FROM {left} ORDER BY id) TO {literal(output)} (FORMAT csv, HEADER, DELIMITER ';', NULLSTR 'NA')"
        sql(copy)
        assert sql(f"SELECT id,note FROM read_csv({literal(output)}, delim=';', nullstr='NA') ORDER BY id") == (
            "1\talpha;first\n2\t第二行\n3\tNULL"
        )
        before = output.read_bytes()
        sql(copy, error="target already exists")
        assert output.read_bytes() == before
        failed = root / "failed.csv"
        sql(f"COPY (SELECT JSON_EXTRACT(note,'$.x') FROM {left}) TO {literal(failed)}", error="ERROR")
        assert not failed.exists()

        batch = root / "batch.csv"
        batch.write_text("id,bucket\n" + "".join(f"{i},{i % 3}\n" for i in range(3000)), encoding="utf-8")
        assert sql(f"SELECT /*+ OPT_PARAM('rowsets_max_rows', 64) */ bucket,COUNT(*),SUM(id) "
                   f"FROM read_csv({literal(batch)}) GROUP BY bucket ORDER BY bucket") == (
            "0\t1000\t1498500\n1\t1000\t1499500\n2\t1000\t1500500"
        )

        exported_table = root / "exported_table.csv"
        sql("CREATE TABLE file_sql_export_fixture(n INT); "
            "INSERT INTO file_sql_export_fixture VALUES (17); "
            f"COPY file_sql_export_fixture TO {literal(exported_table)} (HEADER); "
            "DROP TABLE file_sql_export_fixture")
        assert sql(f"SELECT n FROM read_csv({literal(exported_table)})") == "17"

    print("File SQL integration checks passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    options = parser.parse_args()
    run(options.client, options.port, options.directory)
