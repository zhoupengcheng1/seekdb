# AI function regression tests

AI expressions execute inside the SQL engine. The integration fixture uses a
local deterministic HTTP service; it does not require model access or API keys.

Build seekdb using the normal [build workflow](build-and-run.md). Start a
dedicated disposable instance with an unused port:

```bash
python3 tools/deploy/sdb.py start \
  --binary "$PWD/build/bin/src/observer/seekdb" \
  --base-dir "$PWD/build/ai-test-instance" --port 12884 \
  --parameter cpu_count=2 --parameter memory_budget=2G \
  --parameter datafile_size=2G --parameter log_disk_size=2G
python3 tools/deploy/sdb.py wait-ready \
  --client "$PWD/deps/3rd/u01/obclient/bin/obclient" \
  --base-dir "$PWD/build/ai-test-instance" \
  --host 127.0.0.1 --port 12884 --user root
python3 unittest/sql/engine/expr/test_ai_function_integration.py \
  --client "$PWD/deps/3rd/u01/obclient/bin/obclient" --port 12884
python3 tools/deploy/sdb.py destroy --base-dir "$PWD/build/ai-test-instance"
```

The fixture creates temporary `enh_*` AI models, endpoints and tables. Use a
fresh disposable instance. The mock listens on an automatically allocated
loopback port and shuts down after the tests. Seven test groups cover:

- JSON object/array prompt arguments, nested prompts, repeated placeholders,
  invalid/overflowing indices, Unicode, escaped characters and embedded NUL.
- TEXT/LONGTEXT templates and arguments larger than VARCHAR limits.
- Two- and three-argument completion, SQL NULL configuration, JSON options,
  long input, empty/non-string responses, malformed JSON and HTTP failures.
- Default/explicit embedding dimensions, finite numeric vector elements,
  malformed vectors, count validation and indexed response reordering.
- Case-insensitive Ollama endpoint creation/alteration, empty access keys,
  non-streaming generation and native embedding arrays.
- Rerank strings and objects across 20-document batches, full index
  permutations, stable score ties, preservation of IDs and other fields,
  and rejection of invalid counts, scores and indices.
- Real cross-row embedding requests, per-row model/dimension grouping,
  64-input request limits, NULL propagation, filters, LIMIT, multiple batches,
  and recovery after a failed later batch.

The fixture asserts that multiple SQL rows are sent in one HTTP request.
OpenAI mock embedding responses are deliberately reversed. Expected results
are computed from the original input rows, independently of response order.

Existing SQL regressions can also be run with
`tools/deploy/mysqltest.py run --test-set ai_function.ai_prompt,ai_function.ai_model_endpoint_ddl,ai_function.ai_model_ddl`.
The updated `ai_prompt` expected output reflects its newly supported JSON and
LOB inputs; numeric and NULL template arguments still fail type checking.

Provider protocol references:
[Ollama generate](https://docs.ollama.com/api/generate) and
[Ollama embed](https://docs.ollama.com/api/embed).
