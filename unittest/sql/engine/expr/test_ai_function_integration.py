#!/usr/bin/env python3
"""Exercise AI SQL expressions against a deterministic, local HTTP provider.

Run against a disposable seekdb instance, for example:
  python3 unittest/sql/engine/expr/test_ai_function_integration.py \
    --client deps/3rd/u01/obclient/bin/obclient --port 12884
The mock listens on a free loopback port; no model credentials are needed.
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import subprocess
import threading
import unittest


def quote(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def vector(text, dimension):
    seed = sum(text.encode('utf-8'))
    return [(seed + i) / 100.0 for i in range(dimension)]


def score(text):
    return float(sum(text.encode('utf-8')) % 7)


class MockProvider(BaseHTTPRequestHandler):
    requests = []
    lock = threading.Lock()

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.lock:
            self.requests.append((self.path, body, dict(self.headers)))
        if 'disconnect' in self.path:
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        result = None
        if 'complete' in self.path or 'generate' in self.path:
            prompt = body.get('prompt') if 'generate' in self.path else body['messages'][-1]['content']
            content = json.dumps({'prompt': prompt, 'temperature': body.get('temperature'),
                                  'max_tokens': body.get('max_tokens')}, ensure_ascii=False)
            if 'empty' in self.path:
                content = ''
            elif 'null' in self.path:
                content = None
            elif 'object' in self.path:
                content = {'invalid': True}
            result = {'response': content} if 'generate' in self.path else {
                'choices': [{'message': {'content': content}}]}
        elif 'embed' in self.path:
            inputs = body['input']
            if isinstance(inputs, str):
                inputs = [inputs]
            dimension = body.get('dimensions', 4)
            embeddings = [vector(text, dimension) for text in inputs]
            if 'dimension' in self.path:
                embeddings[0].append(0.0)
            elif 'string_element' in self.path:
                embeddings[0][0] = 'bad'
            elif 'null_element' in self.path:
                embeddings[0][0] = None
            elif 'boolean_element' in self.path:
                embeddings[0][0] = True
            elif 'empty_vector' in self.path:
                embeddings[0] = []
            elif 'scalar_vector' in self.path:
                embeddings[0] = 7
            if 'ollama' in self.path:
                result = {'embeddings': embeddings}
            else:
                data = [{'index': i, 'embedding': v} for i, v in enumerate(embeddings)]
                data.reverse()
                if 'duplicate' in self.path and len(data) > 1:
                    data[0]['index'] = data[1]['index']
                elif 'out_of_range' in self.path:
                    data[0]['index'] = len(inputs)
                elif 'missing_index' in self.path:
                    data[0].pop('index')
                elif 'count' in self.path:
                    data = data[:-1]
                elif 'bad_data' in self.path:
                    data = {'invalid': True}
                elif 'fail_later' in self.path and any(text == 'row-148' for text in inputs):
                    data.pop()
                result = {'data': data}
        elif 'rerank' in self.path:
            documents = body.get('documents', body.get('input', {}).get('documents'))
            results = [{'index': i, 'relevance_score': score(doc)} for i, doc in enumerate(documents)]
            results.reverse()  # Deliberately not sorted by score.
            if 'duplicate' in self.path and len(results) > 1:
                results[0]['index'] = results[1]['index']
            elif 'out_of_range' in self.path:
                results[0]['index'] = len(documents)
            elif 'count' in self.path:
                results.pop()
            elif 'score_type' in self.path:
                results[0]['relevance_score'] = 'bad'
            result = {'results': results}
        if 'array_root' in self.path:
            result = []
        status = 400 if 'http_error' in self.path else 200
        payload = json.dumps(result, ensure_ascii=False).encode('utf-8')
        if 'malformed_json' in self.path:
            payload = b'{incomplete'
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class AISQLTests(unittest.TestCase):
    client = None
    port = None
    mock_url = None

    @classmethod
    def sql(cls, text, expect_error=False):
        result = subprocess.run([cls.client, '-h127.0.0.1', '-P'+str(cls.port), '-uroot',
                                 '-Dtest', '--batch', '--raw', '--skip-column-names'],
                                input=text.encode('utf-8'), capture_output=True, timeout=90)
        if expect_error:
            if result.returncode == 0:
                raise AssertionError('Expected SQL error: '+text[:200])
        elif result.returncode:
            raise AssertionError(result.stderr.decode('utf-8', 'replace')+' SQL='+text[:200])
        return result.stdout.decode('utf-8').strip()

    @classmethod
    def configure(cls, name, provider, path, key='mock-only-key'):
        config = {'ai_model_name': name, 'provider': provider, 'url': cls.mock_url+path,
                  'access_key': key, 'request_model_name': 'wire-'+name}
        cls.sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('+quote(name+'_ep')+','+
                quote(json.dumps(config))+');')

    @classmethod
    def set_path(cls, name, path):
        cls.sql('CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('+quote(name+'_ep')+','+
                quote(json.dumps({'url': cls.mock_url+path}))+');')

    @classmethod
    def setUpClass(cls):
        for name, kind in [('enh_completion', 'completion'), ('enh_embedding', 'dense_embedding'),
                           ('enh_rerank', 'rerank'), ('enh_ollama_complete', 'completion'),
                           ('enh_ollama_embed', 'dense_embedding')]:
            cls.sql('CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('+quote(name)+','+
                    quote(json.dumps({'type': kind, 'model_name': 'model-'+name}))+');')
        cls.configure('enh_completion', 'openai', '/complete')
        cls.configure('enh_embedding', 'OPENAI', '/embed')
        cls.configure('enh_rerank', 'siliconflow', '/rerank')
        cls.configure('enh_ollama_complete', 'oLlAmA', '/ollama/generate', '')
        cls.configure('enh_ollama_embed', 'ollama', '/ollama/embed', '')

    @classmethod
    def tearDownClass(cls):
        for name in ['enh_completion','enh_embedding','enh_rerank','enh_ollama_complete','enh_ollama_embed']:
            cls.sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('+quote(name+'_ep')+');')
            cls.sql('CALL DBMS_AI_SERVICE.DROP_AI_MODEL('+quote(name)+');')

    def complete(self, expression, config='NULL', model='enh_completion'):
        encoded = self.sql('SELECT JSON_QUOTE(AI_COMPLETE('+quote(model)+','+expression+','+config+'));')
        return json.loads(json.loads(encoded))

    def test_01_prompt_json_nested_repeated_unicode(self):
        text = '中文\\"'
        prompt = "AI_PROMPT('{0}|{0}|{1}|{2}', "+quote(text)+", JSON_ARRAY(1,'二'), AI_PROMPT('nested:{0}', 'yes'))"
        got = self.complete(prompt)['prompt']
        self.assertEqual(got, text+'|'+text+'|[1, "二"]|nested:yes')
        self.assertEqual(self.complete("AI_PROMPT('{0}', CONCAT('a',CHAR(0),'b',CHAR(10)))")['prompt'], 'a\0b\n')
        got = json.loads(self.sql("SELECT AI_PROMPT('{0}', JSON_OBJECT('k', JSON_ARRAY(1,2)));"))
        self.assertEqual(got['args'], [{'k': [1,2]}])
        repeated = '{0}'*200
        self.assertEqual(self.complete('AI_PROMPT('+quote(repeated)+", 'long argument')")['prompt'],
                         'long argument'*200)
        for placeholder in ['{999999999999999999999999999999999}', '{1}', '{-1}', '{0']:
            self.sql("SELECT AI_COMPLETE('enh_completion', AI_PROMPT("+quote(placeholder)+", 'x'));", True)

    def test_02_prompt_text_lob(self):
        self.sql('CREATE TABLE enh_prompt_lob(t LONGTEXT, a LONGTEXT, b TEXT);')
        self.sql("INSERT INTO enh_prompt_lob VALUES ('value:{0}|{1}', REPEAT('文', 30000), REPEAT('本', 10000));")
        got = json.loads(json.loads(self.sql("SELECT JSON_QUOTE(AI_COMPLETE('enh_completion', AI_PROMPT(t,a,b))) FROM enh_prompt_lob;")))
        self.assertEqual(got['prompt'], 'value:'+'文'*30000+'|'+'本'*10000)
        self.sql('DROP TABLE enh_prompt_lob;')

    def test_03_completion_config_and_invalid_response(self):
        self.assertEqual(self.complete(quote('plain'))['prompt'], 'plain')
        self.assertEqual(json.loads(json.loads(self.sql("SELECT JSON_QUOTE(AI_COMPLETE('enh_completion','two arguments'));")))['prompt'], 'two arguments')
        got = self.complete(quote('configuration'), "JSON_OBJECT('temperature',0.25,'max_tokens',23)")
        self.assertEqual((got['temperature'],got['max_tokens']), (0.25,23))
        long_prompt = '内容\\"\n'*20000
        self.assertEqual(self.complete(quote(long_prompt))['prompt'], long_prompt)
        for config in ["JSON_ARRAY(1)", "'not json'", '42']:
            self.sql("SELECT AI_COMPLETE('enh_completion','x',"+config+');', True)
        for path in ['/complete/empty','/complete/null','/complete/object','/complete/array_root','/complete/http_error','/complete/malformed_json']:
            self.set_path('enh_completion',path)
            self.sql("SELECT AI_COMPLETE('enh_completion','x');",True)
        self.set_path('enh_completion','/complete')

    def test_04_embedding_scalar_dimensions_and_errors(self):
        self.assertEqual(json.loads(self.sql("SELECT AI_EMBED('enh_embedding','hello');")), vector('hello',4))
        self.assertEqual(json.loads(self.sql("SELECT AI_EMBED('enh_embedding','你好',7);")), vector('你好',7))
        self.assertEqual(self.sql("SELECT AI_EMBED('enh_embedding',NULL) IS NULL;"),'1')
        for dim in ['0','-1','1.5',"'3'",'NULL']:
            self.sql("SELECT AI_EMBED('enh_embedding','x',"+dim+');',True)
        for kind in ['dimension','string_element','null_element','boolean_element','empty_vector',
                     'scalar_vector','out_of_range','missing_index','count','bad_data','array_root','http_error','disconnect']:
            self.set_path('enh_embedding','/embed/'+kind)
            self.sql("SELECT AI_EMBED('enh_embedding','x',4);",True)
        self.set_path('enh_embedding','/embed')

    def test_05_ollama_protocol_and_endpoint_alter(self):
        self.assertEqual(self.complete(quote('native'),model='enh_ollama_complete')['prompt'],'native')
        self.complete(quote('options'),"JSON_OBJECT('temperature',0.5,'max_tokens',9,'stream',TRUE)",model='enh_ollama_complete')
        self.assertEqual(json.loads(self.sql("SELECT AI_EMBED('enh_ollama_embed','native',3);")),vector('native',3))
        requests = [r for r in MockProvider.requests if '/ollama/' in r[0]]
        self.assertTrue(any(r[0].endswith('/generate') and r[1].get('stream') is False for r in requests))
        self.assertTrue(any(r[1].get('options') == {'temperature':0.5,'num_predict':9} and r[1].get('stream') is False for r in requests))
        self.assertTrue(all('Authorization' not in r[2] for r in requests))
        self.assertTrue(all(r[1]['model'].startswith('wire-enh_') for r in MockProvider.requests))
        self.sql("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('enh_ollama_complete_ep',"+
                 quote(json.dumps({'provider':'OPENAI','access_key':'mock-only-key','url':self.mock_url+'/complete'}))+');')
        self.assertEqual(self.complete(quote('changed'),model='enh_ollama_complete')['prompt'],'changed')
        self.sql("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('enh_ollama_complete_ep',"+
                 quote(json.dumps({'provider':'oLLAma','access_key':'','url':self.mock_url+'/ollama/generate'}))+');')
        self.assertEqual(self.sql("SELECT COUNT(*) FROM oceanbase.DBA_OB_AI_MODEL_ENDPOINTS WHERE ENDPOINT_NAME='enh_ollama_complete_ep' AND PROVIDER='oLLAma';"),'1')

    def test_06_rerank_strings_objects_batches_ties(self):
        documents = ['document-'+str(i) for i in range(53)]
        expected = sorted(range(len(documents)),key=lambda i:(-score(documents[i]),i))
        got = json.loads(self.sql("SELECT AI_RERANK('enh_rerank','query',"+quote(json.dumps(documents))+');'))
        self.assertEqual([r['index'] for r in got], expected)
        objects = [{'id':'id-'+str(i),'body':d,'metadata':{'keep':i}} for i,d in enumerate(documents)]
        got = json.loads(self.sql("SELECT AI_RERANK('enh_rerank','query',"+quote(json.dumps(objects))+",'body');"))
        self.assertEqual([r['id'] for r in got], ['id-'+str(i) for i in expected])
        self.assertEqual([r['metadata']['keep'] for r in got], expected)
        for path in ['duplicate','out_of_range','count','score_type','array_root','http_error']:
            self.set_path('enh_rerank','/rerank/'+path)
            self.sql("SELECT AI_RERANK('enh_rerank','q','[\"a\",\"b\"]');",True)
        self.set_path('enh_rerank','/rerank')
        for docs in ['[]','[1]','[null]','[{"other":"x"}]']:
            self.sql("SELECT AI_RERANK('enh_rerank','q',"+quote(docs)+",'body');",True)

    def test_07_embedding_cross_row_batches_null_filter_limit(self):
        self.sql('CREATE TABLE enh_embed_rows(id INT PRIMARY KEY, content TEXT, dim INT);')
        values = []
        for i in range(150):
            text = 'NULL' if i % 13 == 0 else quote('row-'+str(i))
            values.append('('+str(i)+','+text+','+str(3+i%2)+')')
        self.sql('INSERT INTO enh_embed_rows VALUES '+','.join(values)+';')
        start = len(MockProvider.requests)
        rows = self.sql("SELECT id,AI_EMBED('enh_embedding',content,dim) FROM enh_embed_rows ORDER BY id;").splitlines()
        self.assertEqual(len(rows),150)
        for row in rows:
            key,value=row.split('\t');i=int(key)
            if i%13 == 0:
                self.assertEqual(value,'NULL')
            else:
                self.assertEqual(json.loads(value),vector('row-'+str(i),3+i%2))
        requests = [r for r in MockProvider.requests[start:] if r[0] == '/embed']
        sizes = [len(r[1]['input']) for r in requests]
        self.assertTrue(any(n>1 for n in sizes), 'No cross-row batching: '+repr(sizes))
        self.assertLess(len(requests),138)
        self.assertEqual(sum(sizes),138)
        rows = self.sql("SELECT id,AI_EMBED(CASE WHEN id%2=0 THEN 'enh_embedding' ELSE 'enh_ollama_embed' END,content,3) FROM enh_embed_rows ORDER BY id;").splitlines()
        self.assertEqual(len(rows),150)
        for row in rows:
            key,value=row.split('\t');i=int(key)
            if i%13 == 0: self.assertEqual(value,'NULL')
            else: self.assertEqual(json.loads(value),vector('row-'+str(i),3))
        before = len(MockProvider.requests)
        self.assertEqual(self.sql("SELECT SUM(AI_EMBED('enh_embedding',NULL) IS NULL) FROM enh_embed_rows;"),'150')
        self.assertEqual(len(MockProvider.requests),before)
        rows = self.sql("SELECT id,AI_EMBED('enh_embedding',content,3) FROM enh_embed_rows WHERE id%2=1 ORDER BY id LIMIT 17;").splitlines()
        self.assertEqual(len(rows),17)
        self.assertEqual([int(row.split('\t')[0]) for row in rows],list(range(1,34,2)))
        for row in rows:
            key,value=row.split('\t');i=int(key)
            self.assertEqual(value=='NULL', i%13==0)
            if value!='NULL': self.assertEqual(json.loads(value),vector('row-'+str(i),3))
        self.set_path('enh_embedding','/embed/duplicate')
        self.sql("SELECT AI_EMBED('enh_embedding',content,3) FROM enh_embed_rows;",True)
        self.set_path('enh_embedding','/embed/fail_later')
        self.sql("SELECT AI_EMBED('enh_embedding',content,3) FROM enh_embed_rows;",True)
        self.set_path('enh_embedding','/embed')
        self.assertEqual(json.loads(self.sql("SELECT AI_EMBED('enh_embedding','after failure',3);")),vector('after failure',3))
        self.sql('DROP TABLE enh_embed_rows;')
        print('Cross-row request sizes:',sizes,flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client',required=True)
    parser.add_argument('--port',type=int,default=12884)
    args=parser.parse_args()
    server=ThreadingHTTPServer(('127.0.0.1',0),MockProvider)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    AISQLTests.client=args.client;AISQLTests.port=args.port
    AISQLTests.mock_url='http://127.0.0.1:'+str(server.server_port)
    try:
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(AISQLTests)
        result=unittest.TextTestRunner(verbosity=2).run(suite)
        print('HTTP requests:',len(MockProvider.requests),flush=True)
        return 0 if result.wasSuccessful() else 1
    finally:
        server.shutdown();server.server_close()


if __name__=='__main__':
    raise SystemExit(main())
