import butterflow
import morpho
import json
from glob import glob
from butterflow import lex, Parser, TypeChecker, Builder, Runtime
import numpy as np
import argparse
import os
from finetune_util import query_ollama
from morpho.score import SemanticTeacher, SyntaxTeacher
from datetime import datetime


def load_docs(args):
    with open(args.syntax_file, "rt") as f:
        syntax_doc = f.read()
    stdlib_doc = ""
    for path in glob(args.stdlib_path+"/*.txt"):
        with open(path, "rt") as f:
            lines = f.readlines()
            signature = lines[lines.index("1. Type signature\n")+1]
            stdlib_doc += signature
    return syntax_doc, stdlib_doc


def parse_args():

    parser = argparse.ArgumentParser(
        description="Process text file with LLM via Ollama/OpenWebUI")

    # Required Arguments
    parser.add_argument("--input_dir", type=str,
                        required=True, help="Directory of input metadata")
    parser.add_argument("--output_dir", type=str, required=True,
                        help="Directory to save the result")

    parser.add_argument("--selection", type=str, required=True,
                        help="Selection Rule with format: field:condition,field:condition,...")
    parser.add_argument("--api_config", type=str, required=True,
                        help="Path to JSON file containing api config")
    parser.add_argument("--prompt_file", type=str, required=True,
                        help="Path to JSON file containing prompt template")
    parser.add_argument("--datadir", type=str, required=False,
                        help="npy location", default="./data/npy")

    parser.add_argument("--syntax_file", type=str, required=False,
                        default="./morpho/docs/syntax.txt")
    parser.add_argument("--stdlib_path", type=str, required=False,
                        default="./morpho/docs-stdlib/")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    # Initialize pool
    with open(args.api_config, "rt") as f:
        api_config = json.load(f)
    library = morpho.LibraryIndexer(config=api_config)
    library.load(args.input_dir)
    # filter_fn = {"data_type", lambda x: x=="summary"}
    filter_fn = {x.split(":")[0].strip(): lambda y: y == x.split(":")[
        1].strip() for x in args.selection.split(",")}
    library.filter(filter_fn)
    # library.embed()

    # Initialize transpiler
    transpiler = morpho.Transpiler(
        config=api_config, prompt_path=args.prompt_file)
    syntax_doc, stdlib_doc = load_docs(args)
    with open(args.prompt_file, "rt") as f:
        config = json.load(f)

    # Initialize backtester
    teacher_syn = SyntaxTeacher()
    teacher_sem = SemanticTeacher(datadir=args.datadir, propritary=False)

    # Load data
    for hash in library.get_hash_list():
        metadata = library.get_by_hash(hash)
        with open(metadata["path"], "rt") as f:
            idea = f.read()
        response = transpiler.generate(system_context_args={
                                       "syntax": syntax_doc, "functions": stdlib_doc}, user_prompt_args={"idea_text": idea})
        print(response.strategies[0].name)
        results = []
        for r in response.strategies:
            try:
                resp_name = r.name
                resp_desc = r.description
                r.code = "result = data(id=\"close\")"
                graph, scores_1, error_msg_1 = teacher_syn.score(r.code)
                ret_tvr, scores_2, error_msg_2 = teacher_sem.score(graph)
                if ret_tvr:
                    ret, tvr = ret_tvr
                    results.append(
                        {"name": resp_name, "desc": resp_desc, "ret": ret.tolist(), "tvr": tvr.tolist()})
            except:
                pass
        if results:
            metadata["data_type"] = "code"
            metadata["results"] = results
            out_path = args.output_dir + "/" + hash + "_" + \
                datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
            with open(out_path, "rt") as f:
                json.dump(metadata, out_path)

    print(f"\r\nDone!")


if __name__ == "__main__":
    # python transpile.py --input_dir ./metadata --output_dir ./data/backtest --selection data_type:summary --api_config ./prompts/transpile_api.json --prompt_file ./prompts/transpile_prompt.json --datadir ./data/npy  --syntax_file ../../../butterflow/docs/syntax.txt --stdlib_path ../../../butterflow/docs-stdlib/
    # for ITER in `seq 20`; do python transpile.py --input_dir ./metadata --output_dir ./data/backtest --selection data_type:summary --api_config ./prompts/transpile_api.json --prompt_file ./prompts/transpile_prompt.json --datadir ./data/npy  --syntax_file ../../../butterflow/docs/syntax.txt --stdlib_path ../../../butterflow/docs-stdlib/; done
    main()
