import argparse
from glob import glob
import json
import random
from finetune_util import hasher, load_stdlib_doc, query_ollama, check_save
import os


def decompile():
    parser = argparse.ArgumentParser(
        description="Reverse-engineer Butterflow code into LLM query")

    parser.add_argument("--input_code", type=str, required=True)
    parser.add_argument("--butterflow_syntax", type=str,
                        required=False, default="./butterflow/docs/syntax.txt")
    parser.add_argument("--butterflow_stdlib", type=str,
                        required=False, default="./butterflow/docs-stdlib/*.txt")
    parser.add_argument("--prompt_file", type=str,
                        required=False, default="./prompts/finetune-01-reversing.json")
    parser.add_argument("--endpoint", type=str, required=True,
                        help="Ollama API URL (e.g., http://localhost:11434)")
    parser.add_argument("--temperature", type=float,
                        required=False, default=0.8)
    parser.add_argument("--model", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)

    args = parser.parse_args()

    with open(args.butterflow_syntax, "rt") as f:
        syntax = f.read()

    stdlib = ""
    for path in glob(args.butterflow_stdlib):
        stdlib_doc = load_stdlib_doc(path)
        stdlib += f"{stdlib_doc['name']}: {' '.join(stdlib_doc['description'].split())}\n"

    with open(args.input_code, "rt") as f:
        codebase = f.read()

    with open(args.prompt_file, "rt") as f:
        config = json.load(f)

    persona = random.choice(config["persona"])
    system_context = config["system"]
    system_context = system_context.replace("{language_name}", "ButterFlow")
    system_context = system_context.replace("{language_spec}", syntax)
    user_prompt = config["user"]
    user_prompt = user_prompt.replace("{persona}", persona)
    user_prompt = user_prompt.replace("{source_code}", codebase)

    result = query_ollama(endpoint=args.endpoint, model=args.model,
                          prompt=user_prompt, system_prompt=system_context, temperature=args.temperature)
    result["args"] = vars(args)
    result["system_context"] = system_context
    result["user_prompt"] = user_prompt
    code_name = os.path.splitext(os.path.split(args.input_code)[-1])[0]
    output_name = code_name + "_" + hasher(repr(result))
    check_save(args.output_dir, output_name, result)


if __name__ == "__main__":
    # python finetune-01-reversing.py --input_code ../docs/technical_indicators/adl.bf --endpoint http://rocm.c-jk.com:11434 --model gpt-oss:120b --output_dir ./data/finetune-01/
    # for FILE in ../docs/technical_indicators/*.bf; do python finetune-01-reversing.py --input_code $FILE --endpoint http://rocm.c-jk.com:11434 --model gpt-oss:120b --output_dir ./data/finetune-01/; done
    decompile()
