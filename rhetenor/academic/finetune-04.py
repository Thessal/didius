# %%
from glob import glob
import json


def score_summary(scores):
    if scores["position_concentration"] == None:
        position_balance = 1.
    elif scores["position_concentration"] < 0.05:
        position_balance = 5.
    elif scores["position_concentration"] < 0.15:
        position_balance = 4.
    elif scores["position_concentration"] < 0.20:
        position_balance = 3.
    else:
        position_balance = 1.

    if scores["ret"] == None:
        information = 1.
    elif abs(scores["ret"]/(scores["std"]+abs(scores["ret"])+1e-9)) < 0.001:
        information = 2.
    elif abs(scores["ret"]/(scores["std"]+abs(scores["ret"])+1e-9)) < 0.01:
        information = 3.
    elif abs(scores["ret"]/(scores["std"]+abs(scores["ret"])+1e-9)) < 0.03:
        information = 4.
    elif abs(scores["ret"]/(scores["std"]+abs(scores["ret"])+1e-9)) >= 0.03:
        information = 5.
    else:
        information = 1.

    if scores["tvr"] == None:
        turnover = 1.
    elif (scores["tvr"] < 0.005) or (0.7 < scores["tvr"]):
        turnover = 2.
    elif (scores["tvr"] < 0.01) or (0.4 < scores["tvr"]):
        turnover = 3.
    elif (scores["max_tvr"] / scores["tvr"] > 8):
        turnover = 4.
    elif (scores["max_tvr"] / scores["tvr"] <= 8):
        turnover = 5.
    else:
        turnover = 1.

    scores_summary = {
        "syntax-lex": scores['lex'],
        "syntax-parse": scores['parse'],
        "syntax-type_check": scores['type_check'],
        "syntax-runtime": scores['build'],
        "semantics-position_balance": position_balance,
        "semantics-information": information,
        "semantics-turnover": turnover,
    }
    return scores_summary


def extract(path):
    with open(path, "rt") as f:
        h = json.load(f)["history"]
        reasoning = "\n".join(
            [hh["response"]['message']["thinking"] for hh in h])
        x = h[0]["response"]
        y = h[-1]
        question = x['system_context'] + "\n\n" + x['user_prompt']
        scores = y["scores"]
        solution = y["code"]
    return question, scores, reasoning, solution


def score_fn(scores):
    scores_summary = score_summary(scores)
    return sum(v - 1. for v in scores_summary.values() if type(v) == float)


# %%
paths = glob("data/finetune-03/*.json")
for i, path in enumerate(paths):
    question, scores, reasoning, solution = extract(path)
    if score_fn(scores) > 20:
        with open(f"data/finetune-04/{i}.json", "wt") as f:
            json.dump({
                "question": question, "reasoning": reasoning, "solution": solution
            }, f)
# %%


# ## pnl calculation

# from morpho.score import SemanticTeacher, SyntaxTeacher
# import numpy as np 

# datadir="./data/npy"
# teacher_syn = SyntaxTeacher()
# teacher_sem = SemanticTeacher(datadir=datadir, propritary=False)
# def check(code):
#     graph, scores_1, error_msg_1 = teacher_syn.score(code)
#     ret_tvr, scores_2, error_msg_2 =   teacher_sem.score(graph)
#     return ret_tvr


# paths = glob("data/finetune-03/*.json")
# for i, path in enumerate(paths):
#     question, scores, reasoning, solution = extract(path)
#     if score_fn(scores) > 20:
#         with open(f"data/finetune-04/{i}.pnl", "wt") as f:
#             ret, tvr = check(solution)
#             json.dump({
#                 "question": question, "reasoning": reasoning, "solution": solution, "ret":ret.tolist(), "tvr":tvr.tolist(),
#             }, f)


# %%
