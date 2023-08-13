from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys

import polars as pl

DEBUGGING = True

STOP_SIGNAL = "!!!STOP!!!"

EXAMPLES = Path.cwd() / "examples"
BLK_DIR = EXAMPLES / "blocks"
PLDB = EXAMPLES / "pldb"

POLARS_TXNS = PLDB / "transactions.feather"
POLARS_BLKS = PLDB / "blocks.feather"


@dataclass
class PolarsDF:
    file: Path
    df: pl.LazyFrame | pl.DataFrame | None = None


@dataclass
class PolarsDB:
    txns: PolarsDF
    blks: PolarsDF


def setup():
    db = PolarsDB(
        txns=PolarsDF(file=POLARS_TXNS),
        blks=PolarsDF(file=POLARS_BLKS),
    )

    try:
        db.blks.df = pl.scan_ipc(POLARS_BLKS)
        db.txns.df = pl.scan_ipc(POLARS_TXNS)
    except FileNotFoundError as fnfe:
        print(f"File not found: {fnfe}")

    return db


import polars as pl

# Assuming you have two Polars DataFrames `df1` and `df2` with the given structure
# Replace 'df1' and 'df2' with your actual DataFrame variable names


# Define a function to extract the 'rnd' field from the block dictionaries
def extract_rnd(blk: dict) -> int:
    return blk["rnd"]

def fix_missing_rnd(blk):
    if "rnd" not in blk["block"]:
        blk["block"]["rnd"] = 0
    return blk

def merge(pdf: PolarsDF, blocks: list[dict]):
    orig_len = 0
    if pdf.df is None:
        pdf.df = pl.DataFrame([fix_missing_rnd(blk) for blk in blocks])
    else:
        assert isinstance(pdf.df, pl.LazyFrame)
        orig = pdf.df.collect()
        orig_len = len(orig)
        orig_rounds = set(orig["block"].apply(extract_rnd).to_list())

        if blks := [
            fix_missing_rnd(blk)
            for blk in blocks
            if blk["block"].get("rnd", 0) not in orig_rounds
        ]:
            try:
                pdf.df = orig.extend(pl.DataFrame(blks))
            except pl.ShapeError as se:
                print(f"ShapeError: {se}")
                bs, ds = (otd := orig.to_dict())['block'], otd['delta']
                collected_dicts = [{'block': b, 'delta': d} for b, d in zip(bs, ds)]
                pdf.df = pl.DataFrame(collected_dicts + blks)
        else:
            print(f"No new blocks to add to {pdf.file}")
            return

    pdf.df.write_ipc(pdf.file)
    print(
        f"{pdf.file} updated with {len(blocks)} blocks from original {orig_len} blocks"
    )


if __name__ == "__main__":
    blocks_iter = sorted(os.listdir(BLK_DIR)) if DEBUGGING else sys.stdin

    db = setup()
    blocks = []
    try:
        for i, line in enumerate(blocks_iter):
            if not (trimmed := line.strip()):
                continue

            print(f"{i}. {trimmed=}")
            with open(f"{BLK_DIR}/{trimmed}") as f:
                blocks.append(json.loads(f.read()))
    except KeyboardInterrupt:
        print("TERMINATING PROGRAM")
    finally:
        merge(db.blks, blocks)
