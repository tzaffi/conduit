from fastapi import FastAPI
from pydantic import BaseModel

ENDPOINT = "/receive"
PORT = 1337

app = FastAPI()

class BlockData(BaseModel):
    round: int
    blk: dict = None # type: ignore
    empty: bool

@app.post(ENDPOINT)
def receive(blk_round: BlockData):
    # request = blk_round.model_dump()
    print(f"{blk_round=}")
    
    return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)


"""
curl -X POST http://localhost:1337/receive -H "Content-Type: application/json" -d '{"round": 1, "blk": {"someKey": "someValue"}, "empty": false}'
"""