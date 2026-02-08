from fastapi import FastAPI

app = FastAPI()

@app.get("/messages")
async def get_messages():
    return "Not implemented yet"