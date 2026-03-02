import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Alloc API")

# Allow requests from your frontend (update origin when you have a real domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health_check():
    return {"status": "ok"}


@app.post("/run")
async def run_allocation(data: dict = {}):
    # Simulate algorithm running
    await asyncio.sleep(5)
    return {
        "status": "complete",
        "message": "Allocation run finished",
        "input_received": data,
    }
