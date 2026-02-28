from fastapi import FastAPI

app = FastAPI(title="scrapping_app", version="0.1.0")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
