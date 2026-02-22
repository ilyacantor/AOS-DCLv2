import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    import uvicorn
    # Multi-worker mode for concurrency testing (matches production: render.yaml)
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=5000, reload=False, workers=2)
