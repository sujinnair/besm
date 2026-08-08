import uvicorn

if __name__ == "__main__":
    uvicorn.run("besm.ui.app:app", host="127.0.0.1", port=8888, reload=False)
