import os

def log(msg: str, level: str = "INFO"):
    lvl = os.getenv("LOG_LEVEL", "INFO")
    print(f"[{level}] {msg}")
