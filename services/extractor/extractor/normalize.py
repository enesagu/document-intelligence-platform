def normalize_text(t: str) -> str:
    t = t.replace("\x00", " ")
    t = " ".join(t.split())
    return t.strip()
