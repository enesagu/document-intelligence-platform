from PyPDF2 import PdfReader
import io

def extract_text_pypdf(data: bytes):
    reader = PdfReader(io.BytesIO(data))
    pages = []
    for i, p in enumerate(reader.pages, start=1):
        txt = p.extract_text()
        if txt:
            pages.append({"page_number": i, "text": txt})
    return pages
