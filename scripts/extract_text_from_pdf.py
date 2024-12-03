import io
import pdfplumber

def extract_text_from_pdf(response):
    """Extract text from a PDF streamed from MinIO."""
    # Convert MinIO response stream to a seekable BytesIO object
    pdf_data = io.BytesIO(response.read())  # This reads all content into memory
    with pdfplumber.open(pdf_data) as pdf:  # Open the seekable BytesIO object
        text = ""
        for page in pdf.pages:
            text += page.extract_text()  # Extract text from each page
    return text
