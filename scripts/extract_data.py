from minio import Minio 
#import pdfplumber
from transformers import pipeline
from PyPDF2 import PdfReader
import io  


# Connect to MinIO
def get_minio_client(endpoint, access_key, secret_key):
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

# Extract text directly from a PDF binary stream
def extract_text_from_pdf_stream(pdf_content):
    """Extract text directly from a PDF binary stream."""
    pdf_reader = PdfReader(pdf_content)  # Use PyPDF2 to read the binary content
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text() or ""  # Avoid None if page has no text
    return text

# Process text using a Hugging Face LLM
def process_text_with_free_llm(raw_text):
    """Use a free LLM to summarize and structure data."""
    summarizer = pipeline("summarization", model="google/flan-t5-base")  # Free LLM
    summary = summarizer(
        raw_text,
        max_length=300,  # Limit output length
        min_length=50,   # Minimum length
        do_sample=False  # Deterministic output
    )
    return summary[0]["summary_text"]

# Main function
def extract_and_process_data():
    try:
        # Step 1: Connect to MinIO and fetch the PDF
        client = get_minio_client("localhost:9000", "admin", "password")
        bucket_name = "ashevillerelief-raw-documents"
        object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"

        # Fetch the PDF file as binary content
        response = client.get_object(bucket_name, object_name)
        pdf_content = response.read()  # Read the stream once
        print(f"Successfully accessed object: {object_name}")
        print(f"Response content size: {len(pdf_content)} bytes")  # Debugging: Check file size

        # Step 2: Extract text from the PDF binary stream
        raw_text = extract_text_from_pdf_stream(io.BytesIO(pdf_content))  # Convert to BytesIO
        if not raw_text.strip():
            raise ValueError("No text extracted from the PDF.")
        print(f"Extracted raw text:\n{raw_text[:500]}")  # Preview the first 500 characters

        # Step 3: Process text using a free LLM
        structured_data = process_text_with_free_llm(raw_text)
        print("Structured Data:")
        print(structured_data)

        return structured_data

    except Exception as e:
        print(f"Error during processing: {e}")

# Run the script
if __name__ == "__main__":
    extract_and_process_data()

# # This script 
# # Connects to MinIO.
# # Fetches the document Abuse & Trafficking - AshevilleRelief.org.pdf from the bucket ashevillerelief-raw-documents.
# # Extracts the text using tools like pdfplumber or streams the binary data.


# def get_minio_client(endpoint , access_key, secret_key):
#     return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)



# # client= get_minio_client("localhost:900", "admin", "password")


# # # Specify the bucket and folder
# # bucket_name= "ashevillerelief-raw-documents"
# # object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"


# # Stream PDF file content into memory
# # response= client.get_object( bucket_name, object_name)

# # with pdfplumber.open(response) as pdf:
# #     text=""
# #     for page in pdf.pages:
# #         text +=page.extract_text()
# #     print("extract_text")
# #     print(text)


# try:
#     client = get_minio_client("localhost:9000", "admin", "password")
#     bucket_name = "ashevillerelief-raw-documents"
#     object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"

#     # Test connection by listing objects
#     objects = client.list_objects(bucket_name)
#     for obj in objects:
#         print(f"Found object: {obj.object_name}")

#     # Attempt to access the specific file
#     response = client.get_object(bucket_name, object_name)
#     print(f"Successfully accessed object: {object_name}")

# except Error as e:
#     print(f"MinIO error: {e}")
# except Exception as e:
#     print(f"Error: {e}")