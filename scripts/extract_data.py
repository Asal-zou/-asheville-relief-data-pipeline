from minio import Minio 
#import pdfplumber
from transformers import pipeline
from PyPDF2 import PdfReader
import io  
import re  # for regex operations


import re
from minio import Minio
from PyPDF2 import PdfReader
import io

# Connect to MinIO
def get_minio_client(endpoint, access_key, secret_key):
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

# Extract text from PDF
def extract_text_from_pdf_stream(pdf_content):
    """Extract text directly from a PDF binary stream."""
    pdf_reader = PdfReader(pdf_content)
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text() or ""
    return text

# Preprocess text while retaining structure
def preprocess_text(text):
    """Retain structure (bullets, punctuation) while normalizing spaces."""
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize spaces
    return text

# Extract hotlines section specifically
def extract_hotlines_section(text):
    """Extract hotlines with associated details."""
    hotlines_pattern = r"●\s*(.+?)\s*Call\s*([\d()\- ]+).*?Visit\s*([\w.:/]+)"
    matches = re.findall(hotlines_pattern, text)
    hotlines = []
    for match in matches:
        hotline = {
            "name": match[0].strip(),
            "phone": match[1].strip(),
            "website": match[2].strip(),
        }
        hotlines.append(hotline)
    return hotlines

# Extract entities (phone numbers, URLs, emails)
def extract_entities(text):
    """Extract phone numbers, URLs, and emails."""
    phone_numbers = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)
    urls = re.findall(r'https?://[^\s]+|www\.[^\s]+', text)
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    return {
        "phone_numbers": phone_numbers,
        "urls": urls,
        "emails": emails,
    }

# Main function
def extract_and_process_data():
    try:
        # Step 1: Connect to MinIO and fetch the PDF
        client = get_minio_client("localhost:9000", "admin", "password")
        bucket_name = "ashevillerelief-raw-documents"
        object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"

        # Fetch the PDF file as binary content
        response = client.get_object(bucket_name, object_name)
        pdf_content = response.read()
        print(f"Successfully accessed object: {object_name}")

        # Step 2: Extract and preprocess text
        raw_text = extract_text_from_pdf_stream(io.BytesIO(pdf_content))
        if not raw_text.strip():
            raise ValueError("No text extracted from the PDF.")
        preprocessed_text = preprocess_text(raw_text)

        # Print the preprocessed text
        print("\nFull Preprocessed Text:")
        print(preprocessed_text)

        # Step 3: Extract key entities
        entities = extract_entities(preprocessed_text)
        print("\nExtracted Entities:")
        print(entities)

        # Step 4: Extract the hotlines section
        hotlines = extract_hotlines_section(preprocessed_text)
        print("\nExtracted Hotlines:")
        print(hotlines)

        # Step 5: Structure the extracted data
        structured_data = {
            "entities": entities,
            "hotlines": hotlines,
        }
        print("\nStructured Data:")
        print(structured_data)

        return structured_data

    except Exception as e:
        print(f"Error during processing: {e}")

# Run the script
if __name__ == "__main__":
    extract_and_process_data()




# # Connect to MinIO
# def get_minio_client(endpoint, access_key, secret_key):
#     """Establish a connection to MinIO."""
#     return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

# # Extract text directly from a PDF binary stream
# def extract_text_from_pdf_stream(pdf_content):
#     """Extract text directly from a PDF binary stream."""
#     pdf_reader = PdfReader(pdf_content)  # Use PyPDF2 to read the binary content
#     text = ""
#     for page in pdf_reader.pages:
#         text += page.extract_text() or ""  # Avoid None if page has no text
#     return text

# # Clean raw text
# def clean_text(raw_text):
#     """Preprocess raw text to remove unnecessary line breaks and normalize it."""
#     return re.sub(r'\s+', ' ', raw_text).strip()

# # Extract phone numbers using regex
# def extract_phone_numbers(text):
#     """Extract phone numbers from text."""
#     return re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)

# # Extract URLs using regex
# def extract_urls(text):
#     """Extract URLs from text."""
#     return re.findall(r'https?://[^\s]+|www\.[^\s]+', text)

# # Extract email addresses using regex
# def extract_emails(text):
#     """Extract email addresses from text."""
#     return re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)

# # Extract sections based on headings
# def extract_sections_v2(text, keywords):
#     """Extract sections and their content based on specific headings."""
#     sections = {}
#     lines = text.split("\n")
#     current_keyword = None
#     section_content = []

#     for i, line in enumerate(lines):
#         line = line.strip()
#         if not line:
#             continue

#         # Match keyword, ignoring case and trailing colons
#         matched_keyword = next((kw for kw in keywords if kw.lower() in line.lower().replace(":", "")), None)
#         if matched_keyword:
#             # Save the previous section content
#             if current_keyword:
#                 sections[current_keyword] = section_content
#             current_keyword = matched_keyword
#             section_content = []
#             print(f"Matched keyword: {matched_keyword} at line {i}")
#         elif current_keyword:
#             # Add lines matching relevant patterns to the section
#             if re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', line) or re.search(r'https?://|www\.', line) or line.startswith("●"):
#                 section_content.append(line)

#     # Save the last section
#     if current_keyword:
#         sections[current_keyword] = section_content

#     print(f"Final extracted sections: {sections}")
#     return sections

# # Parse structured content from Hotlines section
# def parse_hotlines(hotlines):
#     """Parse hotline section content into structured data."""
#     hotline_data = []
#     for line in hotlines:
#         name_match = re.match(r'^(.*?)(?=\sCall|\sVisit)', line)
#         phone_match = re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', line)
#         url_match = re.search(r'https?://[^\s]+|www\.[^\s]+', line)
#         hotline_data.append({
#             "name": name_match.group(0).strip() if name_match else None,
#             "phone_number": phone_match.group(0) if phone_match else None,
#             "url": url_match.group(0) if url_match else None
#         })
#     return hotline_data

# # Chunk text for LLM processing
# def chunk_text(text, chunk_size=512):
#     """Split text into smaller chunks for LLM processing."""
#     words = text.split()
#     for i in range(0, len(words), chunk_size):
#         yield " ".join(words[i:i+chunk_size])

# # Process text using a Hugging Face LLM
# def process_text_with_free_llm(raw_text):
#     """Use a free LLM to summarize and structure data."""
#     summarizer = pipeline("summarization", model="google/flan-t5-base")
#     summaries = []

#     for chunk in chunk_text(raw_text, chunk_size=512):
#         try:
#             summary = summarizer(
#                 chunk,
#                 max_length=300,  # Limit output length
#                 min_length=50,   # Minimum length
#                 do_sample=False  # Deterministic output
#             )
#             summaries.append(summary[0]["summary_text"])
#         except Exception as e:
#             print(f"Error summarizing chunk: {e}")

#     return " ".join(summaries)

# # Main function
# def extract_and_process_data():
#     try:
#         # Step 1: Connect to MinIO and fetch the PDF
#         client = get_minio_client("localhost:9000", "admin", "password")
#         bucket_name = "ashevillerelief-raw-documents"
#         object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"

#         response = client.get_object(bucket_name, object_name)
#         pdf_content = response.read()  # Read the stream once
#         print(f"Successfully accessed object: {object_name}")
#         print(f"Response content size: {len(pdf_content)} bytes")

#         # Step 2: Extract and clean text
#         raw_text = extract_text_from_pdf_stream(io.BytesIO(pdf_content))
#         if not raw_text.strip():
#             raise ValueError("No text extracted from the PDF.")
#         clean_raw_text = clean_text(raw_text)
#         print(f"Extracted and cleaned raw text:\n{clean_raw_text[:500]}")

#         # Step 3: Extract entities
#         phone_numbers = extract_phone_numbers(clean_raw_text)
#         urls = extract_urls(clean_raw_text)
#         emails = extract_emails(clean_raw_text)
#         sections = extract_sections_v2(clean_raw_text, ["Hotlines", "Steps to Take", "Resources"])

#         # Parse hotlines section
#         hotline_data = parse_hotlines(sections.get("Hotlines", []))

#         # Debugging
#         print(f"Extracted phone numbers: {phone_numbers}")
#         print(f"Extracted URLs: {urls}")
#         print(f"Extracted emails: {emails}")
#         print(f"Extracted Hotlines Data: {hotline_data}")

#         # Step 4: Structure extracted data
#         structured_data = {
#             "phone_numbers": phone_numbers,
#             "urls": urls,
#             "emails": emails,
#             "hotlines": hotline_data,
#             "sections": sections
#         }
#         print("Structured Data:")
#         print(structured_data)

#         # Step 5: Process raw text with LLM
#         summarized_data = process_text_with_free_llm(clean_raw_text)
#         print("Summarized Data:")
#         print(summarized_data)

#         return structured_data

#     except Exception as e:
#         print(f"Error during processing: {e}")

# # Run the script
# if __name__ == "__main__":
#     extract_and_process_data()




        
# #         # Step 3: Process text using a free LLM
# #         structured_data = process_text_with_free_llm(raw_text)
# #         print("Structured Data:")
# #         print(structured_data)

# #         return structured_data

# #     except Exception as e:
# #         print(f"Error during processing: {e}")

# # # Run the script
# # if __name__ == "__main__":
# #     extract_and_process_data()

# # # This script 
# # # Connects to MinIO.
# # # Fetches the document Abuse & Trafficking - AshevilleRelief.org.pdf from the bucket ashevillerelief-raw-documents.
# # # Extracts the text using tools like pdfplumber or streams the binary data.


# # def get_minio_client(endpoint , access_key, secret_key):
# #     return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)



# # # client= get_minio_client("localhost:900", "admin", "password")


# # # # Specify the bucket and folder
# # # bucket_name= "ashevillerelief-raw-documents"
# # # object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"


# # # Stream PDF file content into memory
# # # response= client.get_object( bucket_name, object_name)

# # # with pdfplumber.open(response) as pdf:
# # #     text=""
# # #     for page in pdf.pages:
# # #         text +=page.extract_text()
# # #     print("extract_text")
# # #     print(text)


# # try:
# #     client = get_minio_client("localhost:9000", "admin", "password")
# #     bucket_name = "ashevillerelief-raw-documents"
# #     object_name = "Abuse & Trafficking - AshevilleRelief.org.pdf"

# #     # Test connection by listing objects
# #     objects = client.list_objects(bucket_name)
# #     for obj in objects:
# #         print(f"Found object: {obj.object_name}")

# #     # Attempt to access the specific file
# #     response = client.get_object(bucket_name, object_name)
# #     print(f"Successfully accessed object: {object_name}")

# # except Error as e:
# #     print(f"MinIO error: {e}")
# # except Exception as e:
# #     print(f"Error: {e}")