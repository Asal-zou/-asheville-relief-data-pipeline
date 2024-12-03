from transformers import pipeline
import numpy as np
import torch

# Check versions
print(f"NumPy Version: {np.__version__}")
print(f"PyTorch Version: {torch.__version__}")

# Test a Hugging Face pipeline
summarizer = pipeline("summarization", model="google/flan-t5-base")
result = summarizer("This is a test input.", max_length=50, min_length=10, do_sample=False)
print("Summarization Result:", result)
