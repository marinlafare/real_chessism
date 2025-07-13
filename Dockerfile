# Use an official Python runtime as a parent image
FROM python:3.12-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file into the working directory
# This step is done early to leverage Docker's build cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- CRUCIAL CHANGE: Copy the contents of the current directory (real_chessism/) ---
# Since this Dockerfile will now be in 'real_chessism/', 'COPY . .' copies its contents.
COPY . .

# Expose the port that FastAPI will run on
EXPOSE 444

# Command to run the application using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "444"]
