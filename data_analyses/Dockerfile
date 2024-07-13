# Use the official Streamlit image as a base image
FROM streamlit/streamlit:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt and Dockerfile into the container
COPY requirements.txt ./requirements.txt

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the entire current directory into the container
COPY . .

# Command to run the Streamlit app
CMD ["streamlit", "run", "app.py"]
