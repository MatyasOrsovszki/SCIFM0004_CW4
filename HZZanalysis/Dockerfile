FROM python:3.10-slim

# Install necessary libraries
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Install Docker Compose
RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# Set the working directory in the container
WORKDIR /app

# Copy the application files into the container
COPY . /app
COPY ./shutdown.sh /app/shutdown.sh
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


RUN chmod +x /app/shutdown.sh

# Set the default command to run
CMD ["python", "producer.py"]
