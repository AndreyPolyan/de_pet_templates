FROM redis:alpine

# Install Python and pip
RUN apk add --no-cache python3 py3-pip

# Create a virtual environment and install dependencies
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy requirements.txt and install dependencies inside venv
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy Redis configuration file
COPY redis.conf /usr/local/etc/redis/redis.conf

# Copy the initialization script
COPY ./init_scripts/load_data.py /docker-entrypoint-initdb.d/load_data.py

# Set working directory
WORKDIR /docker-entrypoint-initdb.d

# Run Redis with the new configuration and then execute data loading script
CMD ["sh", "-c", "redis-server /usr/local/etc/redis/redis.conf --daemonize no & sleep 5 && python3 /docker-entrypoint-initdb.d/load_data.py && tail -f /dev/null"]