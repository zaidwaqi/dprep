FROM quay.io/centos/centos:stream8

RUN dnf install -y glibc-langpack-en python3.11 make

# Create and activate virtual environment
RUN python3.11 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Install Python packages
COPY requirements.txt ./requirements.txt
RUN python3.11 -m pip install --no-cache-dir -r requirements.txt
RUN python3.11 -m pip install -e .

# Set virtual environment as entrypoint
ENTRYPOINT ["/bin/bash"]