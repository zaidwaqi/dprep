FROM quay.io/centos/centos:stream8

RUN dnf -y group install "Development Tools"
RUN dnf install -y glibc-langpack-en python3.11 java-17-openjdk java-17-openjdk-devel python3-devel

# Create and activate virtual environment
RUN python3.11 -m venv /venv
ENV PATH="/venv/bin:$PATH"
ENV JAVA_HOME="/etc/alternatives/jre"

# Install Python packages
RUN python3.11 -m pip install pytest build twine pyarrow pandas pydoop Click paramiko geopandas
RUN python3.11 -m pip install -e .

# Set virtual environment as entrypoint
ENTRYPOINT ["/bin/bash"]
