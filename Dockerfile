FROM python:3.13-slim-bookworm

# Install compilation environment
COPY sources.list /etc/apt/sources.list.d/debian.sources
WORKDIR /app

RUN apt-get update \
    && apt-get install -y \
    curl \
    lsb-release \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# Install .NET 8.0 runtime
RUN curl -L https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb

RUN apt-get update \
    && apt-get install -y \
    dotnet-runtime-8.0 \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# Install python dependencies
COPY requirements-docker.txt requirements.txt

RUN python3 -m pip install -i "https://pypi.tuna.tsinghua.edu.cn/simple" --upgrade pip \
    && pip3 install -i "https://pypi.tuna.tsinghua.edu.cn/simple" -r requirements.txt --no-cache-dir

# Copy src
COPY SCMeTA SCMeTA
COPY RawFileReader RawFileReader
COPY main.ipynb.example main.ipynb
COPY web.py.example web.py

# Set entry point
RUN mkdir Data \
    && chmod -R 777 Data
EXPOSE 8888
VOLUME ["/app/Data"]
