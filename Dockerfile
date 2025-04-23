FROM python:3.11-slim-bullseye

ENV MONO_VERSION 6.12.0.182

COPY sources.list /etc/apt/sources.list

WORKDIR /app

RUN apt-get update \
    && apt-get install -y \
    curl \
    gnupg \
    lsb-release \
    gcc \
    python3-dev \
    dirmngr \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN curl -L https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb

RUN apt-get update \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --batch --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF \
    && gpg --batch --export --armor 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF > /etc/apt/trusted.gpg.d/mono.gpg.asc \
    && gpgconf --kill all \
    && rm -rf "$GNUPGHOME" \
    && apt-key list | grep Xamarin \
    && apt-get purge -y --auto-remove gnupg dirmngr
  
RUN echo "deb https://download.mono-project.com/repo/debian stable-buster/snapshots/$MONO_VERSION main" > /etc/apt/sources.list.d/mono-official-stable.list \
    && apt-get update \
    && apt-get install -y mono-devel \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN apt-get update \
    && apt-get install -y \
    dotnet-runtime-6.0 \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# Install python dependencies
COPY requirements-docker.txt requirements.txt

RUN python3 -m pip install -i "https://pypi.tuna.tsinghua.edu.cn/simple" --upgrade pip \
    && pip3 install -i "https://pypi.tuna.tsinghua.edu.cn/simple" -r requirements.txt --no-cache-dir


COPY SCMeTA SCMeTA

COPY main.ipynb.example main.ipynb

RUN mkdir Data \
    && chmod -R 777 Data

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--port=8888" ]

EXPOSE 8888
VOLUME ["/app/Data"]
