# Build exporter
FROM debian:bookworm AS builder

WORKDIR /usr/src/


# Install python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install dependencies
COPY requirements.txt /usr/src/
RUN pip3 install --break-system-packages -r requirements.txt
RUN pip3 install --break-system-packages pyinstaller

# Build a one file executable
COPY . /usr/src/
RUN pyinstaller --onefile ./src/pg253/__main__.py

# Build final image
FROM postgres:16

COPY --from=builder /usr/src/dist/__main__ /usr/bin/pg253
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get upgrade -y -q && \
    apt-get dist-upgrade -y -q && \
    apt-get -y -q autoclean && \
    apt-get -y -q autoremove

ENTRYPOINT ["/usr/bin/pg253"]
