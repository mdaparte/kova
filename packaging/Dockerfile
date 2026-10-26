# Runtime image. The build context is the staged release directory:
# kova, kovadump, libkova.so, kova.toml, seed-oracle.js.
FROM debian:bookworm-slim

RUN useradd --system --uid 10001 --home-dir /var/lib/kova --create-home kova

COPY kova kovadump /usr/local/bin/
COPY libkova.so /usr/local/lib/libkova.so
COPY kova.toml /etc/kova/kova.toml
COPY seed-oracle.js /usr/share/kova/seed-oracle.js

ENV KOVA_CONFIG=/etc/kova/kova.toml \
    KOVA_DATA_DIR=/var/lib/kova

USER kova
WORKDIR /var/lib/kova
EXPOSE 27018
VOLUME /var/lib/kova

LABEL org.opencontainers.image.source="https://github.com/mdaparte/kova" \
      org.opencontainers.image.description="Kova database"

ENTRYPOINT ["kova"]
