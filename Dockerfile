FROM rust:1.64 as build

ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl

ENV DEBIAN_FRONTEND=noninteractive
RUN \
	apt-get update && \
	apt-get -y install ca-certificates musl-tools && \
	rustup target add ${CARGO_BUILD_TARGET}

ENV PKG_CONFIG_ALLOW_CROSS=1

# Build the real project.
COPY ./ ./

RUN cargo build --release

RUN \
	mkdir -p /build && \
	cp target/${CARGO_BUILD_TARGET}/release/rudolfs /build/ && \
	strip /build/rudolfs

# Use scratch so we can get an itty-bitty-teeny-tiny image. This requires us to
# use musl when building the application.
FROM scratch

EXPOSE 8080
VOLUME ["/data"]

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /build/ /

ENTRYPOINT ["/rudolfs"]
CMD ["--cache-dir", "/data"]
