FROM rust:1.33 as build

ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl

ENV DEBIAN_FRONTEND=noninteractive
RUN \
	apt-get update && \
	apt-get -y install ca-certificates musl-tools && \
	rustup target add ${CARGO_BUILD_TARGET}

# Use Tini as our PID 1. This will enable signals to be handled more correctly.
#
# Note that this can't be downloaded inside the scratch container as we have no
# chmod command.
#
# TODO: Use `--init` instead when it is more well-supported (this should be the
# case by Jan 1, 2020).
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static /tini
RUN chmod +x /tini

WORKDIR /source

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

COPY --from=build /tini /tini

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /build/ /

ENTRYPOINT ["/tini", "--", "/rudolfs"]
CMD ["--cache-dir", "/data"]
