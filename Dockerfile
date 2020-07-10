FROM golang:1.14-alpine as builder

ARG BUILD=now
ARG VERSION=dev
ARG REPO=repository

WORKDIR /src

COPY . /src

RUN apk add --update make bash
RUN make bin/neofs-node

# Executable image
FROM scratch AS neofs-node

WORKDIR /

COPY --from=builder /src/bin/neofs-node /bin/neofs-node

CMD ["neofs-node"]
