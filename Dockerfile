FROM golang:1.23 as builder

ARG REPO_TOKEN
ARG BUILD_NAME
ARG BUILD_VERSION
ARG BUILD_COMMIT

ENV TZ=Etc/UCT
ENV GO111MODULE=on
ENV CGO_ENABLED=1

WORKDIR /build

RUN git config --global url."https://${REPO_TOKEN}:x-oauth-basic@github.com/robotjoosen".insteadOf "https://github.com/robotjoosen"
RUN go env -w GOPRIVATE="*github.com/robotjoosen"

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build \
    -ldflags " \
    -X 'main.buildName=$BUILD_NAME' \
    -X 'main.buildVersion=$BUILD_VERSION' \
    -X 'main.buildCommit=$BUILD_COMMIT' \
    " \
    -trimpath \
    -o app .

WORKDIR /dist

RUN cp /build/app ./app

RUN ldd app | tr -s '[:blank:]' '\n' | grep '^/' | \
    xargs -I % sh -c 'mkdir -p $(dirname ./%); cp % ./%;' \

RUN mkdir -p lib64 && cp /lib64/ld-linux-x86-64.so.2 lib64/

FROM alpine

RUN apk --no-cache add curl ngrep iputils bash

COPY --chown=0:0 --from=builder /dist /

USER 0

EXPOSE 8080

ENTRYPOINT [ "/app" ]
