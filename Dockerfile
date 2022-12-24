FROM golang:1.19
RUN mkdir -p /usr/src/discuss
COPY . /usr/src/discuss
WORKDIR /usr/src/discuss
RUN go mod tidy
ENTRYPOINT go run cmd/discuss.go
