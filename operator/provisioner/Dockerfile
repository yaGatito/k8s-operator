FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/provisioner .

FROM alpine:3.20
COPY --from=build /out/provisioner /usr/local/bin/provisioner
EXPOSE 8080
ENTRYPOINT ["provisioner"]
