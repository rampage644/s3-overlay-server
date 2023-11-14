# Stage 1: Build the application

FROM golang:latest AS build

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/overlay-server

# Stage 2: Run the application

FROM alpine:latest

COPY --from=build bin/overlay-server /
# Set the working directory
WORKDIR /bin

# Expose the port the application will run on
EXPOSE 8080

CMD ["/overlay-server"]
