FROM rust:1.88-slim-bullseye AS builder

WORKDIR /app
COPY . .

RUN cargo build --release

FROM debian:bullseye-slim AS production

WORKDIR /app
COPY --from=builder /app/target/release/codecrafters-redis .

EXPOSE 6379
CMD ["./codecrafters-redis"]