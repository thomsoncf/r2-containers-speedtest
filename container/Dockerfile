# syntax=docker/dockerfile:1
# Cloudflare Containers requires linux/amd64. Wrangler's container build
# targets that platform explicitly, so we don't need FROM --platform here.

FROM rclone/rclone:latest AS rclone

FROM python:3.12-alpine

COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY agent.py /agent.py

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python3", "/agent.py"]
