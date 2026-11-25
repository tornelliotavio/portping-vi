FROM python:3.12-slim
WORKDIR /app
COPY *.py ./
COPY static ./static
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
EXPOSE 8080
CMD ["python", "app.py"]
