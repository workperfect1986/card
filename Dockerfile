FROM python:3.11-slim

WORKDIR /app

# Instalar apenas o necessário para o teste
RUN pip install fastapi uvicorn

COPY main.py .

EXPOSE 8000

CMD ["python", "main.py"]
