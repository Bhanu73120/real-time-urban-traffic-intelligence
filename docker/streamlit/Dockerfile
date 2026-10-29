FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    plotly \
    psycopg2-binary

COPY monitoring/dashboard.py /app/monitoring/dashboard.py

EXPOSE 8501

CMD ["streamlit", "run", "monitoring/dashboard.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true"]