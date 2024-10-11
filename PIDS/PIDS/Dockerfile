FROM rasa/rasa:3.6.0-full


WORKDIR /app

# Instala SQLAlchemy 1.x
USER root
RUN pip install --no-cache-dir psycopg2-binary
USER 1001

# Copia tus acciones personalizadas
COPY ./actions /app/actions
