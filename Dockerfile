FROM python:3.10-slim as build

WORKDIR /opt/app
COPY . /opt/app
RUN python -m venv /opt/app/venv
ENV PATH="/opt/app/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["quest_deep_orderbook.py"]
ENTRYPOINT ["python"]