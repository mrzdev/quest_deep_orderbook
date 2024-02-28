FROM python:3.10-slim as build

WORKDIR /opt/app
COPY . /opt/app
RUN python -m venv /opt/app/venv
ENV PATH="/opt/app/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN pip install --no-cache-dir https://codeload.github.com/mrzdev/unicorn-binance-rest-api/zip/refs/heads/master
RUN pip install --no-cache-dir https://codeload.github.com/mrzdev/unicorn-binance-websocket-api/zip/refs/heads/master
RUN pip install --no-cache-dir https://codeload.github.com/mrzdev/unicorn-binance-local-depth-cache/zip/refs/heads/master

CMD ["quest_deep_orderbook.py"]
ENTRYPOINT ["python"]