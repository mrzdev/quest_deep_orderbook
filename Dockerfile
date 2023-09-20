FROM python:3.9-slim-bullseye

RUN pip install --no-cache-dir questdb
RUN pip install --no-cache-dir pyarrow
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir polars
RUN pip install --no-cache-dir unicorn-binance-local-depth-cache
COPY quest_deep_orderbook.py /quest_deep_orderbook.py

CMD ["/quest_deep_orderbook.py"]
ENTRYPOINT ["python"]