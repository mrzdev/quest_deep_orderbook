pip install -r requirements.txt
docker run -p 9000:9000 \
  -p 9009:9009 \
  -p 8812:8812 \
  -p 9003:9003 \
  -v "$(pwd):/var/lib/questdb" \
  -d questdb/questdb:latest

docker build . -t quest_deep_orderbook:latest

docker run --network="host" -d quest_deep_orderbook:latest