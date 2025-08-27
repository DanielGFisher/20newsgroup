docker-compose up --build

curl http://localhost:8000/publish

curl http://localhost:8001/messages/interesting
curl http://localhost:8001/messages/not_interesting
