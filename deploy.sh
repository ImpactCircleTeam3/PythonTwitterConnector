docker stop twitter-listener

git pull origin master

docker-compose up --build -d twitter-listener
