### Run
- Run Docker Engine and create network
  - ```docker network create app-network```
- Run docker-compose
  - ```docker-compose --env-file .env --env-file config.env -f docker/docker-compose.yml up -d```
- Run Main

### Dispose
  - ```docker-compose --env-file .env --env-file config.env -f docker/docker-compose.yml down```