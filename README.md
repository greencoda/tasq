# tasq

Tasq is Golang task queue using SQL database for persistence (currently supporting PostgreSQL only)

## Install

```shell
go get -u github.com/greencoda/tasq
```

## Usage Example

To try tasq locally, you'll need a PostgreSQL DB backend. You may use the supplied docker-compose.yml file to start a local instace
```shell
docker-compose -f example/docker-compose.yml up -d
```

Afterwards simply run the example.go file
```shell
go run _example/example.go
```