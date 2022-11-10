[![godoc for greencoda/tasq][godoc-badge]][godoc-url]
[![Build Status][actions-badge]][actions-url]
[![Go 1.19](https://img.shields.io/badge/Go-1.19-%2300ADD8?logo=go)](https://golang.org/doc/go1.19)

<p align="center"><img src=".github/splash_image.png" width="500"></p>

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

[goversion-badge]: https://img.shields.io/badge/Go-1.19-%2300ADD8?logo=go
[goversion-url]: https://golang.org/doc/go1.19
[godoc-badge]: https://pkg.go.dev/badge/github.com/greencoda/tasq
[godoc-url]: https://pkg.go.dev/github.com/greencoda/tasq
[actions-badge]: https://github.com/greencoda/tasq/actions/workflows/test.yml/badge.svg
[actions-url]: https://github.com/greencoda/tasq/actions/workflows/test.yml