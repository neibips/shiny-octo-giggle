# API burst

Send 100 requests concurrently by default using Go's standard library. Requires Go 1.22 or newer.

```sh
go run . -url https://example.com/api
```

Change the total number of requests and the maximum number running at once:

```sh
go run . -url https://example.com/api -count 10000 -concurrency 200
```

Build once for repeated runs:

```sh
go build -o apiburst .
./apiburst -url https://example.com/api -count 100 -concurrency 100
```

POST with a body and headers:

```sh
./apiburst -url https://example.com/api \
  -count 1000 -concurrency 100 -timeout 5s \
  -method POST -data '{"hello":"world"}' \
  -header 'Content-Type: application/json' \
  -header 'Authorization: Bearer YOUR_TOKEN'
```

The worker pool keeps goroutine and memory usage proportional to concurrency, so increasing `-count` does not create a goroutine for every request. Workers share an HTTP transport, reuse connections, and drain response bodies so those connections can be reused. HTTP/2 is negotiated when available. There is no deliberate delay between requests; increase concurrency to find the best throughput for your endpoint and machine. Higher concurrency does not always mean higher throughput.

`-timeout` covers each whole request, including reading the response. Redirects are reported without following them. The app adds no retries; Go's HTTP transport can internally retry eligible requests after certain connection failures. The count is the number of application-level request attempts, and errors still consume an attempt. Ctrl+C cancels outstanding requests and prints partial results.

The summary includes elapsed time, completed requests per second, min/average/max latency, response bytes, HTTP status counts, and request/read errors. Latency includes consuming the response body and includes failed attempts. Response bytes count the body bytes read (after decompression when Go performs it). Exit codes: `0` for all requests returning 2xx with bodies read successfully, `1` for request errors or non-2xx responses, `2` for invalid arguments, and `130` for interruption.

```sh
go test -race ./...
```
