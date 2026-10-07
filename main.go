package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type headers []string

func (h *headers) String() string { return strings.Join(*h, ", ") }
func (h *headers) Set(value string) error {
	*h = append(*h, value)
	return nil
}

type config struct {
	url         string
	count       int64
	concurrency int
	timeout     time.Duration
	method      string
	data        string
	headers     headers
}

type stats struct {
	completed int64
	succeeded int64
	errors    int64
	bytes     int64
	total     time.Duration
	min       time.Duration
	max       time.Duration
	statuses  map[int]int64
	firstErr  error
	elapsed   time.Duration
}

func main() { os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr)) }

func runCLI(args []string, out, errOut io.Writer) int {
	var cfg config
	flags := flag.NewFlagSet("apiburst", flag.ContinueOnError)
	flags.SetOutput(errOut)
	flags.StringVar(&cfg.url, "url", "", "API endpoint URL (required)")
	flags.Int64Var(&cfg.count, "count", 100, "Total requests to send")
	flags.IntVar(&cfg.concurrency, "concurrency", 100, "Maximum simultaneous requests")
	flags.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "Timeout for each request, including reading its response")
	flags.StringVar(&cfg.method, "method", http.MethodGet, "HTTP method")
	flags.StringVar(&cfg.data, "data", "", "Request body (sent with every request)")
	flags.Var(&cfg.headers, "header", "Request header as 'Name: value' (repeatable)")
	flags.Usage = func() {
		fmt.Fprintln(errOut, "Usage: apiburst -url https://example.com/api [options]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(errOut, "error: unexpected positional arguments")
		return 2
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(errOut, "error:", err)
		return 2
	}
	if int64(cfg.concurrency) > cfg.count {
		cfg.concurrency = int(cfg.count)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(out, "Sending %d %s requests with concurrency %d\n", cfg.count, cfg.method, cfg.concurrency)
	result, err := benchmark(ctx, cfg)
	if err != nil {
		fmt.Fprintln(errOut, "error:", err)
		return 1
	}
	printStats(out, cfg, result)
	if ctx.Err() != nil {
		fmt.Fprintln(errOut, "interrupted")
		return 130
	}
	if result.errors > 0 || result.succeeded != result.completed {
		return 1
	}
	return 0
}

func (cfg config) validate() error {
	u, err := url.Parse(cfg.url)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("-url must be an absolute http or https URL")
	}
	if cfg.count <= 0 || cfg.concurrency <= 0 || cfg.timeout <= 0 {
		return errors.New("-count, -concurrency, and -timeout must be positive")
	}
	return nil
}

func benchmark(ctx context.Context, cfg config) (stats, error) {
	template, err := http.NewRequest(cfg.method, cfg.url, strings.NewReader(cfg.data))
	if err != nil {
		return stats{}, err
	}
	for _, header := range cfg.headers {
		name, value, ok := strings.Cut(header, ":")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return stats{}, fmt.Errorf("invalid header %q; expected 'Name: value'", header)
		}
		if strings.EqualFold(name, "Host") {
			template.Host = strings.TrimSpace(value)
		} else {
			template.Header.Add(name, strings.TrimSpace(value))
		}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.concurrency
	transport.MaxIdleConnsPerHost = cfg.concurrency
	transport.MaxConnsPerHost = cfg.concurrency
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.timeout,
		// Count the endpoint's response without issuing extra redirect requests.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	var next atomic.Int64
	results := make(chan stats, cfg.concurrency)
	started := time.Now()
	for i := 0; i < cfg.concurrency; i++ {
		go func() {
			local := stats{statuses: make(map[int]int64)}
			for ctx.Err() == nil && next.Add(1) <= cfg.count {
				request := template.Clone(ctx)
				if template.GetBody != nil {
					request.Body, _ = template.GetBody() // strings.Reader's GetBody never fails.
				}
				start := time.Now()
				response, requestErr := client.Do(request)
				if response != nil {
					local.statuses[response.StatusCode]++
					bytes, readErr := io.Copy(io.Discard, response.Body)
					response.Body.Close()
					local.bytes += bytes
					if requestErr == nil {
						requestErr = readErr
					}
					if requestErr == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
						local.succeeded++
					}
				}
				latency := time.Since(start)
				local.completed++
				local.total += latency
				if local.min == 0 || latency < local.min {
					local.min = latency
				}
				if latency > local.max {
					local.max = latency
				}
				if requestErr != nil {
					local.errors++
					if local.firstErr == nil {
						local.firstErr = requestErr
					}
				}
			}
			results <- local
		}()
	}

	combined := stats{statuses: make(map[int]int64)}
	for i := 0; i < cfg.concurrency; i++ {
		local := <-results
		combined.completed += local.completed
		combined.succeeded += local.succeeded
		combined.errors += local.errors
		combined.bytes += local.bytes
		combined.total += local.total
		if local.completed > 0 && (combined.min == 0 || local.min < combined.min) {
			combined.min = local.min
		}
		if local.max > combined.max {
			combined.max = local.max
		}
		if combined.firstErr == nil {
			combined.firstErr = local.firstErr
		}
		for code, count := range local.statuses {
			combined.statuses[code] += count
		}
	}
	combined.elapsed = time.Since(started)
	return combined, nil
}

func printStats(out io.Writer, cfg config, result stats) {
	average := time.Duration(0)
	if result.completed > 0 {
		average = result.total / time.Duration(result.completed)
	}
	fmt.Fprintf(out, "Completed: %d/%d\nSuccessful (2xx): %d\nRequest/read errors: %d\n", result.completed, cfg.count, result.succeeded, result.errors)
	fmt.Fprintf(out, "Elapsed: %s\nThroughput: %.2f requests/sec\n", result.elapsed.Round(time.Microsecond), float64(result.completed)/result.elapsed.Seconds())
	fmt.Fprintf(out, "Latency min/avg/max: %s / %s / %s\nResponse bytes: %d\n", result.min.Round(time.Microsecond), average.Round(time.Microsecond), result.max.Round(time.Microsecond), result.bytes)
	codes := make([]int, 0, len(result.statuses))
	for code := range result.statuses {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		fmt.Fprintf(out, "HTTP %d: %d\n", code, result.statuses[code])
	}
	if result.firstErr != nil {
		fmt.Fprintln(out, "First error:", result.firstErr)
	}
}
