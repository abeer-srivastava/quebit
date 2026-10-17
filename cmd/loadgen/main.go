// Command loadgen issues concurrent create-payment requests and reports
// throughput, status codes, and latency percentiles.
//
// It exists to make the numbers in the README reproducible:
//
//	go run ./cmd/loadgen -n 1000 -c 50
//	go run ./cmd/loadgen -n 100 -c 100 -same-key fixed-key   # idempotency race
//
// It has no dependencies beyond the standard library, and it deliberately does
// not use the payment status endpoint — it measures the write path only.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type createResponse struct {
	Payment struct {
		ID string `json:"id"`
	} `json:"payment"`
	Created bool `json:"created"`
}

type sample struct {
	code    int
	latency time.Duration
	id      string
	created bool
}

func main() {
	url := flag.String("url", "http://localhost:8080/v1/payments", "create endpoint")
	n := flag.Int("n", 1000, "total requests")
	c := flag.Int("c", 50, "concurrency")
	same := flag.String("same-key", "", "if set, all requests share this idempotency key")
	amount := flag.Int64("amount", 2500, "amount in minor units")
	currency := flag.String("currency", "USD", "currency code")
	flag.Parse()

	if *n <= 0 || *c <= 0 {
		fmt.Fprintln(os.Stderr, "n and c must be positive")
		os.Exit(2)
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			MaxConnsPerHost:     256,
		},
		Timeout: 15 * time.Second,
	}

	results := make([]sample, *n)
	var idx int
	var idxMu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idxMu.Lock()
				i := idx
				idx++
				idxMu.Unlock()
				if i >= *n {
					return
				}

				key := *same
				if key == "" {
					key = fmt.Sprintf("load-%d-%d", start.UnixNano(), i)
				}
				body := fmt.Sprintf(`{"idempotency_key":%q,"amount":%d,"currency":%q}`, key, *amount, *currency)

				req, err := http.NewRequest(http.MethodPost, *url, strings.NewReader(body))
				if err != nil {
					results[i] = sample{code: -1}
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				t0 := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(t0)

				s := sample{latency: lat}
				if err != nil {
					s.code = -1 // transport error
				} else {
					s.code = resp.StatusCode
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
					resp.Body.Close()
					var cr createResponse
					if json.Unmarshal(b, &cr) == nil {
						s.id = cr.Payment.ID
						s.created = cr.Created
					}
				}
				results[i] = s
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	byCode := make(map[int]int)
	ids := make(map[string]struct{})
	created := 0
	lats := make([]time.Duration, 0, *n)
	for _, s := range results {
		byCode[s.code]++
		if s.id != "" {
			ids[s.id] = struct{}{}
		}
		if s.created {
			created++
		}
		lats = append(lats, s.latency)
	}
	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	pct := func(q float64) time.Duration {
		return lats[int(q*float64(len(lats)-1))].Round(time.Microsecond)
	}

	fmt.Printf("requests:       %d\n", *n)
	fmt.Printf("concurrency:    %d\n", *c)
	fmt.Printf("elapsed:        %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:     %.0f req/s\n", float64(*n)/elapsed.Seconds())
	fmt.Printf("status codes:   %v\n", byCode)
	fmt.Printf("distinct ids:   %d\n", len(ids))
	fmt.Printf("created=true:   %d\n", created)
	fmt.Printf("latency p50:    %s\n", pct(0.50))
	fmt.Printf("latency p95:    %s\n", pct(0.95))
	fmt.Printf("latency p99:    %s\n", pct(0.99))
	fmt.Printf("latency max:    %s\n", lats[len(lats)-1].Round(time.Microsecond))

	if byCode[-1] > 0 {
		fmt.Fprintf(os.Stderr, "%d request(s) failed at the transport level\n", byCode[-1])
		os.Exit(1)
	}
}
