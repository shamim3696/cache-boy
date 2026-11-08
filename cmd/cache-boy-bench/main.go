// Command cache-boy-bench measures throughput and latency, like redis-benchmark.
//
//	cache-boy-bench [-h host] [-p port] [-a password] [-c clients] [-n requests]
//	                [-P pipeline] [-d datasize] [-r keyspace] [-t tests] [--csv] [-q]
package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

type config struct {
	host     string
	port     int
	password string
	clients  int
	requests int
	pipeline int
	dataSize int
	keyspace int
	tests    []string
	csv      bool
	quiet    bool
}

var allTests = []string{"ping", "set", "get", "incr", "lpush", "rpop", "sadd", "hset", "zadd", "mset"}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	addr := net.JoinHostPort(cfg.host, strconv.Itoa(cfg.port))
	// Fail early with a clear message if the server is unreachable.
	if cn, err := dial(addr, cfg.password); err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to %s: %v\n", addr, err)
		os.Exit(1)
	} else {
		cn.Close()
	}
	if cfg.csv {
		fmt.Println(`"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"`)
	}
	for _, t := range cfg.tests {
		res, err := runTest(addr, cfg, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", strings.ToUpper(t), err)
			os.Exit(1)
		}
		report(cfg, res)
	}
}

func parseFlags(args []string) (*config, error) {
	fs := flag.NewFlagSet("cache-boy-bench", flag.ContinueOnError)
	c := &config{}
	var tests string
	fs.StringVar(&c.host, "h", "127.0.0.1", "server hostname")
	fs.IntVar(&c.port, "p", 6380, "server port")
	fs.StringVar(&c.password, "a", "", "password")
	fs.IntVar(&c.clients, "c", 50, "number of parallel connections")
	fs.IntVar(&c.requests, "n", 100000, "total number of requests per test")
	fs.IntVar(&c.pipeline, "P", 1, "pipeline <numreq> requests")
	fs.IntVar(&c.dataSize, "d", 3, "data size of SET/GET value in bytes")
	fs.IntVar(&c.keyspace, "r", 0, "use random keys in [0, keyspace) (0 = one fixed key)")
	fs.StringVar(&tests, "t", "", "comma-separated tests: "+strings.Join(allTests, ","))
	fs.BoolVar(&c.csv, "csv", false, "output in CSV format")
	fs.BoolVar(&c.quiet, "q", false, "quiet: only show requests/sec")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		return nil, err
	}
	if c.clients < 1 || c.requests < 1 || c.pipeline < 1 || c.dataSize < 0 || c.keyspace < 0 {
		return nil, errors.New("-c, -n and -P must be positive; -d and -r must not be negative")
	}
	c.tests = allTests
	if tests != "" {
		c.tests = nil
		for _, t := range strings.Split(tests, ",") {
			t = strings.ToLower(strings.TrimSpace(t))
			if t == "" {
				continue
			}
			if !slices.Contains(allTests, t) {
				return nil, fmt.Errorf("unknown test %q (available: %s)", t, strings.Join(allTests, ","))
			}
			c.tests = append(c.tests, t)
		}
	}
	return c, nil
}

// buildCommand builds the arguments of one request of a test.
func buildCommand(test string, cfg *config, rnd *rand.Rand, value string) []string {
	key := func(prefix string) string {
		if cfg.keyspace > 0 {
			return fmt.Sprintf("%s%012d", prefix, rnd.IntN(cfg.keyspace))
		}
		return prefix + "__rand_int__"
	}
	switch test {
	case "ping":
		return []string{"PING"}
	case "set":
		return []string{"SET", key("key:"), value}
	case "get":
		return []string{"GET", key("key:")}
	case "incr":
		return []string{"INCR", key("counter:")}
	case "lpush":
		return []string{"LPUSH", "mylist", value}
	case "rpop":
		return []string{"RPOP", "mylist"}
	case "sadd":
		return []string{"SADD", "myset", key("element:")}
	case "hset":
		return []string{"HSET", "myhash", key("element:"), value}
	case "zadd":
		return []string{"ZADD", "myzset", strconv.Itoa(rnd.IntN(1000000)), key("element:")}
	case "mset":
		args := []string{"MSET"}
		for range 10 {
			args = append(args, key("key:"), value)
		}
		return args
	}
	panic("unknown test " + test)
}

type result struct {
	test      string
	requests  int
	errors    int64
	firstErr  string
	elapsed   time.Duration
	latencies []time.Duration // one per request
	cfg       *config
}

type conn struct {
	nc net.Conn
	r  *resp.Reader
}

func (c *conn) Close() error { return c.nc.Close() }

func dial(addr, password string) (*conn, error) {
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c := &conn{nc: nc, r: resp.NewReader(nc)}
	if password != "" {
		if _, err := nc.Write(resp.AppendCommand(nil, []string{"AUTH", password})); err != nil {
			nc.Close()
			return nil, err
		}
		v, err := c.r.ReadValue()
		if err == nil && v.IsError() {
			err = errors.New(v.Str)
		}
		if err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

// runTest runs one test with cfg.clients connections. Requests are handed
// out in pipeline-sized batches; the latency of each request in a batch is
// the round-trip time of the batch, as in redis-benchmark.
func runTest(addr string, cfg *config, test string) (*result, error) {
	conns := make([]*conn, cfg.clients)
	for i := range conns {
		c, err := dial(addr, cfg.password)
		if err != nil {
			for _, x := range conns[:i] {
				x.Close()
			}
			return nil, err
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	value := strings.Repeat("x", cfg.dataSize)
	var remaining atomic.Int64
	remaining.Store(int64(cfg.requests))
	res := &result{test: test, requests: cfg.requests, cfg: cfg}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var netErr atomic.Pointer[error]

	start := time.Now()
	for i, c := range conns {
		wg.Add(1)
		go func(c *conn, seed uint64) {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(seed, uint64(time.Now().UnixNano())))
			lat := make([]time.Duration, 0, cfg.requests/cfg.clients+cfg.pipeline)
			var errs int64
			var firstErr string
			buf := make([]byte, 0, 4096)
			for {
				n := int(min(int64(cfg.pipeline), remaining.Add(-int64(cfg.pipeline))+int64(cfg.pipeline)))
				if n <= 0 {
					break
				}
				buf = buf[:0]
				for range n {
					buf = resp.AppendCommand(buf, buildCommand(test, cfg, rnd, value))
				}
				t0 := time.Now()
				if _, err := c.nc.Write(buf); err != nil {
					netErr.CompareAndSwap(nil, &err)
					break
				}
				ok := true
				for range n {
					v, err := c.r.ReadValue()
					if err != nil {
						netErr.CompareAndSwap(nil, &err)
						ok = false
						break
					}
					if v.IsError() {
						if errs == 0 {
							firstErr = v.Str
						}
						errs++
					}
				}
				if !ok {
					break
				}
				d := time.Since(t0)
				for range n {
					lat = append(lat, d)
				}
			}
			mu.Lock()
			res.latencies = append(res.latencies, lat...)
			res.errors += errs
			if res.firstErr == "" {
				res.firstErr = firstErr
			}
			mu.Unlock()
		}(c, uint64(i))
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	if p := netErr.Load(); p != nil {
		return nil, *p
	}
	slices.Sort(res.latencies)
	return res, nil
}

func (r *result) percentile(p float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	i := int(float64(len(r.latencies))*p/100+0.5) - 1
	i = max(0, min(i, len(r.latencies)-1))
	return r.latencies[i]
}

func (r *result) avg() time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range r.latencies {
		sum += d
	}
	return sum / time.Duration(len(r.latencies))
}

func msec(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e6, 'f', 3, 64) }

func report(cfg *config, r *result) {
	rps := float64(len(r.latencies)) / r.elapsed.Seconds()
	name := strings.ToUpper(r.test)
	if r.test == "mset" {
		name = "MSET (10 keys)"
	}
	switch {
	case cfg.csv:
		fmt.Printf("%q,\"%.2f\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n", name, rps,
			msec(r.avg()), msec(r.percentile(0)), msec(r.percentile(50)), msec(r.percentile(95)), msec(r.percentile(99)), msec(r.percentile(100)))
	case cfg.quiet:
		fmt.Printf("%s: %.2f requests per second, p50=%s msec\n", name, rps, msec(r.percentile(50)))
	default:
		fmt.Printf("====== %s ======\n", name)
		fmt.Printf("  %d requests completed in %.2f seconds\n", len(r.latencies), r.elapsed.Seconds())
		fmt.Printf("  %d parallel clients\n", cfg.clients)
		fmt.Printf("  %d bytes payload\n", cfg.dataSize)
		if cfg.pipeline > 1 {
			fmt.Printf("  pipeline: %d\n", cfg.pipeline)
		}
		fmt.Println()
		fmt.Printf("Summary:\n  throughput summary: %.2f requests per second\n", rps)
		fmt.Printf("  latency summary (msec):\n")
		fmt.Printf("  %9s %9s %9s %9s %9s %9s\n", "avg", "min", "p50", "p95", "p99", "max")
		fmt.Printf("  %9s %9s %9s %9s %9s %9s\n", msec(r.avg()), msec(r.percentile(0)), msec(r.percentile(50)),
			msec(r.percentile(95)), msec(r.percentile(99)), msec(r.percentile(100)))
		fmt.Println()
	}
	if r.errors > 0 {
		fmt.Fprintf(os.Stderr, "  %s: %d error replies, first: %s\n", name, r.errors, r.firstErr)
	}
}
