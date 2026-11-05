// Command cache-boy-cli is an interactive client, similar to redis-cli.
//
//	cache-boy-cli [-h host] [-p port] [-a password] [--user u] [-n namespace] [-3] [--raw] [cmd args...]
//
// With arguments, or with commands piped on stdin, it runs them and exits.
// Otherwise it starts a REPL. After SUBSCRIBE, PSUBSCRIBE, KEYWATCH or
// MONITOR it prints messages until Ctrl+C.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"

	"cache-boy/internal/protocol/resp"
)

type cli struct {
	host, password, user, ns string
	port                     int
	proto                    int
	raw                      bool

	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
	out  io.Writer
}

func main() {
	fs := flag.NewFlagSet("cache-boy-cli", flag.ContinueOnError)
	c := &cli{out: os.Stdout}
	fs.StringVar(&c.host, "h", "127.0.0.1", "server hostname")
	fs.IntVar(&c.port, "p", 6380, "server port")
	fs.StringVar(&c.password, "a", "", "password (AUTH)")
	fs.StringVar(&c.user, "user", "", "ACL user name (used with -a)")
	fs.StringVar(&c.ns, "n", "", "namespace (NAMESPACE USE)")
	resp3 := fs.Bool("3", false, "use RESP3 (HELLO 3)")
	fs.BoolVar(&c.raw, "raw", false, "raw output: no quotes or type hints")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cache-boy-cli [-h host] [-p port] [-a password] [--user u] [-n namespace] [-3] [--raw] [cmd args...]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2)
	}
	if env := os.Getenv("CACHEBOY_PASSWORD"); c.password == "" && env != "" {
		c.password = env
	}
	c.proto = 2
	if *resp3 {
		c.proto = 3
	}

	if err := c.connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to cache-boy at %s: %v\n", c.addr(), err)
		os.Exit(1)
	}
	defer c.conn.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr)
		os.Exit(0)
	}()

	if args := fs.Args(); len(args) > 0 {
		os.Exit(c.run(args))
	}
	if !stdinIsTerminal() {
		os.Exit(c.runLines(os.Stdin, false))
	}
	os.Exit(c.runLines(os.Stdin, true))
}

func (c *cli) addr() string { return net.JoinHostPort(c.host, strconv.Itoa(c.port)) }

// connect dials and runs HELLO/AUTH and NAMESPACE USE.
func (c *cli) connect() error {
	nc, err := net.DialTimeout("tcp", c.addr(), 5*time.Second)
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn, c.r, c.w = nc, resp.NewReader(nc), resp.NewWriter(nc, 2)

	var hs [][]string
	if c.proto == 3 {
		h := []string{"HELLO", "3"}
		if c.password != "" {
			user := c.user
			if user == "" {
				user = "default"
			}
			h = append(h, "AUTH", user, c.password)
		}
		hs = append(hs, h)
	} else if c.password != "" {
		if c.user != "" {
			hs = append(hs, []string{"AUTH", c.user, c.password})
		} else {
			hs = append(hs, []string{"AUTH", c.password})
		}
	}
	if c.ns != "" {
		hs = append(hs, []string{"NAMESPACE", "USE", c.ns})
	}
	for _, args := range hs {
		v, err := c.roundTrip(args)
		if err != nil {
			return err
		}
		if v.IsError() {
			if args[0] == "NAMESPACE" {
				fmt.Fprintf(os.Stderr, "NAMESPACE USE %s failed: %s\n", c.ns, v.Str)
				c.ns = ""
				continue
			}
			if args[0] == "AUTH" && strings.Contains(v.Str, "without any password configured") {
				fmt.Fprintln(os.Stderr, "Warning: AUTH failed: the server has no password set")
				continue
			}
			return errors.New(v.Str)
		}
	}
	return nil
}

func (c *cli) send(args []string) error {
	if err := c.w.WriteCommand(args...); err != nil {
		return err
	}
	return c.w.Flush()
}

// readReply returns the next reply, printing push messages (RESP3
// invalidations...) that arrive before it.
func (c *cli) readReply() (resp.Value, error) {
	for {
		v, err := c.r.ReadValue()
		if err != nil || v.Type != resp.TypePush {
			return v, err
		}
		c.print(v)
	}
}

func (c *cli) roundTrip(args []string) (resp.Value, error) {
	if err := c.send(args); err != nil {
		return resp.Value{}, err
	}
	return c.readReply()
}

func (c *cli) print(v resp.Value) {
	if c.raw {
		fmt.Fprintln(c.out, formatRaw(v))
	} else {
		fmt.Fprintln(c.out, formatReply(v))
	}
}

// streaming commands switch the connection to "print every push" mode.
var streaming = map[string]bool{"subscribe": true, "psubscribe": true, "ssubscribe": true, "keywatch": true, "monitor": true}

// run executes one command and prints the reply. Returns an exit code.
func (c *cli) run(args []string) int {
	name := strings.ToLower(args[0])
	v, err := c.roundTrip(args)
	if err != nil {
		// One reconnect attempt (server restarted, idle timeout...).
		if rerr := c.connect(); rerr != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		if v, err = c.roundTrip(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
	}
	c.print(v)
	if v.IsError() {
		return 0
	}
	switch {
	case name == "namespace" && len(args) == 3 && strings.EqualFold(args[1], "use"):
		c.ns = args[2]
		if strings.EqualFold(c.ns, "default") {
			c.ns = ""
		}
	case name == "reset":
		c.ns = ""
	case name == "hello" && len(args) > 1:
		if p, err := strconv.Atoi(args[1]); err == nil {
			c.proto = p
		}
	case streaming[name]:
		if !c.raw {
			fmt.Fprintln(c.out, "Reading messages... (press Ctrl-C to quit)")
		}
		for {
			v, err := c.r.ReadValue()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			c.print(v)
		}
	}
	return 0
}

func (c *cli) prompt() string {
	p := c.addr()
	if c.ns != "" {
		p += "[" + c.ns + "]"
	}
	return p + "> "
}

// runLines runs commands read line by line (the REPL when interactive).
func (c *cli) runLines(in io.Reader, interactive bool) int {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 64*1024), 512<<20)
	code := 0
	for {
		if interactive {
			fmt.Fprint(c.out, c.prompt())
		}
		if !sc.Scan() {
			if interactive {
				fmt.Fprintln(c.out)
			}
			return code
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		args, err := resp.SplitInline(line)
		if err != nil {
			fmt.Fprintln(c.out, "Invalid argument(s)")
			continue
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToLower(args[0]) {
		case "quit", "exit":
			return code
		case "help", "?":
			c.help(args[1:])
			continue
		case "clear":
			if interactive {
				fmt.Fprint(c.out, "\033[H\033[2J")
				continue
			}
		}
		if rc := c.run(args); rc != 0 {
			code = rc
		}
	}
}

// help prints general help or COMMAND DOCS for a command.
func (c *cli) help(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(c.out, "cache-boy-cli "+version)
		fmt.Fprintln(c.out, `Type a command and press Enter, e.g. SET greeting "hello world".`)
		fmt.Fprintln(c.out, `"help <command>" shows help for a command, "COMMAND LIST" lists all commands.`)
		fmt.Fprintln(c.out, `"quit" or "exit" leaves. Ctrl-C stops SUBSCRIBE / KEYWATCH / MONITOR.`)
		return
	}
	name := strings.Join(args, " ")
	v, err := c.roundTrip([]string{"COMMAND", "DOCS", args[0]})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	if v.IsError() {
		c.print(v)
		return
	}
	// {name: {summary, group, ...}} as a RESP3 map or a RESP2 flat array.
	if len(v.Elems) < 2 {
		fmt.Fprintf(c.out, "No help for '%s'\n", name)
		return
	}
	doc := v.Elems[1]
	fields := map[string]string{}
	var keys []string
	for i := 0; i+1 < len(doc.Elems); i += 2 {
		k := doc.Elems[i].String()
		fields[k] = formatRaw(doc.Elems[i+1])
		keys = append(keys, k)
	}
	fmt.Fprintf(c.out, "\n  %s\n", strings.ToUpper(v.Elems[0].String()))
	if s := fields["summary"]; s != "" {
		fmt.Fprintf(c.out, "  summary: %s\n", s)
	}
	if g := fields["group"]; g != "" {
		fmt.Fprintf(c.out, "  group: %s\n", g)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k != "summary" && k != "group" && fields[k] != "" && !strings.Contains(fields[k], "\n") {
			fmt.Fprintf(c.out, "  %s: %s\n", k, fields[k])
		}
	}
	fmt.Fprintln(c.out)
}

const version = "0.1.0"

func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return true
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
