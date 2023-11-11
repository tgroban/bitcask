package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"

	"go.mills.io/bitcask"
)

type server struct {
	bind string
	db   bitcask.DB
}

func newServer(bind, path string) (*server, error) {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).WithField("path", path).Error("error opening database")
		return nil, err
	}

	return &server{
		bind: bind,
		db:   db,
	}, nil
}

func (s *server) handleSet(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]
	value := cmd.Args[2]

	if err := s.db.Put(key, value); err != nil {
		conn.WriteString(fmt.Sprintf("ERR: %s", err))
	}

	conn.WriteString("OK")
}

func (s *server) handleGet(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	value, err := s.db.Get(key)
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(value)
	}
}

func (s *server) handleKeys(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	pattern := string(cmd.Args[1])

	// Fast-track condition for improved speed
	if pattern == "*" {
		conn.WriteArray(s.db.Len())
		s.db.ForEach(func(key bitcask.Key) error {
			conn.WriteBulk(key)
			return nil
		})
		return
	}

	// Prefix handling
	if strings.Count(pattern, "*") == 1 && strings.HasSuffix(pattern, "*") {
		prefix := strings.ReplaceAll(pattern, "*", "")
		count := 0
		keys := make([][]byte, 0)
		s.db.Scan([]byte(prefix), func(key bitcask.Key) error {
			keys = append(keys, key)
			count++
			return nil
		})
		conn.WriteArray(count)
		for _, key := range keys {
			conn.WriteBulk(key)
		}
		return
	}

	// No results means empty array
	conn.WriteArray(0)
}

func (s *server) handleExists(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	if s.db.Has(key) {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
}

func (s *server) handleDel(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	if err := s.db.Delete(key); err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (s *server) Shutdown() (err error) {
	err = s.db.Close()
	return
}

func (s *server) Run() (err error) {
	redServer := redcon.NewServerNetwork("tcp", s.bind,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				s.handleSet(cmd, conn)
			case "get":
				s.handleGet(cmd, conn)
			case "keys":
				s.handleKeys(cmd, conn)
			case "exists":
				s.handleExists(cmd, conn)
			case "del":
				s.handleDel(cmd, conn)
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			}
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		s := <-signals
		log.Infof("Shutdown server on signal %s", s)
		redServer.Close()
	}()

	if err := redServer.ListenAndServe(); err == nil {
		return s.Shutdown()
	}
	return
}
