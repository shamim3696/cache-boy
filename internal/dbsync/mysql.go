package dbsync

import (
	"context"
	"errors"
	"log"
)

// mySQLSource streams MySQL row changes from the binlog. (Being implemented.)
type mySQLSource struct {
	cfg   *SourceConfig
	rules []*rule
	log   *log.Logger
}

func newMySQL(sc *SourceConfig, logger *log.Logger) (*mySQLSource, error) {
	rules, err := compileRules(sc, "")
	if err != nil {
		return nil, err
	}
	return &mySQLSource{cfg: sc, rules: rules, log: logger}, nil
}

func (s *mySQLSource) name() string { return s.cfg.Name }

func (s *mySQLSource) session(ctx context.Context, k *sink) error {
	return errors.New("mysql source not implemented yet")
}
