package dbsync

import (
	"context"
	"errors"
	"log"
)

// mongoSource streams MongoDB change events. (Being implemented.)
type mongoSource struct {
	cfg   *SourceConfig
	rules []*rule
	log   *log.Logger
}

func newMongo(sc *SourceConfig, logger *log.Logger) (*mongoSource, error) {
	rules, err := compileRules(sc, sc.Database)
	if err != nil {
		return nil, err
	}
	return &mongoSource{cfg: sc, rules: rules, log: logger}, nil
}

func (s *mongoSource) name() string { return s.cfg.Name }

func (s *mongoSource) session(ctx context.Context, k *sink) error {
	return errors.New("mongodb source not implemented yet")
}
