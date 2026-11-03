package watch

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/server/common/log/tag"
)

func (s *Service) runPlugins(ctx context.Context) {
	defer close(s.pluginDone)
	defer func() {
		s.mu.Lock()
		s.pluginClosing = true
		s.mu.Unlock()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		err := errors.Join(s.plugins.close(shutdown), s.plugins.runtime.Close(shutdown))
		s.mu.Lock()
		s.pluginCloseErr = err
		s.mu.Unlock()
		if err != nil {
			s.params.Logger.Error("Watch plugin management close remains blocked", tag.Error(err))
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 10*time.Second)
		view, err := readPluginIntent(operation, s.params.ClusterPlugins)
		cancel()
		if err == nil {
			err = s.publishPluginIntent(view, time.Now())
		}
		s.mu.Lock()
		previous := s.pluginProblem
		s.pluginProblem = ""
		if err != nil {
			s.pluginProblem = err.Error()
		}
		changed := previous != s.pluginProblem
		s.mu.Unlock()
		if err != nil && changed && ctx.Err() == nil {
			s.params.Logger.Error("Watch cluster plugin intent is unavailable", tag.Error(err))
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func (s *Service) publishPluginIntent(view *pluginIntent, observedAt time.Time) error {
	s.mu.Lock()
	if err := view.follows(s.pluginView); err != nil {
		s.mu.Unlock()
		return err
	}
	if s.pluginView != nil && view.snapshot.Generation == s.pluginView.snapshot.Generation {
		view = s.pluginView
	}
	s.pluginView, s.pluginObservedAt = view, observedAt
	graphs := make([]*Graph, 0, len(s.namespaces))
	for _, hosted := range s.namespaces {
		if hosted.graph != nil {
			graphs = append(graphs, hosted.graph)
		}
	}
	s.mu.Unlock()
	if err := s.plugins.update(view); err != nil {
		return err
	}
	for _, graph := range graphs {
		if err := graph.updatePlugins(view, s.params.Logger); err != nil {
			return err
		}
	}
	return nil
}
