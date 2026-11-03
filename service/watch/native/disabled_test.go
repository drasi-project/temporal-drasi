//go:build !temporal_with_drasi || !cgo

package native

import (
	"context"
	"errors"
	"testing"
)

func TestDisabledFeatureFailsExplicitly(t *testing.T) {
	t.Parallel()
	if Enabled() {
		t.Fatal("disabled build reports enabled")
	}
	if _, err := Open(t.Context(), Config{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("Open returned %v, expected ErrDisabled", err)
	}
	var runtime Runtime
	for name, operation := range map[string]func() error{
		"apply": func() error { _, err := runtime.Apply(context.Background(), DescriptorsCommand{}); return err },
		"plugin admission": func() error {
			called := false
			_, err := runtime.AdmitPluginSource(t.Context(), NodeDefinition{}, func(context.Context, SecretReference) ([]byte, error) {
				called = true
				return nil, nil
			})
			if called {
				return errors.New("disabled runtime read credentials")
			}
			return err
		},
		"poll":        func() error { _, err := runtime.Poll(context.Background(), 1); return err },
		"ack":         func() error { return runtime.Ack(context.Background(), Receipt{}) },
		"close":       func() error { return runtime.Close(context.Background()) },
		"descriptors": func() error { _, err := runtime.Descriptors(context.Background()); return err },
		"install":     func() error { _, err := runtime.Apply(context.Background(), InstallPluginCommand{}); return err },
		"load":        func() error { _, err := runtime.Apply(context.Background(), LoadPluginCommand{}); return err },
		"attempt":     func() error { _, err := runtime.Plugin(LoadPluginCommand{}); return err },
		"schema":      func() error { _, err := runtime.Plugin(ValidatePluginSourcesCommand{}); return err },
		"wait":        func() error { _, err := (&PluginAttempt{}).Wait(t.Context()); return err },
		"validate":    func() error { _, err := ValidateQuery(t.Context(), Cypher, "MATCH (n) RETURN n"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := operation(); !errors.Is(err, ErrDisabled) {
				t.Fatalf("disabled operation returned %v", err)
			}
		})
	}
}
