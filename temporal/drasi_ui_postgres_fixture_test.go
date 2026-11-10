//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/await"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
)

type drasiUIPluginComponent struct {
	CatalogID     string                            `json:"catalogId"`
	ID            string                            `json:"id"`
	Version       string                            `json:"version"`
	Digest        string                            `json:"digest"`
	Component     string                            `json:"component"`
	Configuration map[string]any                    `json:"configuration"`
	Secrets       map[string]native.SecretReference `json:"secrets"`
}

type drasiUIPostgresManifest struct {
	Source    drasiUIPluginComponent `json:"source"`
	Bootstrap drasiUIPluginComponent `json:"bootstrap"`
}

type drasiUIPostgresRow struct {
	ItemID string `json:"item_id"`
	Name   string `json:"name"`
	Amount int    `json:"amount"`
}

type drasiUIPostgresPrepared struct {
	RunID        string                 `json:"runId"`
	Source       drasiUIPluginComponent `json:"source"`
	Bootstrap    drasiUIPluginComponent `json:"bootstrap"`
	QueryText    string                 `json:"queryText"`
	BootstrapRow drasiUIPostgresRow     `json:"bootstrapRow"`
	InsertRow    drasiUIPostgresRow     `json:"insertRow"`
	UpdatedRow   drasiUIPostgresRow     `json:"updatedRow"`
}

type drasiUIPostgresMutation int

const (
	drasiUIPostgresInsert drasiUIPostgresMutation = iota + 1
	drasiUIPostgresUpdate
	drasiUIPostgresDelete
)

type drasiUIPostgres struct {
	manifest  drasiUIPostgresManifest
	config    *pgx.ConnConfig
	password  []byte
	reference native.SecretReference
	retain    bool
}

type drasiUICredentialReader struct {
	namespace namespace.ID
	reference native.SecretReference
	password  []byte
}

func (reader drasiUICredentialReader) ReadSecret(ctx context.Context, id namespace.ID, reference native.SecretReference) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != reader.namespace || reference != reader.reference {
		return nil, serviceerror.NewPermissionDenied("UI fixture credential reference is not authorized", "")
	}
	return bytes.Clone(reader.password), nil
}

func drasiUIPostgresPins(policy watchservice.PluginPolicy) (drasiUIPostgresManifest, error) {
	var selected *native.ManagedPluginPair
	for _, pair := range policy.ManagedPlugins {
		if pair.Source.Component != "postgres" || pair.Bootstrap.Component != "postgres" {
			continue
		}
		if selected != nil {
			return drasiUIPostgresManifest{}, serviceerror.NewFailedPrecondition("UI PostgreSQL fixture requires exactly one reviewed PostgreSQL source and bootstrap pair")
		}
		selected = &pair
	}
	if selected == nil {
		return drasiUIPostgresManifest{}, serviceerror.NewFailedPrecondition("UI PostgreSQL fixture requires an explicitly reviewed PostgreSQL source and bootstrap pair")
	}
	component := func(pin native.PluginComponentRef) (drasiUIPluginComponent, error) {
		var result drasiUIPluginComponent
		found := false
		for _, catalog := range policy.Catalogs {
			for _, release := range catalog.Plugins {
				_, digest, ok := strings.Cut(release.Artifact.Reference, "@")
				if !ok || digest != pin.Digest {
					continue
				}
				if found {
					return drasiUIPluginComponent{}, serviceerror.NewFailedPrecondition("UI PostgreSQL fixture requires an unambiguous immutable catalog selection")
				}
				found = true
				result = drasiUIPluginComponent{
					CatalogID: catalog.ID, ID: release.Artifact.ID, Version: release.Version,
					Digest: pin.Digest, Component: pin.Component,
				}
			}
		}
		if !found {
			return drasiUIPluginComponent{}, serviceerror.NewFailedPrecondition("reviewed UI PostgreSQL fixture component is absent from the supplied catalog")
		}
		return result, nil
	}
	source, err := component(selected.Source)
	if err != nil {
		return drasiUIPostgresManifest{}, err
	}
	bootstrap, err := component(selected.Bootstrap)
	if err != nil {
		return drasiUIPostgresManifest{}, err
	}
	return drasiUIPostgresManifest{Source: source, Bootstrap: bootstrap}, nil
}

func startDrasiUIPostgres(ctx context.Context, t *testing.T, policy watchservice.PluginPolicy) *drasiUIPostgres {
	t.Helper()
	pins, err := drasiUIPostgresPins(policy)
	require.NoError(t, err)
	password := []byte(uuid.NewString())
	fixture := &drasiUIPostgres{
		manifest: pins, password: password,
		reference: native.SecretReference{Provider: "drasi-ui-fixture", Key: "postgres-password", Version: "1"},
	}
	start, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	name := "temporal-drasi-ui-postgres-" + uuid.NewString()
	container := ""
	t.Cleanup(func() {
		if fixture.retain {
			t.Errorf("PostgreSQL container %s retained because fixture services did not stop safely", name)
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if container == "" {
			owned, err := exec.CommandContext(cleanup, "docker", "ps", "--all",
				"--filter", "name=^/"+name+"$", "--filter", "label=temporal.drasi.verification=ui-postgres",
				"--format", "{{.ID}}").Output()
			require.NoError(t, err)
			if len(bytes.TrimSpace(owned)) == 0 {
				clear(password)
				return
			}
		}
		require.NoError(t, exec.CommandContext(cleanup, "docker", "rm", "--force", name).Run())
		clear(password)
		if fixture.config != nil {
			fixture.config.Password = ""
		}
	})
	create := exec.CommandContext(start, "docker", "create",
		"--name", name,
		"--label", "temporal.drasi.verification=ui-postgres",
		"--publish", "127.0.0.1::5432", "--env", "POSTGRES_DB=watchfixture", "--env", "POSTGRES_PASSWORD",
		"postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b",
		"-c", "wal_level=logical", "-c", "max_replication_slots=64", "-c", "max_wal_senders=64",
	)
	create.Env = append(os.Environ(), "POSTGRES_PASSWORD="+string(password))
	created, err := create.Output()
	require.NoError(t, err, "creating the owned PostgreSQL UI fixture failed")
	container = strings.TrimSpace(string(created))
	require.Regexp(t, "^[0-9a-f]{64}$", container)
	require.NoError(t, exec.CommandContext(start, "docker", "start", container).Run())
	ports, err := exec.CommandContext(start, "docker", "port", container, "5432/tcp").Output()
	require.NoError(t, err)
	host, portText, err := net.SplitHostPort(strings.TrimSpace(string(ports)))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host)
	port, err := strconv.ParseUint(portText, 10, 16)
	require.NoError(t, err)
	fixture.config, err = pgx.ParseConfig(fmt.Sprintf(
		"host=127.0.0.1 port=%d dbname=watchfixture user=postgres sslmode=disable connect_timeout=3", port,
	))
	require.NoError(t, err)
	fixture.config.Password = string(password)
	fixture.config.RuntimeParams["statement_timeout"] = "10000"
	await.Require(ctx, t, func(t *await.T) {
		require.NoError(t, fixture.transaction(t.Context(), func(tx pgx.Tx) error {
			_, err := tx.Exec(t.Context(), "SELECT 1")
			return err
		}))
	}, time.Minute, 100*time.Millisecond)
	require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE drasi_ui_watch_runs (
    run_id text PRIMARY KEY,
    phase smallint NOT NULL DEFAULT 0 CHECK (phase BETWEEN 0 AND 3)
);
CREATE TABLE drasi_ui_watch_items (
    item_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES drasi_ui_watch_runs (run_id),
    name text NOT NULL,
    amount integer NOT NULL
);
ALTER TABLE drasi_ui_watch_items REPLICA IDENTITY FULL;
`)
		return err
	}))
	for _, component := range []*drasiUIPluginComponent{&fixture.manifest.Source, &fixture.manifest.Bootstrap} {
		component.Configuration = map[string]any{
			"host": "127.0.0.1", "port": port, "database": "watchfixture", "user": "postgres",
			"tables": []string{"drasi_ui_watch_items"}, "sslMode": "disable",
		}
		component.Secrets = map[string]native.SecretReference{"password": fixture.reference}
	}
	return fixture
}

func (fixture *drasiUIPostgres) transaction(ctx context.Context, operation func(pgx.Tx) error) error {
	connection, err := pgx.ConnectConfig(ctx, fixture.config)
	if err != nil {
		return err
	}
	err = pgx.BeginFunc(ctx, connection, operation)
	closeContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return errors.Join(err, connection.Close(closeContext))
}

func (fixture *drasiUIPostgres) prepared(runID uuid.UUID) drasiUIPostgresPrepared {
	id := runID.String()
	component := func(template drasiUIPluginComponent) drasiUIPluginComponent {
		template.Configuration = maps.Clone(template.Configuration)
		template.Configuration["slotName"] = "drasi_ui_slot_" + strings.ReplaceAll(id, "-", "")
		template.Configuration["publicationName"] = "drasi_ui_pub_" + strings.ReplaceAll(id, "-", "")
		template.Secrets = maps.Clone(template.Secrets)
		return template
	}
	return drasiUIPostgresPrepared{
		RunID: id, Source: component(fixture.manifest.Source), Bootstrap: component(fixture.manifest.Bootstrap),
		QueryText:    fmt.Sprintf("MATCH (n:drasi_ui_watch_items) WHERE n.run_id = '%s' AND n.amount >= 10 RETURN n.item_id AS item_id, n.name AS name, n.amount AS amount", id),
		BootstrapRow: drasiUIPostgresRow{ItemID: id + ":bootstrap", Name: "bootstrap", Amount: 12},
		InsertRow:    drasiUIPostgresRow{ItemID: id + ":inserted", Name: "inserted", Amount: 18},
		UpdatedRow:   drasiUIPostgresRow{ItemID: id + ":bootstrap", Name: "bootstrap", Amount: 24},
	}
}

func (fixture *drasiUIPostgres) prepare(ctx context.Context, runID uuid.UUID) (drasiUIPostgresPrepared, error) {
	prepared := fixture.prepared(runID)
	err := fixture.transaction(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "LOCK TABLE drasi_ui_watch_runs IN SHARE ROW EXCLUSIVE MODE"); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM drasi_ui_watch_runs WHERE run_id = $1)", prepared.RunID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		var count int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM drasi_ui_watch_runs").Scan(&count); err != nil {
			return err
		}
		if count >= 32 {
			return serviceerror.NewFailedPrecondition("UI PostgreSQL fixture run limit exceeded")
		}
		if _, err := tx.Exec(ctx, "INSERT INTO drasi_ui_watch_runs (run_id) VALUES ($1)", prepared.RunID); err != nil {
			return err
		}
		publication := prepared.Source.Configuration["publicationName"].(string)
		if _, err := tx.Exec(ctx, "CREATE PUBLICATION "+pgx.Identifier{publication}.Sanitize()+" FOR TABLE drasi_ui_watch_items"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO drasi_ui_watch_items (item_id, run_id, name, amount) VALUES ($1, $2, $3, $4)",
			prepared.BootstrapRow.ItemID, prepared.RunID, prepared.BootstrapRow.Name, prepared.BootstrapRow.Amount)
		return err
	})
	if err != nil {
		return drasiUIPostgresPrepared{}, err
	}
	return prepared, nil
}

func (fixture *drasiUIPostgres) mutate(ctx context.Context, runID uuid.UUID, operation drasiUIPostgresMutation) error {
	prepared := fixture.prepared(runID)
	return fixture.transaction(ctx, func(tx pgx.Tx) error {
		var phase int
		if err := tx.QueryRow(ctx, "SELECT phase FROM drasi_ui_watch_runs WHERE run_id = $1 FOR UPDATE", prepared.RunID).Scan(&phase); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return serviceerror.NewNotFound("UI PostgreSQL fixture run has not been prepared")
			}
			return err
		}
		if phase >= int(operation) {
			return nil
		}
		if phase+1 != int(operation) {
			return serviceerror.NewFailedPrecondition("UI PostgreSQL fixture mutations must run in insert, update, delete order")
		}
		var affected int64
		switch operation {
		case drasiUIPostgresInsert:
			result, err := tx.Exec(ctx, "INSERT INTO drasi_ui_watch_items (item_id, run_id, name, amount) VALUES ($1, $2, $3, $4)",
				prepared.InsertRow.ItemID, prepared.RunID, prepared.InsertRow.Name, prepared.InsertRow.Amount)
			if err != nil {
				return err
			}
			affected = result.RowsAffected()
		case drasiUIPostgresUpdate:
			result, err := tx.Exec(ctx, "UPDATE drasi_ui_watch_items SET amount = $1 WHERE run_id = $2 AND item_id = $3 AND amount = $4",
				prepared.UpdatedRow.Amount, prepared.RunID, prepared.UpdatedRow.ItemID, prepared.BootstrapRow.Amount)
			if err != nil {
				return err
			}
			affected = result.RowsAffected()
		case drasiUIPostgresDelete:
			result, err := tx.Exec(ctx, "DELETE FROM drasi_ui_watch_items WHERE run_id = $1 AND item_id = $2", prepared.RunID, prepared.InsertRow.ItemID)
			if err != nil {
				return err
			}
			affected = result.RowsAffected()
		default:
			softassert.Fail(log.NewTestLogger(), "Unexpected UI PostgreSQL fixture mutation")
			return serviceerror.NewInternal("UI PostgreSQL fixture mutation is invalid")
		}
		if affected != 1 {
			return serviceerror.NewDataLoss("UI PostgreSQL fixture mutation did not change its one expected row")
		}
		_, err := tx.Exec(ctx, "UPDATE drasi_ui_watch_runs SET phase = $1 WHERE run_id = $2", int(operation), prepared.RunID)
		return err
	})
}

func (fixture *drasiUIPostgres) registerControls(mux *http.ServeMux, logger log.Logger) {
	mux.HandleFunc("POST /postgres/prepare", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RunID string `json:"runId"`
		}
		if !decodeDrasiUIPostgresRequest(w, r, &request) {
			return
		}
		runID, ok := drasiUIPostgresRunID(w, request.RunID)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		prepared, err := fixture.prepare(ctx, runID)
		if err != nil {
			writeDrasiUIPostgresError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(prepared); err != nil {
			logger.Error("Writing UI PostgreSQL fixture preparation failed", tag.Error(err))
		}
	})
	mux.HandleFunc("POST /postgres/mutate", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RunID     string `json:"runId"`
			Operation string `json:"operation"`
		}
		if !decodeDrasiUIPostgresRequest(w, r, &request) {
			return
		}
		runID, ok := drasiUIPostgresRunID(w, request.RunID)
		if !ok {
			return
		}
		var operation drasiUIPostgresMutation
		switch request.Operation {
		case "insert":
			operation = drasiUIPostgresInsert
		case "update":
			operation = drasiUIPostgresUpdate
		case "delete":
			operation = drasiUIPostgresDelete
		default:
			http.Error(w, "UI PostgreSQL fixture requires insert, update or delete", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := fixture.mutate(ctx, runID, operation); err != nil {
			writeDrasiUIPostgresError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(request); err != nil {
			logger.Error("Writing UI PostgreSQL fixture mutation failed", tag.Error(err))
		}
	})
}

func decodeDrasiUIPostgresRequest(w http.ResponseWriter, r *http.Request, request any) bool {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		http.Error(w, "UI PostgreSQL fixture requires application/json", http.StatusUnsupportedMediaType)
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(request); err != nil {
		http.Error(w, "invalid UI PostgreSQL fixture request", http.StatusBadRequest)
		return false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		http.Error(w, "one UI PostgreSQL fixture request is required", http.StatusBadRequest)
		return false
	}
	return true
}

func drasiUIPostgresRunID(w http.ResponseWriter, value string) (uuid.UUID, bool) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != value {
		http.Error(w, "UI PostgreSQL fixture requires a canonical nonzero run UUID", http.StatusBadRequest)
		return uuid.Nil, false
	}
	return id, true
}

func writeDrasiUIPostgresError(w http.ResponseWriter, err error) {
	var status int
	switch serviceerror.ToStatus(err).Code() {
	case codes.NotFound:
		status = http.StatusNotFound
	case codes.FailedPrecondition:
		status = http.StatusConflict
	default:
		status = http.StatusBadGateway
	}
	http.Error(w, "UI PostgreSQL fixture operation failed", status)
}
