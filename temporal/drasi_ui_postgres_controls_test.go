//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
)

func drasiUIPostgresControlPolicy() watchservice.PluginPolicy {
	source, bootstrap := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	return watchservice.PluginPolicy{
		ManagedPlugins: []native.ManagedPluginPair{{
			Source:    native.PluginComponentRef{Digest: source, Component: "postgres"},
			Bootstrap: native.PluginComponentRef{Digest: bootstrap, Component: "postgres"},
		}},
		Catalogs: []native.CatalogConfig{{ID: "sql-controls-only", Plugins: []native.CatalogRelease{
			{Version: "1.0.0", Artifact: native.Plugin{ID: "fixture-source", Reference: "ghcr.io/drasi-project/fixture-source@" + source}},
			{Version: "1.0.0", Artifact: native.Plugin{ID: "fixture-bootstrap", Reference: "ghcr.io/drasi-project/fixture-bootstrap@" + bootstrap}},
		}}},
	}
}

func TestDrasiUICredentialReader(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		change   func(*drasiUICredentialReader)
		canceled bool
		code     codes.Code
	}{
		{name: "exact namespace and immutable reference"},
		{name: "different namespace", code: codes.PermissionDenied, change: func(reader *drasiUICredentialReader) {
			reader.namespace = namespace.ID("00000000-0000-4000-8000-000000000002")
		}},
		{name: "different provider", code: codes.PermissionDenied, change: func(reader *drasiUICredentialReader) {
			reader.reference.Provider = "other"
		}},
		{name: "different key", code: codes.PermissionDenied, change: func(reader *drasiUICredentialReader) {
			reader.reference.Key = "other"
		}},
		{name: "different version", code: codes.PermissionDenied, change: func(reader *drasiUICredentialReader) {
			reader.reference.Version = "2"
		}},
		{name: "canceled", canceled: true, code: codes.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id := namespace.ID("00000000-0000-4000-8000-000000000001")
			reference := native.SecretReference{Provider: "provider", Key: "key", Version: "1"}
			reader := drasiUICredentialReader{namespace: id, reference: reference, password: []byte("fixture-credential")}
			if test.change != nil {
				test.change(&reader)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				cancel()
			}
			value, err := reader.ReadSecret(ctx, id, reference)
			require.Equal(t, test.code, serviceerror.ToStatus(err).Code())
			if test.code != codes.OK {
				require.Nil(t, value)
				return
			}
			require.Equal(t, []byte("fixture-credential"), value)
			clear(value)
			again, err := reader.ReadSecret(ctx, id, reference)
			require.NoError(t, err)
			require.Equal(t, []byte("fixture-credential"), again)
		})
	}
}

func TestDrasiUIPostgresReviewedPins(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*watchservice.PluginPolicy)
		valid  bool
	}{
		{name: "explicit reviewed pair", valid: true},
		{name: "independent catalog entries are not approval", change: func(policy *watchservice.PluginPolicy) {
			policy.ManagedPlugins = nil
		}},
		{name: "missing bootstrap selection", change: func(policy *watchservice.PluginPolicy) {
			policy.Catalogs[0].Plugins = policy.Catalogs[0].Plugins[:1]
		}},
		{name: "ambiguous reviewed pair", change: func(policy *watchservice.PluginPolicy) {
			policy.ManagedPlugins = append(policy.ManagedPlugins, policy.ManagedPlugins[0])
		}},
		{name: "ambiguous catalog selection", change: func(policy *watchservice.PluginPolicy) {
			policy.Catalogs[0].Plugins = append(policy.Catalogs[0].Plugins, policy.Catalogs[0].Plugins[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy := drasiUIPostgresControlPolicy()
			if test.change != nil {
				test.change(&policy)
			}
			pins, err := drasiUIPostgresPins(policy)
			if !test.valid {
				require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
				require.Equal(t, drasiUIPostgresManifest{}, pins)
				return
			}
			require.NoError(t, err)
			require.Equal(t, drasiUIPostgresManifest{
				Source: drasiUIPluginComponent{
					CatalogID: "sql-controls-only", ID: "fixture-source", Version: "1.0.0",
					Digest: policy.ManagedPlugins[0].Source.Digest, Component: "postgres",
				},
				Bootstrap: drasiUIPluginComponent{
					CatalogID: "sql-controls-only", ID: "fixture-bootstrap", Version: "1.0.0",
					Digest: policy.ManagedPlugins[0].Bootstrap.Digest, Component: "postgres",
				},
			}, pins)
		})
	}
}

func TestDrasiUIPostgresControlBoundaries(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		path        string
		body        string
		contentType string
		status      int
	}{
		{name: "wrong content type", path: "prepare", body: `{}`, contentType: "text/plain", status: http.StatusUnsupportedMediaType},
		{name: "malformed JSON", path: "prepare", body: `{`, status: http.StatusBadRequest},
		{name: "unknown fields", path: "prepare", body: `{"runId":"10000000-0000-4000-8000-000000000001","sql":"DROP TABLE anything"}`, status: http.StatusBadRequest},
		{name: "trailing JSON", path: "prepare", body: `{} {}`, status: http.StatusBadRequest},
		{name: "oversized body", path: "prepare", body: `{"runId":"` + strings.Repeat("a", 1024) + `"}`, status: http.StatusBadRequest},
		{name: "missing run", path: "prepare", body: `{}`, status: http.StatusBadRequest},
		{name: "zero UUID", path: "prepare", body: `{"runId":"00000000-0000-0000-0000-000000000000"}`, status: http.StatusBadRequest},
		{name: "noncanonical UUID", path: "prepare", body: `{"runId":"10000000-0000-4000-8000-0000000000AB"}`, status: http.StatusBadRequest},
		{name: "unknown operation", path: "mutate", body: `{"runId":"10000000-0000-4000-8000-000000000001","operation":"arbitrary SQL"}`, status: http.StatusBadRequest},
		{name: "missing operation", path: "mutate", body: `{"runId":"10000000-0000-4000-8000-000000000001"}`, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := &drasiUIPostgres{}
			mux := http.NewServeMux()
			fixture.registerControls(mux, log.NewTestLogger())
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/postgres/"+test.path, strings.NewReader(test.body))
			contentType := test.contentType
			if contentType == "" {
				contentType = "application/json"
			}
			request.Header.Set("Content-Type", contentType)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code)
		})
	}
}

type drasiUIDroppedResponse struct {
	header http.Header
}

func (response *drasiUIDroppedResponse) Header() http.Header { return response.header }
func (*drasiUIDroppedResponse) WriteHeader(int)              {}
func (*drasiUIDroppedResponse) Write([]byte) (int, error)    { return 0, io.ErrClosedPipe }

func TestDrasiUIPostgresSQLControls(t *testing.T) {
	if os.Getenv("TEMPORAL_DRASI_UI_POSTGRES_SQL") == "" {
		t.Skip("set TEMPORAL_DRASI_UI_POSTGRES_SQL=1 to run real PostgreSQL control tests")
	}
	t.Parallel()
	require.Equal(t, "1", os.Getenv("TEMPORAL_DRASI_UI_POSTGRES_SQL"))
	ctx := testcontext.For(t, testcontext.WithTimeout(3*time.Minute))
	fixture := startDrasiUIPostgres(ctx, t, drasiUIPostgresControlPolicy())
	require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE drasi_ui_watch_mutations (run_id text NOT NULL, operation text NOT NULL);
CREATE FUNCTION drasi_ui_record_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO drasi_ui_watch_mutations VALUES (OLD.run_id, TG_OP);
        RETURN OLD;
    END IF;
    INSERT INTO drasi_ui_watch_mutations VALUES (NEW.run_id, TG_OP);
    RETURN NEW;
END;
$$;
CREATE TRIGGER drasi_ui_mutation AFTER INSERT OR UPDATE OR DELETE ON drasi_ui_watch_items
FOR EACH ROW EXECUTE FUNCTION drasi_ui_record_mutation();
`)
		return err
	}))
	mux := http.NewServeMux()
	fixture.registerControls(mux, log.NewTestLogger())
	post := func(t *testing.T, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/postgres/"+path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		return response
	}
	rows := func(t *testing.T, id uuid.UUID) []drasiUIPostgresRow {
		t.Helper()
		var result []drasiUIPostgresRow
		require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
			selected, err := tx.Query(ctx, "SELECT item_id, name, amount FROM drasi_ui_watch_items WHERE run_id = $1 ORDER BY item_id", id.String())
			if err != nil {
				return err
			}
			result, err = pgx.CollectRows(selected, func(row pgx.CollectableRow) (drasiUIPostgresRow, error) {
				var value drasiUIPostgresRow
				err := row.Scan(&value.ItemID, &value.Name, &value.Amount)
				return value, err
			})
			return err
		}))
		return result
	}
	mutations := func(t *testing.T, id uuid.UUID) map[string]int {
		t.Helper()
		result := make(map[string]int)
		require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
			selected, err := tx.Query(ctx, "SELECT operation, count(*) FROM drasi_ui_watch_mutations WHERE run_id = $1 GROUP BY operation", id.String())
			if err != nil {
				return err
			}
			defer selected.Close()
			for selected.Next() {
				var operation string
				var count int
				if err := selected.Scan(&operation, &count); err != nil {
					return err
				}
				result[operation] = count
			}
			return selected.Err()
		}))
		return result
	}
	t.Run("committed phases and old retries", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustParse(testvars.New(t).NamespaceID().String())
		prepareBody := fmt.Sprintf(`{"runId":%q}`, id.String())
		first := post(t, "prepare", prepareBody)
		require.Equal(t, http.StatusOK, first.Code)
		var prepared drasiUIPostgresPrepared
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &prepared))
		require.Equal(t, drasiUIPostgresRow{ItemID: id.String() + ":bootstrap", Name: "bootstrap", Amount: 12}, prepared.BootstrapRow)
		require.Equal(t, drasiUIPostgresRow{ItemID: id.String() + ":inserted", Name: "inserted", Amount: 18}, prepared.InsertRow)
		require.Equal(t, drasiUIPostgresRow{ItemID: id.String() + ":bootstrap", Name: "bootstrap", Amount: 24}, prepared.UpdatedRow)
		require.Equal(t, []drasiUIPostgresRow{prepared.BootstrapRow}, rows(t, id))
		manifest, err := json.Marshal(fixture.manifest)
		require.NoError(t, err)
		for _, value := range [][]byte{first.Body.Bytes(), manifest} {
			require.NotContains(t, string(value), string(fixture.password))
		}
		conflict := post(t, "mutate", fmt.Sprintf(`{"runId":%q,"operation":"update"}`, id.String()))
		require.Equal(t, http.StatusConflict, conflict.Code)
		require.Equal(t, []drasiUIPostgresRow{prepared.BootstrapRow}, rows(t, id))
		for _, test := range []struct {
			operation string
			rows      []drasiUIPostgresRow
		}{
			{"insert", []drasiUIPostgresRow{prepared.BootstrapRow, prepared.InsertRow}},
			{"update", []drasiUIPostgresRow{prepared.UpdatedRow, prepared.InsertRow}},
			{"delete", []drasiUIPostgresRow{prepared.UpdatedRow}},
		} {
			t.Run(test.operation, func(t *testing.T) {
				// These phases must commit in order on the same run.
				body := fmt.Sprintf(`{"runId":%q,"operation":%q}`, id.String(), test.operation)
				response := post(t, "mutate", body)
				require.Equal(t, http.StatusOK, response.Code)
				require.JSONEq(t, body, response.Body.String())
				require.Equal(t, test.rows, rows(t, id))
			})
		}
		expectedMutations := map[string]int{"INSERT": 2, "UPDATE": 1, "DELETE": 1}
		require.Equal(t, expectedMutations, mutations(t, id))
		for _, operation := range []string{"insert", "update", "delete"} {
			response := post(t, "mutate", fmt.Sprintf(`{"runId":%q,"operation":%q}`, id.String(), operation))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, []drasiUIPostgresRow{prepared.UpdatedRow}, rows(t, id))
		}
		repeated := post(t, "prepare", prepareBody)
		require.Equal(t, http.StatusOK, repeated.Code)
		require.JSONEq(t, first.Body.String(), repeated.Body.String())
		require.Equal(t, []drasiUIPostgresRow{prepared.UpdatedRow}, rows(t, id))
		require.Equal(t, expectedMutations, mutations(t, id), "retries must not emit new PostgreSQL changes")
	})
	t.Run("lost HTTP response does not repeat commit", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustParse(testvars.New(t).NamespaceID().String())
		prepared, err := fixture.prepare(ctx, id)
		require.NoError(t, err)
		body := fmt.Sprintf(`{"runId":%q,"operation":"insert"}`, id.String())
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/postgres/mutate", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(&drasiUIDroppedResponse{header: make(http.Header)}, request)
		response := post(t, "mutate", body)
		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, []drasiUIPostgresRow{prepared.BootstrapRow, prepared.InsertRow}, rows(t, id))
		require.Equal(t, map[string]int{"INSERT": 2}, mutations(t, id))
	})
	t.Run("concurrent retries share one run and mutation", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustParse(testvars.New(t).NamespaceID().String())
		for _, operation := range []func() error{
			func() error { _, err := fixture.prepare(ctx, id); return err },
			func() error { return fixture.mutate(ctx, id, drasiUIPostgresInsert) },
		} {
			started, completed := make(chan struct{}), make(chan error, 8)
			for range cap(completed) {
				go func() { <-started; completed <- operation() }()
			}
			close(started)
			var failures error
			for range cap(completed) {
				failures = errors.Join(failures, await.Rcv(t, completed))
			}
			require.NoError(t, failures)
		}
		prepared := fixture.prepared(id)
		require.Equal(t, []drasiUIPostgresRow{prepared.BootstrapRow, prepared.InsertRow}, rows(t, id))
		require.Equal(t, map[string]int{"INSERT": 2}, mutations(t, id))
	})
	t.Run("phase failure rolls back row change before retry", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustParse(testvars.New(t).NamespaceID().String())
		prepared, err := fixture.prepare(ctx, id)
		require.NoError(t, err)
		require.NoError(t, fixture.mutate(ctx, id, drasiUIPostgresInsert))
		constraint := pgx.Identifier{"drasi_ui_phase_" + strings.ReplaceAll(id.String(), "-", "")}.Sanitize()
		require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "ALTER TABLE drasi_ui_watch_runs ADD CONSTRAINT "+constraint+
				" CHECK (run_id <> '"+id.String()+"' OR phase < 2)")
			return err
		}))
		require.Error(t, fixture.mutate(ctx, id, drasiUIPostgresUpdate))
		require.Equal(t, []drasiUIPostgresRow{prepared.BootstrapRow, prepared.InsertRow}, rows(t, id))
		require.Equal(t, map[string]int{"INSERT": 2}, mutations(t, id), "uncommitted row changes and audit must both roll back")
		require.NoError(t, fixture.transaction(ctx, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "ALTER TABLE drasi_ui_watch_runs DROP CONSTRAINT "+constraint)
			return err
		}))
		require.NoError(t, fixture.mutate(ctx, id, drasiUIPostgresUpdate))
		require.Equal(t, []drasiUIPostgresRow{prepared.UpdatedRow, prepared.InsertRow}, rows(t, id))
		require.Equal(t, map[string]int{"INSERT": 2, "UPDATE": 1}, mutations(t, id))
	})
	t.Run("unknown run and canceled preparation", func(t *testing.T) {
		t.Parallel()
		id := uuid.MustParse(testvars.New(t).NamespaceID().String())
		response := post(t, "mutate", fmt.Sprintf(`{"runId":%q,"operation":"insert"}`, id.String()))
		require.Equal(t, http.StatusNotFound, response.Code)
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := fixture.prepare(canceled, id)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, rows(t, id))
	})
}
