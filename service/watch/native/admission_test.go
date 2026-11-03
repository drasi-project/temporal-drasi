package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func clearCredentialTest(value []byte) { clear(value) }

func TestCredentialPacketEncodingAndOwnedValueClearing(t *testing.T) {
	t.Parallel()
	value := []byte("opaque-test-value")
	reference := SecretReference{Provider: "p", Key: "k", Version: "v"}
	recipe := PluginSourceRecipe{Source: ComponentRecipe{Secrets: map[string]SecretReference{"f": reference}}}
	target := make([]byte, credentialPacketBytes)
	length, err := encodeAdmissionValues(t.Context(), recipe,
		func(_ context.Context, actual SecretReference) ([]byte, error) {
			require.Equal(t, reference, actual)
			return value, nil
		}, target, clearCredentialTest)
	require.NoError(t, err)
	require.Equal(t, []byte(
		"TDC1\x01\x00\x00\x00\x00"+
			"\x01\x00\x00\x00f\x01\x00\x00\x00p"+
			"\x01\x00\x00\x00k\x01\x00\x00\x00v"+
			"\x11\x00\x00\x00opaque-test-value"), target[:length])
	require.Equal(t, make([]byte, len(value)), value)
}

func TestCredentialPacketFailuresClearProviderBytesAndRedactErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		value    []byte
		failure  bool
		cancel   bool
		capacity int
	}{
		{name: "partial provider failure", value: []byte("opaque-test-value"), failure: true},
		{name: "cancelled provider", value: []byte("opaque-test-value"), cancel: true},
		{name: "invalid UTF-8", value: []byte{0xff}},
		{name: "embedded NUL", value: []byte{'a', 0, 'b'}},
		{name: "oversized value", value: bytes.Repeat([]byte{'a'}, (16<<10)+1)},
		{name: "short packet", value: []byte("opaque-test-value"), capacity: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reference := SecretReference{Provider: "p", Key: "k", Version: "v"}
			recipe := PluginSourceRecipe{Source: ComponentRecipe{Secrets: map[string]SecretReference{"f": reference}}}
			capacity := tc.capacity
			if capacity == 0 {
				capacity = credentialPacketBytes
			}
			_, err := encodeAdmissionValues(ctx, recipe,
				func(context.Context, SecretReference) ([]byte, error) {
					if tc.cancel {
						cancel()
					}
					if tc.failure {
						return tc.value, errors.New("opaque-test-value")
					}
					return tc.value, nil
				}, make([]byte, capacity), clearCredentialTest)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "opaque-test-value")
			require.Equal(t, make([]byte, len(tc.value)), tc.value)
		})
	}
}

func TestCredentialPacketReferenceValidationPrecedesReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		field     string
		reference SecretReference
	}{
		{"missing version", "f", SecretReference{Provider: "p", Key: "k"}},
		{"empty provider", "f", SecretReference{Key: "k", Version: "v"}},
		{"invalid field", "\n", SecretReference{Provider: "p", Key: "k", Version: "v"}},
		{"oversized key", "f", SecretReference{Provider: "p", Key: strings.Repeat("k", 1025), Version: "v"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recipe := PluginSourceRecipe{Source: ComponentRecipe{Secrets: map[string]SecretReference{tc.field: tc.reference}}}
			_, err := encodeAdmissionValues(t.Context(), recipe,
				func(context.Context, SecretReference) ([]byte, error) {
					t.Fatal("invalid reference reached provider")
					return nil, nil
				}, make([]byte, credentialPacketBytes), clearCredentialTest)
			require.Error(t, err)
		})
	}
}

func TestCredentialPacketComponentAudiencesAndGroupLimit(t *testing.T) {
	t.Parallel()
	reference := SecretReference{Provider: "p", Key: "k", Version: "v"}
	component := ComponentRecipe{Secrets: map[string]SecretReference{"f": reference}}
	recipe := PluginSourceRecipe{Source: component, Bootstrap: component}
	target := make([]byte, credentialPacketBytes)
	length, err := encodeAdmissionValues(t.Context(), recipe,
		func(context.Context, SecretReference) ([]byte, error) { return []byte{'x'}, nil }, target, clearCredentialTest)
	require.NoError(t, err)
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(target[4:]))
	require.Equal(t, byte(0), target[8])
	require.Equal(t, byte(1), target[8+(length-8)/2])

	for _, count := range []int{16, 17, 65} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			t.Parallel()
			secrets := make(map[string]SecretReference)
			for index := range count {
				secrets[fmt.Sprintf("field-%02d", index)] = reference
			}
			var values [][]byte
			_, err := encodeAdmissionValues(t.Context(), PluginSourceRecipe{Source: ComponentRecipe{Secrets: secrets}},
				func(context.Context, SecretReference) ([]byte, error) {
					value := bytes.Repeat([]byte{'x'}, 16<<10)
					values = append(values, value)
					return value, nil
				}, make([]byte, credentialPacketBytes), clearCredentialTest)
			if count == 16 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if count == 65 {
				require.Empty(t, values)
			} else {
				require.Len(t, values, count)
			}
			for _, value := range values {
				require.Equal(t, make([]byte, len(value)), value)
			}
		})
	}
}
