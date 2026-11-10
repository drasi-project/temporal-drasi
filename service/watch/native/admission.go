package native

import (
	"context"
	"encoding/binary"
	"maps"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// SecretResolver reads an exact immutable reference authorized by its caller's
// namespace. Returned bytes transfer to the runtime and are cleared even on error.
type SecretResolver func(context.Context, SecretReference) ([]byte, error)

const credentialPacketBytes = 1 << 20

type credentialPacket struct {
	data   []byte
	offset int
	bytes  int
}

func (packet *credentialPacket) blob(value []byte) error {
	if len(value) > len(packet.data)-packet.offset-4 {
		return &Error{Code: CodeInvalidArgument, Message: "plugin credential packet exceeds its byte limit"}
	}
	binary.LittleEndian.PutUint32(packet.data[packet.offset:], uint32(len(value)))
	packet.offset += 4
	packet.offset += copy(packet.data[packet.offset:], value)
	return nil
}

func (packet *credentialPacket) field(
	ctx context.Context,
	slot byte,
	field string,
	reference SecretReference,
	resolve SecretResolver,
	wipe func([]byte),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, err := resolve(ctx, reference)
	defer wipe(value)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return &Error{Code: CodeConflict, Message: "plugin credential read failed"}
	}
	if len(value) > 16<<10 || !utf8.Valid(value) || slices.Contains(value, byte(0)) {
		return &Error{Code: CodeInvalidArgument, Message: "plugin credential value is invalid"}
	}
	packet.bytes += len(value)
	if packet.bytes > 256<<10 {
		return &Error{Code: CodeInvalidArgument, Message: "plugin credential group byte capacity exceeded"}
	}
	if packet.offset >= len(packet.data) {
		return &Error{Code: CodeInvalidArgument, Message: "plugin credential packet exceeds its byte limit"}
	}
	packet.data[packet.offset] = slot
	packet.offset++
	for _, text := range []string{field, reference.Provider, reference.Key, reference.Version} {
		if err := packet.blob([]byte(text)); err != nil {
			return err
		}
	}
	return packet.blob(value)
}

func credentialText(value string, limit int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= limit &&
		utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}

func encodeAdmissionValues(
	ctx context.Context,
	recipe PluginSourceRecipe,
	resolve SecretResolver,
	target []byte,
	wipe func([]byte),
) (int, error) {
	if len(target) < 8 || len(target) > credentialPacketBytes {
		return 0, &Error{Code: CodeInvalidArgument, Message: "invalid plugin credential packet capacity"}
	}
	components := []ComponentRecipe{recipe.Source, recipe.Bootstrap}
	count := len(recipe.Source.Secrets) + len(recipe.Bootstrap.Secrets)
	for _, component := range components {
		if len(component.Secrets) > 64 {
			return 0, &Error{Code: CodeInvalidArgument, Message: "plugin credential field capacity exceeded"}
		}
		for field, reference := range component.Secrets {
			if !credentialText(field, 256) || !credentialText(reference.Provider, 128) ||
				!credentialText(reference.Key, 1024) || !credentialText(reference.Version, 1024) {
				return 0, &Error{Code: CodeInvalidArgument, Message: "plugin credential reference is invalid"}
			}
		}
	}
	if count != 0 && resolve == nil {
		return 0, &Error{Code: CodeConflict, Message: "plugin credential reader is not configured"}
	}
	copy(target, "TDC1")
	binary.LittleEndian.PutUint32(target[4:], uint32(count))
	packet := credentialPacket{data: target, offset: 8}
	for slot, component := range components {
		for _, field := range slices.Sorted(maps.Keys(component.Secrets)) {
			if err := packet.field(ctx, byte(slot), field, component.Secrets[field], resolve, wipe); err != nil {
				return 0, err
			}
		}
	}
	return packet.offset, nil
}
