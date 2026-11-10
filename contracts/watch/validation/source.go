package validation

import (
	"maps"
	"math"
	"slices"
	"strings"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// ValidateSourceDefinition checks credential identities and literal configuration
// for a fresh Source definition. Servers call it after idempotency receipt lookup.
// It does not validate descriptor schemas, provider access, or runtime readiness.
func ValidateSourceDefinition(spec *watchpb.SourceSpec) error {
	if err := source(spec); err != nil {
		return err
	}
	for _, component := range []struct {
		field string
		value *watchpb.ComponentConfiguration
	}{
		{"spec.external", spec.GetExternal()},
		{"spec.bootstrap", spec.Bootstrap},
	} {
		if component.value == nil {
			continue
		}
		for _, name := range slices.Sorted(maps.Keys(component.value.Secrets)) {
			secret := component.value.Secrets[name]
			if err := text(component.field+".secrets.version", secret.Version, 1024); err != nil {
				return err
			}
			if _, present := component.value.Configuration.GetFields()[name]; present {
				return invalid(component.field+".configuration", "must not contain fields also supplied as secret references")
			}
		}
		if configuration := component.value.Configuration; configuration != nil {
			if err := literalObject(component.field+".configuration", configuration, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func literalObject(field string, object *structpb.Struct, depth int) error {
	if value := object.Fields["kind"]; value != nil {
		if kind, ok := value.Kind.(*structpb.Value_StringValue); ok && kind != nil &&
			(kind.StringValue == "Secret" || kind.StringValue == "EnvironmentVariable") {
			return invalid(field, "must not contain SDK configuration references")
		}
	}
	for _, name := range slices.Sorted(maps.Keys(object.Fields)) {
		if err := literalValue(field, object.Fields[name], depth+1); err != nil {
			return err
		}
	}
	return nil
}

func literalList(field string, list *structpb.ListValue, depth int) error {
	for _, element := range list.Values {
		if err := literalValue(field, element, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func literalValue(field string, value *structpb.Value, depth int) error {
	const invalidJSON = "must contain valid JSON values"
	if depth > 32 {
		return invalid(field, "must not exceed 32 levels of nesting")
	}
	if value == nil {
		return invalid(field, invalidJSON)
	}
	switch kind := value.Kind.(type) {
	case *structpb.Value_StringValue:
		if kind == nil {
			return invalid(field, invalidJSON)
		}
		if strings.HasPrefix(kind.StringValue, "${") && strings.HasSuffix(kind.StringValue, "}") {
			return invalid(field, "must not contain SDK configuration references")
		}
	case *structpb.Value_NumberValue:
		if kind == nil || math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) {
			return invalid(field, invalidJSON)
		}
	case *structpb.Value_BoolValue:
		if kind == nil {
			return invalid(field, invalidJSON)
		}
	case *structpb.Value_NullValue:
		if kind == nil || kind.NullValue != structpb.NullValue_NULL_VALUE {
			return invalid(field, invalidJSON)
		}
	case *structpb.Value_StructValue:
		if kind == nil || kind.StructValue == nil {
			return invalid(field, invalidJSON)
		}
		return literalObject(field, kind.StructValue, depth)
	case *structpb.Value_ListValue:
		if kind == nil || kind.ListValue == nil {
			return invalid(field, invalidJSON)
		}
		return literalList(field, kind.ListValue, depth)
	default:
		return invalid(field, invalidJSON)
	}
	return nil
}
