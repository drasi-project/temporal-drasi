package validation_test

import (
	"math"
	"strings"
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func externalDefinition() *watchpb.SourceSpec {
	return &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_External{External: &watchpb.ComponentConfiguration{
		Plugin: &watchpb.PluginComponentRef{Digest: artifactDigest(), Component: "fixture-source"},
		Secrets: map[string]*watchpb.SecretReference{
			"connection": {Provider: "credentials", Key: "reader", Version: "generation-17"},
		},
	}}}
}

func TestSourceDefinitionCredentialVersions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		version string
		valid   bool
	}{
		{"missing", "", false},
		{"blank", " ", false},
		{"control", "generation\n17", false},
		{"opaque", "generation/17:rotation", true},
		{"maximum", strings.Repeat("x", 1024), true},
		{"oversized", strings.Repeat("x", 1025), false},
	} {
		for _, bootstrap := range []bool{false, true} {
			name := "source/"
			if bootstrap {
				name = "bootstrap/"
			}
			t.Run(name+tc.name, func(t *testing.T) {
				t.Parallel()
				spec := externalDefinition()
				component := spec.GetExternal()
				if bootstrap {
					spec.Bootstrap = proto.Clone(component).(*watchpb.ComponentConfiguration)
					component = spec.Bootstrap
				}
				component.Secrets["connection"].Version = tc.version
				err := validation.ValidateSourceDefinition(spec)
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.Equal(t, codes.InvalidArgument, status.Code(err))
					require.ErrorContains(t, err, "secrets.version")
				}
			})
		}
	}
}

func TestSourceDefinitionLiteralConfiguration(t *testing.T) {
	t.Parallel()
	object := func(fields map[string]*structpb.Value) *structpb.Value {
		return structpb.NewStructValue(&structpb.Struct{Fields: fields})
	}
	for _, tc := range []struct {
		name  string
		value *structpb.Value
		valid bool
	}{
		{"literal", structpb.NewStringValue("literal"), true},
		{"embedded template", structpb.NewStringValue("prefix${HOST}suffix"), true},
		{"template with prefix", structpb.NewStringValue(" ${HOST}"), true},
		{"number", structpb.NewNumberValue(17), true},
		{"boolean", structpb.NewBoolValue(true), true},
		{"null", structpb.NewNullValue(), true},
		{"ordinary kind", object(map[string]*structpb.Value{"kind": structpb.NewStringValue("Record")}), true},
		{"environment", structpb.NewStringValue("${HOST}"), false},
		{"environment fallback", structpb.NewStringValue("${HOST:-localhost}"), false},
		{"empty environment", structpb.NewStringValue("${}"), false},
		{"structured secret", object(map[string]*structpb.Value{"kind": structpb.NewStringValue("Secret")}), false},
		{"structured environment", object(map[string]*structpb.Value{"kind": structpb.NewStringValue("EnvironmentVariable")}), false},
		{"nested reference", structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			object(map[string]*structpb.Value{"host": structpb.NewStringValue("${HOST}")}),
		}}), false},
		{"nan", structpb.NewNumberValue(math.NaN()), false},
		{"infinity", structpb.NewNumberValue(math.Inf(1)), false},
		{"nil value", nil, false},
		{"unset value", &structpb.Value{}, false},
		{"nil string", &structpb.Value{Kind: (*structpb.Value_StringValue)(nil)}, false},
		{"nil number", &structpb.Value{Kind: (*structpb.Value_NumberValue)(nil)}, false},
		{"nil boolean", &structpb.Value{Kind: (*structpb.Value_BoolValue)(nil)}, false},
		{"nil null", &structpb.Value{Kind: (*structpb.Value_NullValue)(nil)}, false},
		{"unknown null", &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: 1}}, false},
		{"nil object", structpb.NewStructValue(nil), false},
		{"nil object variant", &structpb.Value{Kind: (*structpb.Value_StructValue)(nil)}, false},
		{"nil list", structpb.NewListValue(nil), false},
		{"nil list variant", &structpb.Value{Kind: (*structpb.Value_ListValue)(nil)}, false},
	} {
		for _, bootstrap := range []bool{false, true} {
			name := "source/"
			if bootstrap {
				name = "bootstrap/"
			}
			t.Run(name+tc.name, func(t *testing.T) {
				t.Parallel()
				spec := externalDefinition()
				component := spec.GetExternal()
				if bootstrap {
					spec.Bootstrap = proto.Clone(component).(*watchpb.ComponentConfiguration)
					component = spec.Bootstrap
				}
				component.Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{"payload": tc.value}}
				err := validation.ValidateSourceDefinition(spec)
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.Equal(t, codes.InvalidArgument, status.Code(err))
					require.ErrorContains(t, err, "configuration")
				}
			})
		}
	}
}

func TestSourceDefinitionConfigurationDepth(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		depth int
		valid bool
	}{
		{"exact depth", 31, true},
		{"excessive depth", 32, false},
	} {
		for _, kind := range []string{"object", "list"} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				value := structpb.NewStringValue("literal")
				for range tc.depth {
					if kind == "object" {
						value = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"nested": value}})
					} else {
						value = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{value}})
					}
				}
				spec := externalDefinition()
				spec.GetExternal().Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{"payload": value}}
				err := validation.ValidateSourceDefinition(spec)
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "32 levels")
				}
			})
		}
	}
}

func TestSourceDefinitionRejectsWithoutMutationOrValueDisclosure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*watchpb.ComponentConfiguration)
	}{
		{"version", func(component *watchpb.ComponentConfiguration) {
			component.Secrets["connection"].Version = strings.Repeat("PRIVATE_SENTINEL", 100)
		}},
		{"environment", func(component *watchpb.ComponentConfiguration) {
			component.Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
				"PRIVATE_SENTINEL": structpb.NewStringValue("${PRIVATE_SENTINEL}"),
			}}
		}},
		{"overlap", func(component *watchpb.ComponentConfiguration) {
			component.Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
				"connection": structpb.NewStringValue("PRIVATE_SENTINEL"),
			}}
		}},
		{"null overlap", func(component *watchpb.ComponentConfiguration) {
			component.Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
				"connection": structpb.NewNullValue(),
			}}
		}},
	} {
		for _, bootstrap := range []bool{false, true} {
			name := "source/"
			if bootstrap {
				name = "bootstrap/"
			}
			t.Run(name+tc.name, func(t *testing.T) {
				t.Parallel()
				spec := externalDefinition()
				component := spec.GetExternal()
				if bootstrap {
					spec.Bootstrap = proto.Clone(component).(*watchpb.ComponentConfiguration)
					component = spec.Bootstrap
				}
				tc.change(component)
				before := proto.Clone(spec)
				err := validation.ValidateSourceDefinition(spec)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.NotContains(t, err.Error(), "PRIVATE_SENTINEL")
				require.True(t, proto.Equal(before, spec))
			})
		}
	}
}

func TestSourceDefinitionPolicyDoesNotChangeLegacyStructuralValidation(t *testing.T) {
	t.Parallel()
	spec := externalDefinition()
	spec.GetExternal().Secrets["connection"].Version = ""
	request := &watchpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "legacy", Spec: spec}
	require.NoError(t, validation.ValidateRequest(request))
	require.ErrorContains(t, validation.ValidateSourceDefinition(spec), "secrets.version")
	require.NoError(t, validation.ValidateSourceDefinition(temporalSource()))
	require.ErrorContains(t, validation.ValidateSourceDefinition(nil), "spec")
}
