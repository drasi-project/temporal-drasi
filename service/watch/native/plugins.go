package native

import (
	"context"
	"encoding/json"
)

// Descriptors returns the real schemas registered by verified native plugins.
func (r *Runtime) Descriptors(ctx context.Context) (PluginDescriptors, error) {
	result, err := r.Apply(ctx, DescriptorsCommand{})
	if err != nil {
		return PluginDescriptors{}, err
	}
	var descriptors PluginDescriptors
	if err := json.Unmarshal(result.Plugins, &descriptors); err != nil {
		return PluginDescriptors{}, err
	}
	return descriptors, nil
}
