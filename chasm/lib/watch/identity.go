package watch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// ExecutionID matches the native subscription identity for a retained Watch lifetime.
func ExecutionID(namespaceID, watchID, incarnation string) (string, error) {
	encoded, err := json.Marshal(struct {
		NamespaceID string `json:"namespace_id"`
		ResourceID  string `json:"resource_id"`
		Incarnation string `json:"incarnation"`
	}{namespaceID, watchID, incarnation})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("temporal-native-watch-v1\x00"), encoded...))
	return "w-" + hex.EncodeToString(digest[:]), nil
}
