package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const offlineExamplesCommand = "GOWORK=off go test -race ./..."

type examplesManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	Repository    string `json:"repository"`
	ProofSources  []struct {
		ID   string `json:"id"`
		Type string `json:"type"`
		Path string `json:"path"`
	} `json:"proofSources"`
	Examples []struct {
		ID             string            `json:"id"`
		Ecosystem      string            `json:"ecosystem"`
		Owner          string            `json:"owner"`
		SourcePath     string            `json:"sourcePath"`
		Availability   string            `json:"availability"`
		Versions       map[string]string `json:"versions"`
		OfflineCommand string            `json:"offlineCommand"`
		Assertion      string            `json:"assertion"`
		WorkflowPath   string            `json:"workflowPath"`
		JobID          string            `json:"jobId"`
		Cleanup        string            `json:"cleanup"`
		LiveGate       json.RawMessage   `json:"liveGate"`
		ProofIDs       []string          `json:"proofIds"`
	} `json:"examples"`
}

func TestDocsExamplesArtifacts(t *testing.T) {
	repositoryRoot := filepath.Clean("..")
	manifestData, err := os.ReadFile(filepath.Join(repositoryRoot, "testdata/docs/examples.json"))
	if err != nil {
		t.Fatalf("read examples manifest: %v", err)
	}

	var manifest examplesManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode examples manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Repository != "fsstore" {
		t.Fatalf("manifest identity = schema %d repository %q", manifest.SchemaVersion, manifest.Repository)
	}

	proofs := make(map[string]bool, len(manifest.ProofSources))
	for _, proof := range manifest.ProofSources {
		if !strings.HasPrefix(proof.ID, "proof-fsstore-") || proof.Type == "" || proof.Path == "" {
			t.Errorf("invalid proof source: %#v", proof)
		}
		if proofs[proof.ID] {
			t.Errorf("duplicate proof source ID %q", proof.ID)
		}
		proofs[proof.ID] = true
		path := strings.SplitN(proof.Path, "#", 2)[0]
		if _, err := os.Stat(filepath.Join(repositoryRoot, path)); err != nil {
			t.Errorf("proof source %q does not resolve locally: %v", proof.ID, err)
		}
	}

	if len(manifest.Examples) != 1 {
		t.Fatalf("manifest examples = %d, want 1", len(manifest.Examples))
	}
	example := manifest.Examples[0]
	if example.ID != "example-fsstore-durable-composite-store" {
		t.Errorf("example ID = %q", example.ID)
	}
	if example.Ecosystem != "go" || example.Owner != "fsstore" || example.Availability != "source-workspace" {
		t.Errorf("example classification is incorrect: %#v", example)
	}
	if len(example.Versions) != 1 || example.Versions["github.com/looprig/fsstore"] != "source-workspace" {
		t.Errorf("example versions = %#v", example.Versions)
	}
	if example.SourcePath != "examples/durable/example_test.go" {
		t.Errorf("example sourcePath = %q", example.SourcePath)
	}
	if example.OfflineCommand != offlineExamplesCommand {
		t.Errorf("example offlineCommand = %q", example.OfflineCommand)
	}
	if example.Assertion != "Deterministic Go example output proves option validation, composite ledger/KV/blob behavior, explicit resource ownership, and durable reads after reopening the same local root." {
		t.Errorf("example assertion = %q", example.Assertion)
	}
	if example.WorkflowPath != ".github/workflows/docs-examples.yml" || example.JobID != "docs-examples" {
		t.Errorf("example workflow metadata = %q / %q", example.WorkflowPath, example.JobID)
	}
	if example.Cleanup != "The example defers removal of its unique temporary store root after closing all cursors, blob readers, and stores." {
		t.Errorf("example cleanup = %q", example.Cleanup)
	}
	if string(example.LiveGate) != "null" {
		t.Errorf("example liveGate = %s, want null", example.LiveGate)
	}
	if len(example.ProofIDs) != 2 {
		t.Fatalf("example proofIds = %v, want source and contract-test proofs", example.ProofIDs)
	}
	for _, proofID := range example.ProofIDs {
		if !proofs[proofID] {
			t.Errorf("example references unknown proof %q", proofID)
		}
	}

	workflow, err := os.ReadFile(filepath.Join(repositoryRoot, ".github/workflows/docs-examples.yml"))
	if err != nil {
		t.Fatalf("read docs examples workflow: %v", err)
	}
	for _, literal := range []string{
		"docs-examples:",
		offlineExamplesCommand,
		"GOWORK=off make check",
	} {
		if !strings.Contains(string(workflow), literal) {
			t.Errorf("workflow does not contain %q", literal)
		}
	}
}
