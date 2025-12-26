package integrationtests_test

import (
	"strings"
	"testing"
	"time"

	"github.com/langgenius/dify-sandbox/internal/core/runner/types"
	"github.com/langgenius/dify-sandbox/internal/service"
)

func TestPythonStreamBasic(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunPython3CodeStream(`
import time
print("hello")
print("world")
		`, "", &types.RunnerOptions{
			EnableNetwork: false,
		})
		if err != nil {
			t.Fatal(err)
		}

		var stdout []string
		var stderr []string
		done := false

		for !done {
			select {
			case <-stream.Done:
				done = true
			case out, ok := <-stream.Stdout:
				if ok {
					stdout = append(stdout, string(out))
				}
			case err, ok := <-stream.Stderr:
				if ok {
					stderr = append(stderr, string(err))
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for stream to complete")
			}
		}

		// Drain remaining channels
		for len(stream.Stdout) > 0 {
			stdout = append(stdout, string(<-stream.Stdout))
		}
		for len(stream.Stderr) > 0 {
			stderr = append(stderr, string(<-stream.Stderr))
		}

		stdoutStr := strings.Join(stdout, "")
		stderrStr := strings.Join(stderr, "")

		if stderrStr != "" {
			t.Fatalf("unexpected error: %s\n", stderrStr)
		}

		if !strings.Contains(stdoutStr, "hello") {
			t.Fatalf("expected stdout to contain 'hello', got: %s\n", stdoutStr)
		}

		if !strings.Contains(stdoutStr, "world") {
			t.Fatalf("expected stdout to contain 'world', got: %s\n", stdoutStr)
		}
	})
}

func TestPythonStreamStderr(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunPython3CodeStream(`
import sys
print("error message", file=sys.stderr)
print("normal message")
		`, "", &types.RunnerOptions{
			EnableNetwork: false,
		})
		if err != nil {
			t.Fatal(err)
		}

		var stdout []string
		var stderr []string
		done := false

		for !done {
			select {
			case <-stream.Done:
				done = true
			case out, ok := <-stream.Stdout:
				if ok {
					stdout = append(stdout, string(out))
				}
			case err, ok := <-stream.Stderr:
				if ok {
					stderr = append(stderr, string(err))
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for stream to complete")
			}
		}

		// Drain remaining channels
		for len(stream.Stdout) > 0 {
			stdout = append(stdout, string(<-stream.Stdout))
		}
		for len(stream.Stderr) > 0 {
			stderr = append(stderr, string(<-stream.Stderr))
		}

		stdoutStr := strings.Join(stdout, "")
		stderrStr := strings.Join(stderr, "")

		if !strings.Contains(stderrStr, "error message") {
			t.Fatalf("expected stderr to contain 'error message', got: %s\n", stderrStr)
		}

		if !strings.Contains(stdoutStr, "normal message") {
			t.Fatalf("expected stdout to contain 'normal message', got: %s\n", stdoutStr)
		}
	})
}

func TestPythonStreamIncrementalOutput(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunPython3CodeStream(`
import time
for i in range(5):
    print(f"line {i}")
    time.sleep(0.1)
		`, "", &types.RunnerOptions{
			EnableNetwork: false,
		})
		if err != nil {
			t.Fatal(err)
		}

		var stdout []string
		done := false

		for !done {
			select {
			case <-stream.Done:
				done = true
			case out, ok := <-stream.Stdout:
				if ok {
					stdout = append(stdout, string(out))
				}
			case _, ok := <-stream.Stderr:
				if !ok {
					continue
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for stream to complete")
			}
		}

		stdoutStr := strings.Join(stdout, "")

		// Check that we got incremental output
		for i := 0; i < 5; i++ {
			if !strings.Contains(stdoutStr, "line "+string(rune('0'+i))) {
				t.Fatalf("expected stdout to contain 'line %d', got: %s\n", i, stdoutStr)
			}
		}
	})
}
