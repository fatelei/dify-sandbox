package integrationtests_test

import (
	"strings"
	"testing"
	"time"

	"github.com/langgenius/dify-sandbox/internal/core/runner/types"
	"github.com/langgenius/dify-sandbox/internal/service"
)

func TestNodeJsStreamBasic(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunNodeJsCodeStream(`
console.log("hello");
console.log("world");
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

func TestNodeJsStreamStderr(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunNodeJsCodeStream(`
console.error("error message");
console.log("normal message");
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

func TestNodeJsStreamIncrementalOutput(t *testing.T) {
	runMultipleTestings(t, 10, func(t *testing.T) {
		stream, err := service.RunNodeJsCodeStream(`
const { setTimeout } = require('timers/promises');
for (let i = 0; i < 5; i++) {
    console.log(` + "`" + `line ${i}` + "`" + `);
    await new Promise(resolve => setTimeout(resolve, 100));
}
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
			expected := "line " + string(rune('0'+i))
			if !strings.Contains(stdoutStr, expected) {
				t.Fatalf("expected stdout to contain '%s', got: %s\n", expected, stdoutStr)
			}
		}
	})
}
