package controller

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/gin-gonic/gin"
	runner_types "github.com/langgenius/dify-sandbox/internal/core/runner/types"
	"github.com/langgenius/dify-sandbox/internal/service"
	"github.com/langgenius/dify-sandbox/internal/types"
)

type SSEMessage struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

func RunSandboxController(c *gin.Context) {
	BindRequest(c, func(req struct {
		Language      string `json:"language" form:"language" binding:"required"`
		Code          string `json:"code" form:"code" binding:"required"`
		Preload       string `json:"preload" form:"preload"`
		EnableNetwork bool   `json:"enable_network" form:"enable_network"`
		Stream        bool   `json:"stream" form:"stream"`
	}) {
		options := &runner_types.RunnerOptions{
			EnableNetwork: req.EnableNetwork,
		}

		// Stream mode using SSE
		if req.Stream {
			c.SSEvent("content-type", "text/event-stream")
			c.Writer.Header().Set("Cache-Control", "no-cache")
			c.Writer.Header().Set("Connection", "keep-alive")
			c.Writer.Header().Set("Transfer-Encoding", "chunked")

			switch req.Language {
			case "python3":
				stream, err := service.RunPython3CodeStream(req.Code, req.Preload, options)
				if err != nil {
					sendSSEError(c, err)
					return
				}
				handleStreamOutput(c, stream.Stdout, stream.Stderr, stream.Done)
			case "nodejs":
				stream, err := service.RunNodeJsCodeStream(req.Code, req.Preload, options)
				if err != nil {
					sendSSEError(c, err)
					return
				}
				handleStreamOutput(c, stream.Stdout, stream.Stderr, stream.Done)
			default:
				sendSSEError(c, errors.New("unsupported language"))
			}
			return
		}

		// Non-stream mode (original behavior)
		switch req.Language {
		case "python3":
			c.JSON(200, service.RunPython3Code(req.Code, req.Preload, options))
		case "nodejs":
			c.JSON(200, service.RunNodeJsCode(req.Code, req.Preload, options))
		default:
			c.JSON(400, types.ErrorResponse(-400, "unsupported language"))
		}
	})
}

func handleStreamOutput(c *gin.Context, stdout <-chan []byte, stderr <-chan []byte, done <-chan bool) {
	_, ok := c.Writer.(gin.ResponseWriter)
	if !ok {
		c.JSON(500, types.ErrorResponse(-500, "streaming not supported"))
		return
	}

	c.Stream(func(w io.Writer) bool {
		select {
		case <-done:
			// Send completion event
			msg := SSEMessage{Type: "done", Data: ""}
			data, _ := json.Marshal(msg)
			c.SSEvent("message", data)
			return false
		case out, ok := <-stdout:
			if !ok {
				return true
			}
			msg := SSEMessage{Type: "stdout", Data: string(out)}
			data, _ := json.Marshal(msg)
			c.SSEvent("message", data)
			return true
		case err, ok := <-stderr:
			if !ok {
				return true
			}
			msg := SSEMessage{Type: "stderr", Data: string(err)}
			data, _ := json.Marshal(msg)
			c.SSEvent("message", data)
			return true
		}
	})
}

func sendSSEError(c *gin.Context, err error) {
	msg := SSEMessage{Type: "error", Data: err.Error()}
	data, _ := json.Marshal(msg)
	c.SSEvent("message", data)
}

func GetDependencies(c *gin.Context) {
	BindRequest(c, func(req struct {
		Language string `json:"language" form:"language" binding:"required"`
	}) {
		switch req.Language {
		case "python3":
			c.JSON(200, service.ListPython3Dependencies())
		default:
			c.JSON(400, types.ErrorResponse(-400, "unsupported language"))
		}
	})
}

func UpdateDependencies(c *gin.Context) {
	BindRequest(c, func(req struct {
		Language string `json:"language" form:"language" binding:"required"`
	}) {
		switch req.Language {
		case "python3":
			c.JSON(200, service.UpdateDependencies())
		default:
			c.JSON(400, types.ErrorResponse(-400, "unsupported language"))
		}
	})
}

func RefreshDependencies(c *gin.Context) {
	BindRequest(c, func(req struct {
		Language string `json:"language" form:"language" binding:"required"`
	}) {
		switch req.Language {
		case "python3":
			c.JSON(200, service.RefreshPython3Dependencies())
		default:
			c.JSON(400, types.ErrorResponse(-400, "unsupported language"))
		}
	})
}
