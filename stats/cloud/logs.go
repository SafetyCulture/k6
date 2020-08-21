package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type msg struct {
	Streams []struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"` // this can be optimized
	} `json:"streams"`
	DroppedEntries []struct {
		Labels    map[string]interface{} `json:"labels"`
		Timestamp string                 `json:"timestamp"`
	} `json:"dropped_entries"`
}

func getLevelsStr(level string) ([]string, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("unknown log level %s", level) // specifically use a custom error
	}
	index := sort.Search(len(logrus.AllLevels), func(i int) bool {
		return logrus.AllLevels[i] > lvl
	})
	result := make([]string, index)
	for i, lvl := range logrus.AllLevels[:index] {
		result[i] = lvl.String()
	}

	return result, nil
}

func (m *msg) Log(logger logrus.FieldLogger) {
	var level string

	for _, stream := range m.Streams {
		fields := make(logrus.Fields, len(stream.Stream)-1)

		for key, val := range stream.Stream {
			if key == "level" {
				level = val

				continue
			}

			fields[key] = val
		}

		for _, value := range stream.Values {
			nsec, _ := strconv.Atoi(value[0])
			e := logger.WithFields(fields).WithTime(time.Unix(0, int64(nsec)))
			lvl, err := logrus.ParseLevel(level)
			if err != nil {
				e.Info(value[1])
				e.Warn("last message had unknown level " + level)
			} else {
				e.Log(lvl, value[1])
			}
		}
	}

	for _, dropped := range m.DroppedEntries {
		nsec, _ := strconv.Atoi(dropped.Timestamp)
		logger.WithFields(
			logrus.Fields(dropped.Labels),
		).WithTime(time.Unix(0, int64(nsec))).Warn("dropped")
	}
}

func parseFilters(id, level string) ([]string, error) {
	idFilter := `test_run_id="` + id + `"`

	lvls, err := getLevelsStr(level)
	if err != nil {
		return nil, err
	}
	levelFilter := `level=~"(` + strings.Join(lvls, "|") + `)"`

	return []string{idFilter, levelFilter}, nil
}

func (c *Config) getRequest(referenceID string, start time.Duration, limit int) (*url.URL, error) {
	u, err := url.Parse(c.LogsHost.String)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse cloud logs host %w", err)
	}

	filters, err := parseFilters(referenceID, logrus.InfoLevel.String())
	if err != nil {
		return nil, err
	}

	u.RawQuery = fmt.Sprintf(`query={%s}&limit=%d&start=%d`,
		strings.Join(filters, ","),
		limit,
		time.Now().Add(-start).UnixNano(),
	)

	return u, nil
}

// StreamLogsToLogger streams the logs for the configured test to the provided logger until ctx is
// Done or an error occurs.
func (c *Config) StreamLogsToLogger(
	ctx context.Context, logger logrus.FieldLogger, referenceID string, start time.Duration, limit int,
) error {
	u, err := c.getRequest(referenceID, start, limit)
	if err != nil {
		return err
	}

	headers := make(http.Header)
	headers.Add("Sec-WebSocket-Protocol", "token="+c.Token.String)

	// We don't need to close the http body or use it for anything until we want to actually log
	// what the server returned as body when it errors out
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), headers) //nolint:bodyclose
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()

		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "closing"),
			time.Now().Add(time.Second))

		_ = conn.Close()
	}()

	msgBuffer := make(chan []byte, 10)

	defer close(msgBuffer)

	go func() {
		for message := range msgBuffer {
			var m msg
			err := json.Unmarshal(message, &m)
			if err != nil {
				logger.WithError(err).Errorf("couldn't unmarshal a message from the cloud: %s", string(message))

				continue
			}

			m.Log(logger)
		}
	}()

	for {
		_, message, err := conn.ReadMessage()
		select { // check if we should stop before continuing
		case <-ctx.Done():
			return nil
		default:
		}

		if err != nil {
			logger.WithError(err).Warn("error reading a message from the cloud")

			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case msgBuffer <- message:
		}
	}
}
