package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

const sqlQueryPath = "/sql/v1/query"
const sqlArrowStreamFormat = "SQL_SERVICE_QUERY_RESULT_FORMAT_ARROW_STREAM"

type sqlClient struct {
	baseURL string
	http    *http.Client
}

func newSqlClient(baseURL string, transport http.RoundTripper) *sqlClient {
	return &sqlClient{baseURL: strings.TrimSuffix(baseURL, "/"), http: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
}

type sqlEndpointError struct {
	Status                                    int
	ErrorName, Detail, SqlQueryId, InstanceId string
}

func (e *sqlEndpointError) Error() string {
	message := e.Detail
	if message == "" {
		message = e.ErrorName
	}
	if message == "" {
		message = fmt.Sprintf("SQL endpoint returned HTTP %d", e.Status)
	}
	if e.SqlQueryId != "" {
		message += " (sqlQueryId: " + e.SqlQueryId + ")"
	}
	return message
}

func (e *sqlEndpointError) backendStatus() backend.Status {
	switch e.Status {
	case 400, 404:
		return backend.StatusBadRequest
	case 401:
		return backend.StatusUnauthorized
	case 403:
		return backend.StatusForbidden
	case 429:
		return backend.StatusTooManyRequests
	default:
		return backend.StatusInternal
	}
}

func (c *sqlClient) Query(ctx context.Context, token, workspaceRid, sql string) (io.ReadCloser, error) {
	payload, err := json.Marshal(struct {
		Query        string `json:"query"`
		WorkspaceRid string `json:"workspace_rid"`
		ResultFormat string `json:"result_format"`
	}{sql, workspaceRid, sqlArrowStreamFormat})
	if err != nil {
		return nil, err
	}
	return c.post(ctx, token, sqlQueryPath, payload, "application/octet-stream")
}

func (c *sqlClient) post(ctx context.Context, token, path string, payload []byte, accept string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SQL request failed: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.Body, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &sqlEndpointError{Status: resp.StatusCode}
	var raw struct {
		ErrorName       string `json:"errorName"`
		ErrorInstanceId string `json:"errorInstanceId"`
		Parameters      struct {
			Detail     string `json:"detail"`
			SqlQueryId string `json:"sqlQueryId"`
		} `json:"parameters"`
	}
	if json.Unmarshal(body, &raw) == nil {
		e.ErrorName, e.InstanceId, e.Detail, e.SqlQueryId = raw.ErrorName, raw.ErrorInstanceId, raw.Parameters.Detail, raw.Parameters.SqlQueryId
	}
	return nil, e
}

// Use the existing catalog REST endpoint. Importing the catalog package from
// the pinned Go SDK conflicts with scout/api's global Conjure error registration.
func (c *sqlClient) DatasetOptions(ctx context.Context, token, workspace, search string) ([]map[string]string, error) {
	payload, err := json.Marshal(map[string]any{
		"query": map[string]any{"type": "and", "and": []map[string]any{
			{"type": "workspace", "workspace": workspace},
			{"type": "searchText", "searchText": search},
			{"type": "archiveStatus", "archiveStatus": false},
		}},
		"pageSize":    100,
		"sortOptions": map[string]any{"field": "INGEST_DATE", "isDescending": true},
	})
	if err != nil {
		return nil, err
	}
	body, err := c.post(ctx, token, "/catalog/v1/search-datasets-v2", payload, "application/json")
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var result struct {
		Results []struct {
			Name string `json:"name"`
			Rid  string `json:"rid"`
		} `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(body, 8<<20)).Decode(&result); err != nil {
		return nil, fmt.Errorf("invalid dataset search response: %w", err)
	}
	options := make([]map[string]string, 0, min(len(result.Results), 100))
	for _, dataset := range result.Results[:min(len(result.Results), 100)] {
		options = append(options, map[string]string{"label": dataset.Name, "value": dataset.Rid})
	}
	return options, nil
}
