package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/nominal-io/nominal-api-go/api/rids"
	workspaceapi "github.com/nominal-io/nominal-api-go/security/api/workspace"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

const testWorkspaceRid = "ri.security.test.workspace.11111111-1111-1111-1111-111111111111"

type mockWorkspaceService struct {
	displayName *string
	err         error
}

func (m *mockWorkspaceService) GetWorkspace(_ context.Context, _ bearertoken.Token, workspaceRid rids.WorkspaceRid) (workspaceapi.Workspace, error) {
	return workspaceapi.Workspace{Rid: workspaceRid, DisplayName: m.displayName}, m.err
}
func (m *mockWorkspaceService) GetWorkspaces(context.Context, bearertoken.Token) ([]workspaceapi.Workspace, error) {
	return nil, nil
}
func (m *mockWorkspaceService) UpdateWorkspace(context.Context, bearertoken.Token, rids.WorkspaceRid, workspaceapi.UpdateWorkspaceRequest) (workspaceapi.Workspace, error) {
	return workspaceapi.Workspace{}, nil
}
func (m *mockWorkspaceService) GetDefaultWorkspace(context.Context, bearertoken.Token) (*workspaceapi.Workspace, error) {
	return nil, nil
}

func newWorkspaceTestDatasource(t *testing.T, baseURL, workspaceRid string, ws *mockWorkspaceService) *Datasource {
	t.Helper()
	ds := newTestDatasource(baseURL, &mockAuthService{}, &mockDatasourceService{})
	ds.settings.JSONData = []byte(fmt.Sprintf(`{"baseUrl": %q, "workspaceRid": %q}`, baseURL, workspaceRid))
	ds.workspaceService = ws
	if workspaceRid != "" {
		parsed, err := rid.ParseRID(workspaceRid)
		if err != nil {
			t.Fatal(err)
		}
		typed := rids.WorkspaceRid(parsed)
		ds.workspaceRid = &typed
	}
	return ds
}

func TestCheckHealthWorkspace(t *testing.T) {
	name := "ITAR"
	cases := []struct {
		name, workspaceRid string
		ws                 *mockWorkspaceService
		wantStatus         backend.HealthStatus
		wantMessage        string
	}{
		{"no workspace", "", &mockWorkspaceService{}, backend.HealthStatusOk, "Successfully connected to Nominal API"},
		{"named workspace", testWorkspaceRid, &mockWorkspaceService{displayName: &name}, backend.HealthStatusOk, "Workspace: ITAR"},
		{"unnamed workspace", testWorkspaceRid, &mockWorkspaceService{}, backend.HealthStatusOk, "Workspace: " + testWorkspaceRid},
		{"inaccessible workspace", testWorkspaceRid, &mockWorkspaceService{err: &apiError{Status: http.StatusForbidden}}, backend.HealthStatusError, "not accessible"},
		{"workspace lookup times out", testWorkspaceRid, &mockWorkspaceService{err: context.DeadlineExceeded}, backend.HealthStatusError, "Connection timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := newWorkspaceTestDatasource(t, "http://example", tc.workspaceRid, tc.ws)
			result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
				PluginContext: backend.PluginContext{DataSourceInstanceSettings: &ds.settings},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != tc.wantStatus || !strings.Contains(result.Message, tc.wantMessage) {
				t.Fatalf("got %s %q, want %s containing %q", result.Status, result.Message, tc.wantStatus, tc.wantMessage)
			}
		})
	}
}

func TestAssetsVariableAppliesWorkspaceFilter(t *testing.T) {
	var gotQuery interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		gotQuery = body["query"]
		json.NewEncoder(w).Encode(AssetResponse{})
	}))
	defer server.Close()

	ds := newWorkspaceTestDatasource(t, server.URL, testWorkspaceRid, &mockWorkspaceService{})
	body, _ := json.Marshal(map[string]any{"searchText": "eng"})
	resp := callResourceAndCapture(t, ds, &backend.CallResourceRequest{Path: "assets", Method: http.MethodPost, Body: body})
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}

	want, _ := json.Marshal(map[string]interface{}{
		"type": "and",
		"and": []interface{}{
			map[string]interface{}{"type": "searchText", "searchText": "eng"},
			map[string]interface{}{"type": "workspace", "workspace": testWorkspaceRid},
		},
	})
	if got, _ := json.Marshal(gotQuery); string(got) != string(want) {
		t.Fatalf("query = %s, want %s", got, want)
	}
}

func TestNewDatasourceWorkspaceRid(t *testing.T) {
	cases := []struct {
		name, workspaceRid string
		wantErr            bool
	}{
		{"empty", "", false},
		{"valid", testWorkspaceRid, false},
		{"malformed", "not-a-rid", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{
				JSONData: []byte(fmt.Sprintf(`{"baseUrl":"http://example","workspaceRid":%q}`, tc.workspaceRid)),
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
