package plugin

import (
	"github.com/nominal-inc/nominal-ds/pkg/models"
)

func newTestQueryExecution(ds *Datasource, config *models.PluginSettings) *NominalQueryExecution {
	if config == nil {
		config = &models.PluginSettings{
			Secrets: &models.SecretPluginSettings{ApiKey: "test-key"},
		}
	}
	return newNominalQueryExecution(ds, config)
}
