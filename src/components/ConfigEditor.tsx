import React, { ChangeEvent } from 'react';
import { css } from '@emotion/css';
import { InlineField, Input, SecretInput, useStyles2 } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, GrafanaTheme2 } from '@grafana/data';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<NominalDataSourceOptions, NominalSecureJsonData> { }

const getStyles = (theme: GrafanaTheme2) => ({
  quickSetup: css({
    backgroundColor: theme.colors.background.secondary,
    borderRadius: theme.shape.radius.default,
    marginTop: theme.spacing(2),
    padding: theme.spacing(1.5),
  }),
  quickSetupTitle: css({
    color: theme.colors.text.primary,
    fontSize: theme.typography.h5.fontSize,
    margin: `0 0 ${theme.spacing(1)} 0`,
  }),
  quickSetupNote: css({
    color: theme.colors.text.primary,
    fontSize: theme.typography.bodySmall.fontSize,
    lineHeight: theme.typography.body.lineHeight,
    margin: `0 0 ${theme.spacing(1)} 0`,
  }),
  quickSetupList: css({
    color: theme.colors.text.primary,
    fontSize: theme.typography.bodySmall.fontSize,
    lineHeight: theme.typography.body.lineHeight,
    margin: 0,
    paddingLeft: theme.spacing(2.5),
  }),
});

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const styles = useStyles2(getStyles);
  const { jsonData, secureJsonData, secureJsonFields } = options;
  const apiKey = secureJsonData?.apiKey || '';

  const onBaseUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        baseUrl: event.target.value,
      },
    });
  };

  const onWorkspaceRidChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        workspaceRid: event.target.value,
      },
    });
  };

  // Secure field (only sent to the backend)
  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        apiKey: event.target.value,
      },
    });
  };

  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        apiKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        apiKey: '',
      },
    });
  };

  return (
    <>
      <InlineField
        label="Base URL"
        labelWidth={26}
        interactive
        tooltip={'Nominal API base URL including the full path (e.g., https://api.gov.nominal.io/api)'}
      >
        <Input
          id="config-editor-base-url"
          onChange={onBaseUrlChange}
          value={jsonData.baseUrl || ''}
          placeholder="https://api.gov.nominal.io/api"
          width={40}
        />
      </InlineField>


      <InlineField
        label="API Key"
        labelWidth={26}
        interactive
        tooltip={'Your Nominal API key (NOM_KEY) - this is stored securely and only sent to the backend'}
      >
        <SecretInput
          required
          id="config-editor-api-key"
          isConfigured={secureJsonFields?.apiKey || false}
          value={apiKey}
          placeholder="Enter your Nominal API key"
          width={40}
          onReset={onResetAPIKey}
          onChange={onAPIKeyChange}
        />
      </InlineField>


      <InlineField
        label="Workspace RID"
        labelWidth={26}
        interactive
        tooltip={
          'Recommended. Limits asset search to this workspace instead of every workspace the key can access. Only search is filtered; assets from other workspaces still work in queries.'
        }
      >
        <Input
          id="config-editor-workspace-rid"
          onChange={onWorkspaceRidChange}
          value={jsonData.workspaceRid || ''}
          placeholder="ri.security.<env>.workspace.<uuid>"
          width={40}
        />
      </InlineField>

      <div className={styles.quickSetup}>
        <h4 className={styles.quickSetupTitle}>Quick Setup Guide:</h4>
        <p className={styles.quickSetupNote}>
          All three values can be copied from <strong>Settings &gt; API keys</strong> in the Nominal app (e.g.
          https://app.gov.nominal.io/settings/org/api-keys).
        </p>
        <ol className={styles.quickSetupList}>
          <li>Set Base URL to your Nominal API endpoint including the full path (e.g. https://api.gov.nominal.io/api)</li>
          <li>Create a Nominal API key and enter it in the API Key field</li>
          <li>Recommended: paste a Workspace RID to limit asset search to one workspace</li>
          <li>Click &quot;Save &amp; Test&quot; to verify and save the configuration</li>
        </ol>
      </div>
    </>
  );
}
