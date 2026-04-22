import React, { ChangeEvent } from 'react';
import { css } from '@emotion/css';
import { InlineField, Input, SecretInput, Stack, useStyles2 } from '@grafana/ui';
import { ConfigSection, DataSourceDescription } from '@grafana/plugin-ui';
import { DataSourcePluginOptionsEditorProps, GrafanaTheme2 } from '@grafana/data';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';
import { DEFAULT_NOMINAL_BASE_URL, NOMINAL_DOCS_URL, NOMINAL_PLUGIN_README_URL } from '../constants';

interface Props extends DataSourcePluginOptionsEditorProps<NominalDataSourceOptions, NominalSecureJsonData> { }

const getStyles = (theme: GrafanaTheme2) => ({
  quickSetupList: css({
    paddingLeft: theme.spacing(3),
  }),
});

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonData, secureJsonFields } = options;
  const apiKey = secureJsonData?.apiKey || '';
  const styles = useStyles2(getStyles);

  const onBaseUrlChange = (event: ChangeEvent<HTMLInputElement>) =>
    onOptionsChange({ ...options, jsonData: { ...jsonData, baseUrl: event.target.value } });

  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) =>
    onOptionsChange({ ...options, secureJsonData: { apiKey: event.target.value } });

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
    <Stack direction="column" gap={4}>
      <DataSourceDescription
        dataSourceName="Nominal"
        docsLink={NOMINAL_PLUGIN_README_URL}
        hasRequiredFields
      />
      <ConfigSection title="Connection">
        <InlineField
          label="Base URL"
          labelWidth={14}
          interactive
          tooltip={`Nominal API base URL including the full path (e.g., ${DEFAULT_NOMINAL_BASE_URL})`}
        >
          <Input
            id="config-editor-base-url"
            onChange={onBaseUrlChange}
            value={jsonData.baseUrl || ''}
            placeholder={DEFAULT_NOMINAL_BASE_URL}
            width={40}
          />
        </InlineField>
      </ConfigSection>
      <ConfigSection title="Authentication">
        <InlineField
          label="API Key"
          labelWidth={14}
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
      </ConfigSection>
      <ConfigSection title="Quick Setup">
        <ol className={styles.quickSetupList}>
          <li>Set Base URL to your Nominal API endpoint including the full path (e.g., {DEFAULT_NOMINAL_BASE_URL}).</li>
          <li>Enter your Nominal API key (NOM_KEY) in the API Key field.</li>
          <li>Click &quot;Save &amp; Test&quot; to verify and save the configuration.</li>
        </ol>
        <p>
          For more on using Nominal, see the{' '}
          <a href={NOMINAL_DOCS_URL} target="_blank" rel="noreferrer">
            Nominal documentation
          </a>
          .
        </p>
      </ConfigSection>
    </Stack>
  );
}
