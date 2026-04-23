import React, { ChangeEvent } from 'react';
import { css } from '@emotion/css';
import { InlineField, Input, SecretInput, Stack, Text, TextLink, useStyles2 } from '@grafana/ui';
import { ConfigSection, DataSourceDescription } from '@grafana/plugin-ui';
import { DataSourcePluginOptionsEditorProps, GrafanaTheme2 } from '@grafana/data';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';
import { DEFAULT_NOMINAL_BASE_URL, NOMINAL_DOCS_URL, NOMINAL_PLUGIN_README_URL } from '../constants';

const getStyles = (theme: GrafanaTheme2) => ({
  stepsList: css({
    paddingLeft: theme.spacing(3),
    margin: 0,
  }),
});

interface Props extends DataSourcePluginOptionsEditorProps<NominalDataSourceOptions, NominalSecureJsonData> { }

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonData, secureJsonFields } = options;
  const apiKey = secureJsonData?.apiKey || '';

  const onBaseUrlChange = (event: ChangeEvent<HTMLInputElement>) =>
    onOptionsChange({ ...options, jsonData: { ...jsonData, baseUrl: event.target.value } });

  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) =>
    onOptionsChange({ ...options, secureJsonData: { apiKey: event.target.value } });

  const styles = useStyles2(getStyles);

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
      <Stack direction="column" gap={2}>
        <DataSourceDescription
          dataSourceName="Nominal"
          docsLink={NOMINAL_PLUGIN_README_URL}
          hasRequiredFields
        />
        <ol className={styles.stepsList}>
          <Text element="li" color="secondary">
            Set Base URL to your Nominal API endpoint including the full path (e.g., {DEFAULT_NOMINAL_BASE_URL}).
          </Text>
          <Text element="li" color="secondary">
            Enter your Nominal API key (NOM_KEY) in the API Key field.
          </Text>
          <Text element="li" color="secondary">
            Click &quot;Save &amp; Test&quot; to verify and save the configuration.
          </Text>
        </ol>
        <Text element="p" color="secondary">
          For more information on using Nominal, see the{' '}
          <TextLink href={NOMINAL_DOCS_URL} external inline>
            Nominal documentation
          </TextLink>
          .
        </Text>
      </Stack>
      <ConfigSection title="Connection">
        <InlineField
          label="Base URL"
          labelWidth={14}
          required
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
          required
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
    </Stack>
  );
}
