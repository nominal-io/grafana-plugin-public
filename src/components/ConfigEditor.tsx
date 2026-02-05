import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<NominalDataSourceOptions, NominalSecureJsonData> { }

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
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
        labelWidth={14}
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


      <div style={{ marginTop: '16px', padding: '12px', backgroundColor: '#e0e0e0', borderRadius: '4px' }}>
        <h4 style={{ margin: '0 0 8px 0', fontSize: '14px', color: '#333' }}>Quick Setup Guide:</h4>
        <ol style={{ margin: '0', paddingLeft: '20px', fontSize: '12px', lineHeight: '1.4', color: '#333' }}>
          <li>Set Base URL to your Nominal API endpoint including the full path (e.g., https://api.gov.nominal.io/api)</li>
          <li>Enter your Nominal API key (NOM_KEY) in the API Key field</li>
          <li>Click &quot;Save &amp; Test&quot; to verify and save the configuration</li>
        </ol>
      </div>
    </>
  );
}
