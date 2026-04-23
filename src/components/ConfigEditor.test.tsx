import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { DataSourceSettings } from '@grafana/data';
import { ConfigEditor } from './ConfigEditor';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';
import { NOMINAL_DOCS_URL, NOMINAL_PLUGIN_README_URL } from '../constants';

type Options = DataSourceSettings<NominalDataSourceOptions, NominalSecureJsonData>;

function makeOptions(overrides: Partial<Options> = {}): Options {
  return {
    id: 1,
    uid: 'test-uid',
    orgId: 1,
    name: 'Nominal',
    typeLogoUrl: '',
    type: 'nominaltest-nominalds-datasource',
    typeName: 'Nominal',
    access: 'proxy',
    url: '',
    user: '',
    database: '',
    basicAuth: false,
    basicAuthUser: '',
    withCredentials: false,
    isDefault: false,
    jsonData: {},
    secureJsonData: {},
    secureJsonFields: {},
    readOnly: false,
    ...overrides,
  } as Options;
}

describe('ConfigEditor', () => {
  describe('layout', () => {
    it('does not write to options on mount (no side-effect pre-fill)', () => {
      const onOptionsChange = jest.fn();
      render(<ConfigEditor options={makeOptions()} onOptionsChange={onOptionsChange} />);

      expect(onOptionsChange).not.toHaveBeenCalled();
    });

    it('links to the plugin README and the general Nominal docs', () => {
      render(<ConfigEditor options={makeOptions()} onOptionsChange={jest.fn()} />);

      const hrefs = screen.getAllByRole('link').map((a) => a.getAttribute('href'));
      expect(hrefs).toContain(NOMINAL_PLUGIN_README_URL);
      expect(hrefs).toContain(NOMINAL_DOCS_URL);
    });
  });

  describe('handlers', () => {
    it('writes base URL changes to jsonData.baseUrl', () => {
      const onOptionsChange = jest.fn();
      render(<ConfigEditor options={makeOptions()} onOptionsChange={onOptionsChange} />);

      fireEvent.change(screen.getByPlaceholderText('https://api.gov.nominal.io/api'), {
        target: { value: 'https://example.test/api' },
      });

      expect(onOptionsChange).toHaveBeenCalledWith(
        expect.objectContaining({
          jsonData: expect.objectContaining({ baseUrl: 'https://example.test/api' }),
        })
      );
    });

    it('writes API key changes to secureJsonData (not jsonData)', () => {
      const onOptionsChange = jest.fn();
      render(<ConfigEditor options={makeOptions()} onOptionsChange={onOptionsChange} />);

      fireEvent.change(screen.getByPlaceholderText('Enter your Nominal API key'), {
        target: { value: 'my-key' },
      });

      const call = onOptionsChange.mock.calls[0][0];
      expect(call.secureJsonData).toEqual({ apiKey: 'my-key' });
      expect(call.jsonData).not.toHaveProperty('apiKey');
    });

    it('reset clears both secureJsonFields.apiKey and secureJsonData.apiKey', () => {
      const onOptionsChange = jest.fn();
      render(
        <ConfigEditor
          options={makeOptions({ secureJsonFields: { apiKey: true } })}
          onOptionsChange={onOptionsChange}
        />
      );

      fireEvent.click(screen.getByRole('button', { name: /reset/i }));

      expect(onOptionsChange).toHaveBeenCalledWith(
        expect.objectContaining({
          secureJsonFields: expect.objectContaining({ apiKey: false }),
          secureJsonData: expect.objectContaining({ apiKey: '' }),
        })
      );
    });

    it('renders the configured SecretInput when secureJsonFields.apiKey is true', () => {
      render(
        <ConfigEditor
          options={makeOptions({ secureJsonFields: { apiKey: true } })}
          onOptionsChange={jest.fn()}
        />
      );

      expect(screen.getByRole('button', { name: /reset/i })).toBeInTheDocument();
    });
  });
});
