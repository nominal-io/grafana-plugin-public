import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { DataSourceSettings } from '@grafana/data';
import { ConfigEditor } from './ConfigEditor';
import { NominalDataSourceOptions, NominalSecureJsonData } from '../types';
import { DEFAULT_NOMINAL_BASE_URL, NOMINAL_DOCS_URL, NOMINAL_PLUGIN_README_URL } from '../constants';

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
  describe('input trimming', () => {
    it('trims whitespace from base URL on change', () => {
      const onOptionsChange = jest.fn();
      render(<ConfigEditor options={makeOptions()} onOptionsChange={onOptionsChange} />);

      const input = screen.getByPlaceholderText(DEFAULT_NOMINAL_BASE_URL) as HTMLInputElement;
      fireEvent.change(input, { target: { value: '  https://api.example.com  ' } });

      expect(onOptionsChange).toHaveBeenCalledWith(
        expect.objectContaining({
          jsonData: expect.objectContaining({ baseUrl: 'https://api.example.com' }),
        })
      );
    });

    it('trims whitespace from API key on change', () => {
      const onOptionsChange = jest.fn();
      render(<ConfigEditor options={makeOptions()} onOptionsChange={onOptionsChange} />);

      const input = screen.getByPlaceholderText('Enter your Nominal API key') as HTMLInputElement;
      fireEvent.change(input, { target: { value: '  secret-key  ' } });

      expect(onOptionsChange).toHaveBeenCalledWith(
        expect.objectContaining({
          secureJsonData: expect.objectContaining({ apiKey: 'secret-key' }),
        })
      );
    });
  });

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

    it('renders Connection, Authentication, and Quick Setup sections', () => {
      render(<ConfigEditor options={makeOptions()} onOptionsChange={jest.fn()} />);

      expect(screen.getByText('Connection')).toBeInTheDocument();
      expect(screen.getByText('Authentication')).toBeInTheDocument();
      expect(screen.getByText('Quick Setup')).toBeInTheDocument();
    });

    it('renders the three quick-setup steps', () => {
      render(<ConfigEditor options={makeOptions()} onOptionsChange={jest.fn()} />);

      const items = screen.getAllByRole('listitem');
      expect(items).toHaveLength(3);
      expect(items[0]).toHaveTextContent('Base URL');
      expect(items[1]).toHaveTextContent('API key');
      expect(items[2]).toHaveTextContent('Save');
    });
  });
});
