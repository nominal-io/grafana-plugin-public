import React from 'react';
import { render, screen } from '@testing-library/react';
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
});
