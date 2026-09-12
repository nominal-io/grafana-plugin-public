import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import type { DataSourceSettings } from '@grafana/data';
import { ConfigEditor } from './ConfigEditor';
import type { NominalDataSourceOptions, NominalSecureJsonData } from '../types';

type Options = DataSourceSettings<NominalDataSourceOptions, NominalSecureJsonData>;

describe('ConfigEditor', () => {
  it.each([
    ['trims a pasted workspace RID', '  ri.security.test.workspace.1  ', 'ri.security.test.workspace.1'],
    ['stores an empty string when the workspace RID is cleared', '   ', ''],
  ])('%s', (_name, typed, stored) => {
    const onOptionsChange = jest.fn();
    const options = {
      jsonData: { baseUrl: 'https://api.example/api' },
      secureJsonData: {},
      secureJsonFields: {},
    } as Options;
    render(<ConfigEditor options={options} onOptionsChange={onOptionsChange} />);

    fireEvent.change(screen.getByLabelText('Workspace RID (recommended)'), { target: { value: typed } });

    expect(onOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({ jsonData: { baseUrl: 'https://api.example/api', workspaceRid: stored } })
    );
  });
});
