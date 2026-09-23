import React from 'react';
import { render, screen } from '@testing-library/react';
import { ConfigEditor } from './ConfigEditor';

describe('ConfigEditor', () => {
  it('configures one connection for both query APIs', () => {
    render(
      <ConfigEditor
        options={{ jsonData: {}, secureJsonData: {}, secureJsonFields: {} } as any}
        onOptionsChange={jest.fn()}
      />
    );
    expect(screen.queryByRole('radio', { name: 'SQL' })).not.toBeInTheDocument();
    expect(screen.getByText(/choose Compute or SQL for each query/)).toBeInTheDocument();
  });
});
