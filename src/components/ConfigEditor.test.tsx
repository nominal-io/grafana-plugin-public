import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { ConfigEditor } from './ConfigEditor';

describe('ConfigEditor', () => {
  it('selects the SQL API for this datasource', () => {
    const onOptionsChange = jest.fn();
    render(
      <ConfigEditor
        options={{ jsonData: {}, secureJsonData: {}, secureJsonFields: {} } as any}
        onOptionsChange={onOptionsChange}
      />
    );

    fireEvent.click(screen.getByRole('radio', { name: 'SQL' }));

    expect(onOptionsChange).toHaveBeenCalledWith(expect.objectContaining({
      jsonData: expect.objectContaining({ queryApi: 'sql' }),
    }));
  });
});
