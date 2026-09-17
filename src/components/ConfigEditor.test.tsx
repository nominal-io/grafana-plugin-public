import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { ConfigEditor } from './ConfigEditor';

describe('ConfigEditor', () => {
  it('writes enableSql when the SQL queries switch changes', () => {
    const onOptionsChange = jest.fn();
    render(
      <ConfigEditor
        options={{ jsonData: {}, secureJsonData: {}, secureJsonFields: {} } as any}
        onOptionsChange={onOptionsChange}
      />
    );

    fireEvent.click(screen.getByRole('switch', { name: 'SQL queries' }));

    expect(onOptionsChange).toHaveBeenCalledWith(expect.objectContaining({
      jsonData: expect.objectContaining({ enableSql: true }),
    }));
  });
});
