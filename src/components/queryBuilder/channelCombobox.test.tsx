import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { Combobox } from '@grafana/ui';
import { buildChannelOptions, channelsToOptions } from './queryBuilderOptions';
import { resolveTemplateValue } from './templateResolution';

class IntersectionObserverMock {
  disconnect() {}
  observe() {}
  unobserve() {}
}

describe('channel Combobox', () => {
  beforeAll(() => {
    Object.defineProperty(globalThis, 'IntersectionObserver', {
      configurable: true,
      value: IntersectionObserverMock,
    });
    Object.defineProperties(HTMLElement.prototype, {
      offsetHeight: { configurable: true, get: () => 234 },
      offsetWidth: { configurable: true, get: () => 384 },
    });
  });

  it('selects the exact returned channel when Enter is pressed', async () => {
    const exact = 'vehicle.telemetry.engine_metrics.rpm_rx_count';
    const onChange = jest.fn();
    const channelResults = channelsToOptions([
      {
        name: 'vehicle_telemetry_engine_metrics_rpm_rx_count',
        dataSource: 'ds',
        description: '',
        dataType: 'numeric',
      },
      { name: exact, dataSource: 'ds', description: '', dataType: 'numeric' },
    ]);

    render(
      <Combobox
        data-testid="channel-combobox"
        value={null}
        options={async (searchText) =>
          buildChannelOptions({
            channelResults,
            channel: resolveTemplateValue('', (value) => value),
            searchText,
          })
        }
        onChange={onChange}
        createCustomValue
      />
    );

    const input = screen.getByTestId('channel-combobox');
    // JSDOM cannot provide item offsets to Grafana's virtualizer while the menu opens.
    const virtualizerWarning = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    fireEvent.click(input);
    fireEvent.change(input, { target: { value: exact } });

    await waitFor(() => expect(screen.getByRole('option', { name: exact })).toHaveAttribute('aria-selected', 'true'));
    virtualizerWarning.mockRestore();
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ value: exact }));
  });
});
