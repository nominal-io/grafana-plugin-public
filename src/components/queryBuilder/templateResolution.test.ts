import { renderHook } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import {
  resolveQueryTemplateValues,
  resolveTemplateValue,
  templateDisplayLabel,
  useResolutionSnapshot,
} from './templateResolution';

describe('templateResolution', () => {
  const replace = (value: string) =>
    ({ '$asset': 'ri.scout.asset.a', '$scope': 'primary', '$chan': 'temperature' })[value] ?? value;

  it('resolves query template fields through one replacement adapter', () => {
    const query = {
      refId: 'A',
      assetRid: '$asset',
      dataScopeName: '$scope',
      channel: '$chan',
    } as NominalQuery;

    const result = resolveQueryTemplateValues({ query, replace });

    expect(result.assetRid).toEqual({
      raw: '$asset',
      resolved: 'ri.scout.asset.a',
      hasTemplate: true,
      isResolved: true,
    });
    expect(result.dataScopeName).toEqual({
      raw: '$scope',
      resolved: 'primary',
      hasTemplate: true,
      isResolved: true,
    });
    expect(result.channel).toEqual({
      raw: '$chan',
      resolved: 'temperature',
      hasTemplate: true,
      isResolved: true,
    });
  });

  it('marks unresolved template values as unresolved', () => {
    expect(resolveTemplateValue('$missing', replace)).toEqual({
      raw: '$missing',
      resolved: '$missing',
      hasTemplate: true,
      isResolved: false,
    });
  });

  it('builds raw to resolved display labels only when the resolved value is usable', () => {
    expect(templateDisplayLabel({ raw: '$scope', resolved: 'primary', hasTemplate: true, isResolved: true })).toBe(
      '$scope \u2192 primary'
    );
    expect(templateDisplayLabel({ raw: '$scope', resolved: '$scope', hasTemplate: true, isResolved: false })).toBe(
      '$scope'
    );
    expect(templateDisplayLabel({ raw: 'primary', resolved: 'primary', hasTemplate: false, isResolved: true })).toBe(
      'primary'
    );
  });

  it('memoizes resolution snapshots by primitive fields', () => {
    const value = {
      raw: '$scope',
      resolved: 'primary',
      hasTemplate: true,
      isResolved: true,
    };
    const { result, rerender } = renderHook(({ resolution }) => useResolutionSnapshot(resolution), {
      initialProps: { resolution: value },
    });
    const snapshot = result.current;

    rerender({ resolution: { ...value } });

    expect(result.current).toBe(snapshot);

    rerender({ resolution: { ...value, resolved: 'secondary' } });

    expect(result.current).not.toBe(snapshot);
    expect(result.current.resolved).toBe('secondary');
  });
});
