import { ridType } from './rid';

describe('ridType', () => {
  it.each([
    ['ri.scout.main.asset.a1', 'asset'],
    ['ri.catalog..dataset.x-1.y', 'dataset'],
    ['  ri.scout.main.asset.a1  ', 'asset'],
    ['ri.Scout.main.asset.a1', undefined],
    ['ri.scout.main.data_set.a1', undefined],
    ['ri.scout.main.asset.', undefined],
    ['ri.scout.main.asset.a 1', undefined],
    ['$asset', undefined],
  ])('ridType(%j) is %s', (value, want) => {
    expect(ridType(value)).toBe(want);
  });
});
