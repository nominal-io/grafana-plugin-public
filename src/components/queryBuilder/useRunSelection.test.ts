/* eslint-disable @typescript-eslint/no-deprecated */
import { act, renderHook, waitFor } from '@testing-library/react';
import type { NominalQuery } from '../../types';
import type { RunItem } from '../../utils/api';
import type { RunOption } from './queryBuilderTypes';
import { resolveQueryTemplateValues } from './templateResolution';
import { useRunSelection } from './useRunSelection';

let variables: Array<{ name: string }> = [];
const post = jest.fn();
const publish = jest.fn();

jest.mock('@grafana/runtime', () => ({
  getBackendSrv: () => ({ post }),
  getAppEvents: () => ({ publish }),
  getTemplateSrv: () => ({ getVariables: () => variables }),
}));

const URL = '/resources';
const ASSET = 'ri.scout.main.asset.a1';
const single: RunItem = { rid: 'ri.scout.main.run.single', title: 'Single', runNumber: 1, startMs: 0, assetRids: [ASSET] };
const multi: RunItem = { rid: 'ri.scout.main.run.multi', title: 'Multi', runNumber: 2, startMs: 0, assetRids: [ASSET, 'ri.scout.main.asset.a2'] };

const identity = (v: string) => v;

function setup(runRid: string, replace = identity) {
  const onChange = jest.fn();
  const query = { refId: 'A', queryType: 'compute', computeBy: 'run', runRid } as NominalQuery;
  const hook = renderHook(() =>
    useRunSelection({
      query,
      onChange,
      datasourceUrl: URL,
      queryResolution: resolveQueryTemplateValues({ query, replace }),
      replace,
      markInteracted: jest.fn(),
    })
  );
  return { ...hook, onChange, query };
}

beforeEach(() => {
  variables = [{ name: 'run' }, { name: 'asset' }];
  post.mockReset();
  publish.mockReset();
  post.mockImplementation(async (url: string) => {
    if (url.endsWith('/runs')) {
      return [single, multi];
    }
    return single;
  });
});

describe('useRunSelection', () => {
  it('exposes the run asset after fetching a saved run', async () => {
    const { result } = setup(single.rid);
    await waitFor(() => expect(result.current.cascade.query.assetRid).toBe(ASSET));
  });

  it('does not fetch while the run variable is unresolved', async () => {
    const { result } = setup('$run');
    expect(post).not.toHaveBeenCalled();
    expect(result.current.cascade.query.assetRid).toBe('');
  });

  type Loader = { current: { runOptions: (text: string) => Promise<RunOption[]> } };
  async function listedOption(result: Loader, rid: string) {
    let options: RunOption[] = [];
    await act(async () => {
      options = await result.current.runOptions('');
    });
    return options.find((o) => o.value === rid)!;
  }

  it('rejects a listed multi-asset run without writing the query', async () => {
    const { result, onChange } = setup('');
    const option = await listedOption(result, multi.rid);
    post.mockClear();
    act(() => result.current.selectRun(option));
    expect(onChange).not.toHaveBeenCalled();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(post).not.toHaveBeenCalled();
  });

  it.each([
    ['a listed single-asset run', single.rid, true],
    ['a pasted RID that was never listed', 'ri.scout.main.run.pasted', false],
  ])('selects %s synchronously and changes only runRid', async (_name, rid, listFirst) => {
    const { result, onChange, query } = setup('');
    const option = listFirst ? await listedOption(result, rid) : { label: rid, value: ` ${rid} ` };
    act(() => result.current.selectRun(option));
    expect(onChange).toHaveBeenCalledWith({ ...query, runRid: rid });
  });

  it('lists only dashboard variables for $ text', async () => {
    const { result } = setup('');
    let options: Array<{ value: string }> = [];
    await act(async () => {
      options = await result.current.runOptions('$ru');
    });
    expect(options.map((o) => o.value)).toEqual(['$run']);
    expect(post).not.toHaveBeenCalled();
  });
});
