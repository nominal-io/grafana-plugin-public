import { useCallback, useEffect, useMemo, useState } from 'react';
import { COMPUTE_BY_RUN, type NominalQuery } from '../../types';
import { fetchRun, formatRunStart, searchRuns, type RunItem } from '../../utils/api';
import { resolveTemplateValue, type QueryTemplateResolution, type TemplateValueReplacer } from './templateResolution';
import type { RunOption } from './queryBuilderTypes';
import { notifyError } from './notifyError';
import { dashboardVariableOptions } from './variableOptions';

function runToOption(run: RunItem): RunOption {
  const where =
    run.assetRids.length > 1 ? `Spans ${run.assetRids.length} assets, not supported yet` : formatRunStart(run.startMs);
  return {
    label: run.title,
    value: run.rid,
    description: `#${run.runNumber} · ${where}`,
    assetCount: run.assetRids.length,
  };
}

interface UseRunSelectionArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  datasourceUrl: string;
  queryResolution: QueryTemplateResolution;
  replace: TemplateValueReplacer;
  markInteracted: () => void;
}

export function useRunSelection({
  query,
  onChange,
  datasourceUrl,
  queryResolution,
  replace,
  markInteracted,
}: UseRunSelectionArgs) {
  const isRunMode = query?.computeBy === COMPUTE_BY_RUN;
  const runRidResolution = queryResolution.runRid;
  const [run, setRun] = useState<RunItem | null>(null);
  const resolvedRunRid = isRunMode && runRidResolution.isResolved ? runRidResolution.resolved : '';

  useEffect(() => {
    setRun(null);
    if (!resolvedRunRid) {
      return;
    }
    let cancelled = false;
    fetchRun(datasourceUrl, resolvedRunRid).then(
      (found) => !cancelled && setRun(found),
      () => !cancelled && notifyError('Unable to load Nominal run', 'The RID was kept, but the run could not be loaded.')
    );
    return () => {
      cancelled = true;
    };
  }, [datasourceUrl, resolvedRunRid]);

  const runOptions = useCallback(
    async (searchText: string): Promise<RunOption[]> => {
      if (searchText.startsWith('$')) {
        return dashboardVariableOptions(searchText);
      }
      try {
        return (await searchRuns(datasourceUrl, { searchText })).map(runToOption);
      } catch {
        notifyError('Unable to load Nominal runs', 'Check the data source configuration and try again.');
        return [];
      }
    },
    [datasourceUrl]
  );

  // Synchronous on purpose: awaiting a lookup here would let a slower earlier
  // selection, or a stale captured query, overwrite newer edits. A pasted RID or
  // variable carries no asset count and is accepted; a multi-asset run then fails
  // as a per-query error.
  const selectRun = useCallback(
    (selection: RunOption) => {
      markInteracted();
      const assetCount = selection.assetCount ?? 0;
      if (assetCount > 1) {
        notifyError('Multi-asset runs are not supported yet', `${selection.label} spans ${assetCount} assets.`);
        return;
      }
      onChange({ ...query, runRid: selection.value.trim() });
    },
    [markInteracted, onChange, query]
  );

  const runSelectValue = useMemo<RunOption | null>(() => {
    if (!query.runRid) {
      return null;
    }
    return run && run.rid === resolvedRunRid && !runRidResolution.hasTemplate
      ? runToOption(run)
      : { label: query.runRid, value: query.runRid };
  }, [query.runRid, run, resolvedRunRid, runRidResolution.hasTemplate]);

  const effectiveAssetRid = run && run.rid === resolvedRunRid && run.assetRids.length === 1 ? run.assetRids[0] : '';

  // In Run mode the asset cascade sees the run's asset, and every write restores
  // the user's own assetRid so the run's asset is never saved.
  const cascadeQuery = useMemo(
    () => (isRunMode ? { ...query, assetRid: effectiveAssetRid } : query),
    [isRunMode, query, effectiveAssetRid]
  );
  const cascadeOnChange = useCallback(
    (next: NominalQuery) => {
      if (!isRunMode) {
        return onChange(next);
      }
      const { assetRid: _runAsset, ...rest } = next;
      onChange(query.assetRid === undefined ? rest : { ...rest, assetRid: query.assetRid });
    },
    [isRunMode, onChange, query.assetRid]
  );
  const cascadeResolution = isRunMode
    ? { ...queryResolution, assetRid: resolveTemplateValue(effectiveAssetRid, replace) }
    : queryResolution;

  return {
    isRunMode,
    run,
    runOptions,
    runSelectValue,
    selectRun,
    cascade: { query: cascadeQuery, onChange: cascadeOnChange, resolution: cascadeResolution },
  };
}
