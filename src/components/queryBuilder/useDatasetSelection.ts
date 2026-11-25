import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { dateTimeFormat } from '@grafana/data';
import type { NominalQuery } from '../../types';
import { badRequestMessage, fetchDatasetsByRid, searchDatasets, type Dataset } from '../../utils/api';
import { ridType } from '../../utils/rid';
import { changeDatasetRidQuery } from './queryMutations';
import { resolvedRidError } from './queryBuilderOptions';
import { notifyError } from './notifyError';
import { dashboardVariableOptions } from './variableOptions';
import { templateDisplayLabel, useResolutionSnapshot, type TemplateValueResolution } from './templateResolution';
import type { ChannelSource, DatasetOptionsLoader, PickerOption } from './queryBuilderTypes';

interface UseDatasetSelectionArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  datasourceUrl: string;
  datasetRidResolution: TemplateValueResolution;
  markInteracted: () => void;
  /** Whether the dataset picker is shown. */
  active: boolean;
  timeZone?: string;
}

export interface DatasetSelectionModel {
  selectedDataset: Dataset | null;
  datasetOptions: DatasetOptionsLoader;
  datasetSelectValue: PickerOption | null;
  datasetError: string | undefined;
  channelSource: ChannelSource;
  selectDataset: (value: string) => void;
}

export function useDatasetSelection({
  query,
  onChange,
  datasourceUrl,
  datasetRidResolution,
  markInteracted,
  active,
  timeZone,
}: UseDatasetSelectionArgs): DatasetSelectionModel {
  const snapshot = useResolutionSnapshot(datasetRidResolution);
  const { resolved, isResolved } = snapshot;
  const ridError = isResolved ? resolvedRidError(resolved, 'dataset') : undefined;
  // `dataset: null` is a completed lookup that found nothing; no entry means loading or failed.
  const [lookup, setLookup] = useState<{ rid: string; dataset: Dataset | null }>();
  // Each pick looks the dataset up again, so picking it again can clear a lookup that found nothing.
  const [pickCount, setPickCount] = useState(0);
  // A typed value that was not a dataset RID; the query keeps its previous dataset.
  const [inputError, setInputError] = useState<string>();

  useEffect(() => {
    if (!resolved || !isResolved || ridError) {
      return;
    }
    let cancelled = false;
    fetchDatasetsByRid(datasourceUrl, [resolved])
      .then((found) => {
        if (!cancelled) {
          setLookup({ rid: resolved, dataset: found.find((d) => d.rid === resolved) ?? null });
        }
      })
      .catch((error) => {
        if (!cancelled) {
          notifyError(
            'Unable to load Nominal dataset',
            badRequestMessage(error, 'The RID was kept, but its name could not be loaded.')
          );
        }
      });
    return () => {
      cancelled = true;
    };
  }, [datasourceUrl, resolved, isResolved, ridError, pickCount]);

  const current = lookup?.rid === resolved ? lookup.dataset : undefined;
  const selectedDataset = current ?? null;

  const searchRequestId = useRef(0);
  // A search still in flight when the picker is hidden or unmounted stays quiet, and a
  // rejected entry is forgotten.
  useEffect(
    () => () => {
      searchRequestId.current += 1;
      setInputError(undefined);
    },
    [active]
  );

  const datasetOptions = useCallback<DatasetOptionsLoader>(
    async (searchText) => {
      const requestId = ++searchRequestId.current;
      if (searchText.startsWith('$')) {
        return dashboardVariableOptions(searchText);
      }
      try {
        const found = await searchDatasets(datasourceUrl, searchText);
        return found.map((dataset) => ({
          label: dataset.name,
          value: dataset.rid,
          description:
            dataset.startMs === undefined ? undefined : `Starts ${dateTimeFormat(dataset.startMs, { timeZone })}`,
        }));
      } catch (error) {
        if (searchRequestId.current === requestId) {
          notifyError(
            'Unable to load Nominal datasets',
            badRequestMessage(error, 'Check the data source configuration and try again.')
          );
        }
        return [];
      }
    },
    [datasourceUrl, timeZone]
  );

  const datasetSelectValue = useMemo<PickerOption | null>(() => {
    if (!snapshot.raw) {
      return null;
    }
    const label = snapshot.hasTemplate ? templateDisplayLabel(snapshot, current?.name) : current?.name ?? snapshot.raw;
    return { label, value: snapshot.raw };
  }, [snapshot, current]);

  const datasetError = ridError ?? (current === null ? 'Dataset not found' : undefined);

  const selectDataset = useCallback(
    (value: string) => {
      const trimmed = value.trim();
      if (!trimmed.includes('$') && ridType(trimmed) !== 'dataset') {
        setInputError('Not a dataset RID');
        return;
      }
      setInputError(undefined);
      markInteracted();
      setPickCount((count) => count + 1);
      onChange(changeDatasetRidQuery(query, trimmed));
    },
    [markInteracted, onChange, query]
  );

  const complete = resolved !== '' && isResolved && !datasetError;
  const channelSource = useMemo<ChannelSource>(
    () => ({
      complete,
      dataSourceRids: complete ? [resolved] : [],
      canSearchChannels: complete,
      channelPickerKey: resolved || 'no-dataset',
    }),
    [complete, resolved]
  );

  return {
    selectedDataset,
    datasetOptions,
    datasetSelectValue,
    datasetError: inputError ?? datasetError,
    channelSource,
    selectDataset,
  };
}
