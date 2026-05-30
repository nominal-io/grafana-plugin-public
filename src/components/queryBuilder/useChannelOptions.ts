import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AppEvents, type SelectableValue } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import { debounce } from 'lodash';
import type { NominalQuery } from '../../types';
import { resolveDataSourceRids, searchChannels, type Asset } from '../../utils/api';
import { buildChannelOptions, channelsToOptions, getChannelSelectValue } from './queryBuilderOptions';
import { changeSelectedChannelQuery, inferChannelDataTypeQuery } from './queryMutations';
import type { TemplateValueResolution } from './templateResolution';
import type { AssetInputMethod, ChannelOption } from './queryBuilderTypes';

interface UseChannelOptionsArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  selectedAsset: Asset | null;
  assetInputMethod: AssetInputMethod;
  channelResolution: TemplateValueResolution;
  dataScopeResolution: TemplateValueResolution;
  datasourceUrl: string;
  markInteracted: () => void;
}

export interface ChannelOptionsModel {
  channelOptions: ChannelOption[];
  channelSelectValue: SelectableValue<string> | null;
  isLoadingChannels: boolean;
  searchChannels: (searchText: string) => void;
  openChannelMenu: () => void;
  selectChannel: (selection: ChannelOption) => void;
}

const notifyError = (title: string, message: string) => {
  getAppEvents().publish({
    type: AppEvents.alertError.name,
    payload: [title, message],
  });
};

export function useChannelOptions({
  query,
  onChange,
  selectedAsset,
  assetInputMethod,
  channelResolution,
  dataScopeResolution,
  datasourceUrl,
  markInteracted,
}: UseChannelOptionsArgs): ChannelOptionsModel {
  const [channelResults, setChannelResults] = useState<ChannelOption[]>([]);
  const [isLoadingChannels, setIsLoadingChannels] = useState(false);

  const queryRef = useRef(query);
  queryRef.current = query;
  const isMountedRef = useRef(true);

  // Dynamically search channels via backend; called by the Select loadOptions prop.
  const loadChannelOptions = useCallback(
    async (searchText: string): Promise<ChannelOption[]> => {
      if (!selectedAsset) {
        return [];
      }
      const dataSourceRids = resolveDataSourceRids(selectedAsset, dataScopeResolution.resolved || undefined);
      try {
        const channels = await searchChannels(datasourceUrl, dataSourceRids, searchText);
        return channelsToOptions(channels);
      } catch {
        if (isMountedRef.current) {
          notifyError('Unable to load Nominal channels', 'Check the selected asset, data scope, and data source configuration.');
        }
        return [];
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    },
    [selectedAsset, datasourceUrl, dataScopeResolution.resolved]
  );

  // Keep a ref so the stable debounce below always calls the latest closure without
  // needing to be recreated (and without leaving stale pending timeouts behind).
  const loadChannelOptionsRef = useRef(loadChannelOptions);
  loadChannelOptionsRef.current = loadChannelOptions;

  // Debounced channel search that populates state (synchronous options) instead of
  // returning a Promise. This allows allowCustomValue to work on the Select.
  // The counter guard discards late responses so a slow earlier request can't overwrite newer results.
  const channelSearchId = useRef(0);
  const debouncedChannelSearch = useRef(
    debounce((searchText: string) => {
      const id = ++channelSearchId.current;
      setIsLoadingChannels(true);
      loadChannelOptionsRef.current(searchText)
        .then((results) => {
          if (isMountedRef.current && channelSearchId.current === id) {
            setChannelResults(results);
          }
        })
        .catch(() => {
          if (isMountedRef.current && channelSearchId.current === id) {
            setChannelResults([]);
          }
        })
        .finally(() => {
          if (isMountedRef.current && channelSearchId.current === id) {
            setIsLoadingChannels(false);
          }
        });
    }, 300)
  ).current;

  const openChannelMenu = useCallback(() => {
    debouncedChannelSearch('');
  }, [debouncedChannelSearch]);

  // Pre-load channel options when the channel dropdown becomes visible or the
  // underlying asset/datascope changes (mirrors the old defaultOptions behaviour).
  useEffect(() => {
    if (selectedAsset) {
      setChannelResults([]);
      setIsLoadingChannels(true);
      debouncedChannelSearch('');
    }
  }, [selectedAsset, dataScopeResolution.resolved, debouncedChannelSearch]);

  // Infer channelDataType when the resolved channel changes (e.g. template variable).
  // The backend does its own inference, but the frontend needs the correct type to show
  // the right aggregation UI (numeric MultiSelect vs disabled "Mode" / "Logs (raw)").
  useEffect(() => {
    if (!channelResolution.resolved || !channelResolution.isResolved || !selectedAsset) {
      return;
    }
    // Skip if type was already set by direct dropdown selection (not a variable)
    if (queryRef.current?.channelDataType && !channelResolution.hasTemplate) {
      return;
    }
    const dataSourceRids = resolveDataSourceRids(selectedAsset, dataScopeResolution.resolved || undefined);
    if (dataSourceRids.length === 0) {
      return;
    }

    let cancelled = false;
    searchChannels(datasourceUrl, dataSourceRids, channelResolution.resolved)
      .then((channels) => {
        if (cancelled) {
          return;
        }
        const match = channels.find((ch) => ch.name === channelResolution.resolved);
        if (match && match.dataType && match.dataType !== queryRef.current?.channelDataType) {
          onChange(inferChannelDataTypeQuery(queryRef.current, match.dataType));
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
    // Depend on selectedAsset?.rid (not the full object) to avoid a redundant /channels
    // call whenever setSelectedAsset is called with a logically identical asset.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    channelResolution.resolved,
    channelResolution.isResolved,
    channelResolution.hasTemplate,
    selectedAsset?.rid,
    dataScopeResolution.resolved,
    datasourceUrl,
  ]);

  const selectChannel = useCallback(
    (selection: ChannelOption) => {
      markInteracted();
      // NOTE: channelDataType is captured at selection time from the dropdown option.
      // If channel is later overridden by a template variable that resolves to a
      // different channel, the stored channelDataType may be stale. The backend will
      // fall back to numeric for an unknown type, but mismatches can cause query errors.
      // Mitigation: the backend error message hints the user to re-select the channel.
      onChange(changeSelectedChannelQuery(query, {
        channel: selection?.value || '',
        dataType: selection?.dataType || '',
        assetInputMethod,
      }));
    },
    [assetInputMethod, markInteracted, onChange, query]
  );

  // Clean up the debounced search and discard in-flight responses on unmount.
  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      channelSearchId.current += 1;
      debouncedChannelSearch.cancel();
    };
  }, [debouncedChannelSearch]);

  const channelOptions = useMemo(
    () =>
      buildChannelOptions({
        channelResults,
        channel: channelResolution,
      }),
    [channelResults, channelResolution]
  );

  const channelSelectValue = useMemo(
    () => getChannelSelectValue({ channel: channelResolution }),
    [channelResolution]
  );

  return {
    channelOptions,
    channelSelectValue,
    isLoadingChannels,
    searchChannels: debouncedChannelSearch,
    openChannelMenu,
    selectChannel,
  };
}
