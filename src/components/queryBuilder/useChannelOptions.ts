import { useCallback, useEffect, useMemo, useRef } from 'react';
import { AppEvents } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';
import type { NominalQuery } from '../../types';
import { resolveDataSourceRids, searchChannels, type Asset } from '../../utils/api';
import { buildChannelOptions, channelsToOptions, getChannelSelectValue } from './queryBuilderOptions';
import { changeSelectedChannelQuery, inferChannelDataTypeQuery } from './queryMutations';
import type { TemplateValueResolution } from './templateResolution';
import type { AssetInputMethod, ChannelOption, ChannelOptionsLoader } from './queryBuilderTypes';

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
  channelOptions: ChannelOptionsLoader;
  channelSelectValue: ChannelOption | null;
  selectChannel: (selection: ChannelOption) => void;
}

interface ChannelOptionsContext {
  dataSourceRids: string[];
  hasSelectedAsset: boolean;
  key: string;
}

const notifyError = (title: string, message: string) => {
  getAppEvents().publish({
    type: AppEvents.alertError.name,
    payload: [title, message],
  });
};

function getChannelOptionsContext({
  datasourceUrl,
  selectedAsset,
  dataScopeName,
}: {
  datasourceUrl: string;
  selectedAsset: Asset | null;
  dataScopeName: string;
}): ChannelOptionsContext {
  const dataSourceRids = selectedAsset ? resolveDataSourceRids(selectedAsset, dataScopeName || undefined) : [];

  return {
    dataSourceRids,
    hasSelectedAsset: selectedAsset !== null,
    key: JSON.stringify([datasourceUrl, selectedAsset?.rid || '', dataScopeName, dataSourceRids]),
  };
}

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
  const queryRef = useRef(query);
  queryRef.current = query;
  const isMountedRef = useRef(true);
  const channelOptionsRequestId = useRef(0);

  const channelOptionsContext = useMemo(
    () =>
      getChannelOptionsContext({
        datasourceUrl,
        selectedAsset,
        dataScopeName: dataScopeResolution.resolved,
      }),
    [dataScopeResolution.resolved, datasourceUrl, selectedAsset]
  );
  const channelOptionsContextKeyRef = useRef(channelOptionsContext.key);
  channelOptionsContextKeyRef.current = channelOptionsContext.key;

  const channelResolutionSnapshot = useMemo(
    () => ({
      raw: channelResolution.raw,
      resolved: channelResolution.resolved,
      hasTemplate: channelResolution.hasTemplate,
      isResolved: channelResolution.isResolved,
    }),
    [
      channelResolution.raw,
      channelResolution.resolved,
      channelResolution.hasTemplate,
      channelResolution.isResolved,
    ]
  );

  const channelOptions = useCallback<ChannelOptionsLoader>(
    async (searchText: string): Promise<ChannelOption[]> => {
      const requestId = ++channelOptionsRequestId.current;
      const requestContextKey = channelOptionsContext.key;
      if (!channelOptionsContext.hasSelectedAsset) {
        return [];
      }
      try {
        const channels = await searchChannels(datasourceUrl, channelOptionsContext.dataSourceRids, searchText);
        return buildChannelOptions({
          channelResults: channelsToOptions(channels),
          channel: channelResolutionSnapshot,
        });
      } catch {
        // Only the latest loader request may emit alerts; stale failures can belong to an old asset/scope/search.
        if (
          isMountedRef.current &&
          channelOptionsRequestId.current === requestId &&
          channelOptionsContextKeyRef.current === requestContextKey
        ) {
          notifyError(
            'Unable to load Nominal channels',
            'Check the selected asset, data scope, and data source configuration.'
          );
        }
        return [];
      }
    },
    [channelOptionsContext, channelResolutionSnapshot, datasourceUrl]
  );

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
    // Intentionally depend on selectedAsset?.rid rather than the whole object to avoid
    // redundant /channels calls when setSelectedAsset receives a logically identical asset.
    // onChange is omitted for the same reason this effect uses queryRef.current: this lookup
    // should be driven by resolved channel/scope/asset identity, not parent callback identity.
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
      onChange(
        changeSelectedChannelQuery(query, {
          channel: selection?.value || '',
          dataType: selection?.dataType || '',
          assetInputMethod,
        })
      );
    },
    [assetInputMethod, markInteracted, onChange, query]
  );

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      channelOptionsRequestId.current += 1;
    };
  }, []);

  const channelSelectValue = useMemo(
    () =>
      getChannelSelectValue({
        channel: channelResolutionSnapshot,
        channelDataType: query.channelDataType,
      }),
    [channelResolutionSnapshot, query.channelDataType]
  );

  return {
    channelOptions,
    channelSelectValue,
    selectChannel,
  };
}
