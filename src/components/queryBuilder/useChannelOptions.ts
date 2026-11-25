import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { NominalQuery } from '../../types';
import { searchChannels } from '../../utils/api';
import { buildChannelOptions, channelsToOptions, getChannelSelectValue } from './queryBuilderOptions';
import { changeSelectedChannelQuery, inferChannelDataTypeQuery } from './queryMutations';
import { useResolutionSnapshot, type TemplateValueResolution } from './templateResolution';
import { notifyError } from './notifyError';
import type { ChannelOption, ChannelOptionsLoader } from './queryBuilderTypes';

interface UseChannelOptionsArgs {
  query: NominalQuery;
  onChange: (query: NominalQuery) => void;
  dataSourceRids: string[];
  channelResolution: TemplateValueResolution;
  datasourceUrl: string;
  hasUserInteracted: boolean;
  markInteracted: () => void;
}

export interface ChannelOptionsModel {
  channelOptions: ChannelOptionsLoader;
  channelSelectValue: ChannelOption | null;
  /** The type the aggregation controls follow: one inferred for the current source and channel, else the saved one. */
  channelDataType: string | undefined;
  selectChannel: (selection: ChannelOption) => void;
}

let nextChannelOptionsLoaderId = 0;

function createChannelOptionsBackendRequestId(): string {
  nextChannelOptionsLoaderId += 1;
  return `nominal-channel-options-${nextChannelOptionsLoaderId}`;
}

export function useChannelOptions({
  query,
  onChange,
  dataSourceRids,
  channelResolution,
  datasourceUrl,
  hasUserInteracted,
  markInteracted,
}: UseChannelOptionsArgs): ChannelOptionsModel {
  const queryRef = useRef(query);
  queryRef.current = query;
  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;
  const isMountedRef = useRef(true);
  const channelOptionsRequestId = useRef(0);
  const channelOptionsBackendRequestId = useMemo(createChannelOptionsBackendRequestId, []);

  const ridsKey = JSON.stringify(dataSourceRids);
  const channelOptionsContextKey = JSON.stringify([datasourceUrl, dataSourceRids]);
  const channelOptionsContextKeyRef = useRef(channelOptionsContextKey);
  channelOptionsContextKeyRef.current = channelOptionsContextKey;
  // The RID list the current channel's data type was set or inferred under.
  const typeRidsKeyRef = useRef<string>(undefined);
  // A type inferred without editing the query, for the source and channel in `key`.
  const [inferredType, setInferredType] = useState<{ key: string; dataType: string }>();
  const typeKey = JSON.stringify([ridsKey, channelResolution.resolved]);

  const channelResolutionSnapshot = useResolutionSnapshot(channelResolution);

  const channelOptions = useCallback<ChannelOptionsLoader>(
    async (searchText: string): Promise<ChannelOption[]> => {
      const requestId = ++channelOptionsRequestId.current;
      const requestContextKey = channelOptionsContextKey;
      try {
        const channels = await searchChannels(datasourceUrl, dataSourceRids, searchText, {
          requestId: channelOptionsBackendRequestId,
        });
        // Combobox discards superseded resolved async options before they reach the menu.
        // The local request/context guard below is only for alert side effects.
        return buildChannelOptions({
          channelResults: channelsToOptions(channels),
          channel: channelResolutionSnapshot,
          searchText,
        });
      } catch {
        // Only the latest loader request may emit alerts; stale failures can belong to an old source/search.
        if (
          isMountedRef.current &&
          channelOptionsRequestId.current === requestId &&
          channelOptionsContextKeyRef.current === requestContextKey
        ) {
          notifyError('Unable to load Nominal channels', 'Check the selected source and data source configuration.');
        }
        return [];
      }
    },
    [channelOptionsBackendRequestId, channelOptionsContextKey, dataSourceRids, channelResolutionSnapshot, datasourceUrl]
  );

  // Infer channelDataType when the resolved channel changes (e.g. template variable).
  // The backend does its own inference, but the frontend needs the correct type to show
  // the right aggregation UI (numeric MultiSelect vs disabled "Mode" / "Logs (raw)").
  useEffect(() => {
    if (!channelResolution.resolved || !channelResolution.isResolved || dataSourceRids.length === 0) {
      return;
    }
    // A literal channel picked from the dropdown already has its type, as long as the
    // sources are the ones it was picked under. Before any edit, a saved query adopts the
    // first list it sees. A later list, such as one a dashboard variable switched to, is
    // looked up but only shown, not written to the query; the backend checks the type at
    // query time. After an edit, a new list may belong to a newly picked source.
    if (queryRef.current?.channelDataType && !channelResolution.hasTemplate) {
      if (!hasUserInteracted) {
        typeRidsKeyRef.current ??= ridsKey;
      }
      if (typeRidsKeyRef.current === ridsKey) {
        return;
      }
    }

    let cancelled = false;
    const persist = channelResolution.hasTemplate || hasUserInteracted;
    searchChannels(datasourceUrl, dataSourceRids, channelResolution.resolved)
      .then((channels) => {
        if (cancelled) {
          return;
        }
        typeRidsKeyRef.current = ridsKey;
        const dataType = channels.find((ch) => ch.name === channelResolution.resolved)?.dataType;
        if (!dataType) {
          return;
        }
        setInferredType({ key: typeKey, dataType });
        if (persist && dataType !== queryRef.current?.channelDataType) {
          onChangeRef.current(inferChannelDataTypeQuery(queryRef.current, dataType));
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
    // dataSourceRids is tracked through its key so a new array of the same RIDs
    // does not trigger another /channels call. hasUserInteracted only matters when
    // the RIDs change, so it is read here but not tracked. typeKey follows ridsKey and the
    // resolved channel.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [channelResolution.resolved, channelResolution.isResolved, channelResolution.hasTemplate, ridsKey, datasourceUrl]);

  const selectChannel = useCallback(
    (selection: ChannelOption) => {
      markInteracted();
      typeRidsKeyRef.current = ridsKey;
      // NOTE: channelDataType is captured at selection time from the dropdown option.
      // If channel is later overridden by a template variable that resolves to a
      // different channel, the stored channelDataType may be stale. The backend will
      // fall back to numeric for an unknown type, but mismatches can cause query errors.
      // Mitigation: the backend error message hints the user to re-select the channel.
      onChange(
        changeSelectedChannelQuery(query, {
          channel: selection?.value || '',
          dataType: selection?.dataType || '',
        })
      );
    },
    [markInteracted, onChange, query, ridsKey]
  );

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      channelOptionsRequestId.current += 1;
    };
  }, []);

  const channelDataType = inferredType?.key === typeKey ? inferredType.dataType : query.channelDataType;
  const channelSelectValue = useMemo(
    () => getChannelSelectValue({ channel: channelResolutionSnapshot, channelDataType }),
    [channelResolutionSnapshot, channelDataType]
  );

  return {
    channelOptions,
    channelSelectValue,
    channelDataType,
    selectChannel,
  };
}
