import type { ChannelOption } from './queryBuilderTypes';

// Match tiers, highest first. Full-name equality outranks a bounded token
// match, which outranks partial matches.
const FULL_CASE_SENSITIVE_EQUAL = 9;
const FULL_EQUAL = 8;
const BOUNDED_CASE_SENSITIVE = 7;
const BOUNDED = 6;
const STARTS_WITH = 5;
const WORD_STARTS_WITH = 4;
const CONTAINS = 3;
const SUBSEQUENCE = 1;
const NO_MATCH = 0;

const WORD_BOUNDARY_CHARS = new Set(['.', '_', '-', '/', ' ']);

function isSubsequence(query: string, candidate: string): boolean {
  let queryIndex = 0;
  for (let i = 0; i < candidate.length && queryIndex < query.length; i++) {
    if (candidate[i] === query[queryIndex]) {
      queryIndex++;
    }
  }
  return queryIndex === query.length;
}

// A bounded match means the query appears as a complete token: a delimiter or
// string edge on both sides.
function hasBoundedMatch(candidate: string, query: string): boolean {
  let index = candidate.indexOf(query);
  while (index !== -1) {
    const boundedBefore = index === 0 || WORD_BOUNDARY_CHARS.has(candidate[index - 1]);
    const end = index + query.length;
    const boundedAfter = end === candidate.length || WORD_BOUNDARY_CHARS.has(candidate[end]);
    if (boundedBefore && boundedAfter) {
      return true;
    }
    index = candidate.indexOf(query, index + 1);
  }
  return false;
}

function startsAtWordBoundary(candidate: string, query: string): boolean {
  let index = candidate.indexOf(query);
  while (index !== -1) {
    // index 0 cannot reach here: matchRank tests startsWith before calling this.
    if (WORD_BOUNDARY_CHARS.has(candidate[index - 1])) {
      return true;
    }
    index = candidate.indexOf(query, index + 1);
  }
  return false;
}

function matchRank(name: string, query: string, lowerQuery: string): number {
  if (name === query) {
    return FULL_CASE_SENSITIVE_EQUAL;
  }
  const lower = name.toLowerCase();
  if (lower === lowerQuery) {
    return FULL_EQUAL;
  }
  if (hasBoundedMatch(name, query)) {
    return BOUNDED_CASE_SENSITIVE;
  }
  if (hasBoundedMatch(lower, lowerQuery)) {
    return BOUNDED;
  }
  if (lower.startsWith(lowerQuery)) {
    return STARTS_WITH;
  }
  if (startsAtWordBoundary(lower, lowerQuery)) {
    return WORD_STARTS_WITH;
  }
  if (lower.includes(lowerQuery)) {
    return CONTAINS;
  }
  if (isSubsequence(lowerQuery, lower)) {
    return SUBSEQUENCE;
  }
  return NO_MATCH;
}

// Hoisted: localeCompare with an options object builds a fresh collator per call,
// which dominates the sort at the 1000-channel page size.
const collator = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true });
const naturalCompare = collator.compare;

// Latency metrics the Python SDK's stream writer injects into every streamed dataset.
const SDK_METRIC_PREFIX = '__nominal.';

function isSdkMetric(name: string): boolean {
  return name.startsWith(SDK_METRIC_PREFIX);
}

function compareNames(a: string, b: string): number {
  const aInternal = isSdkMetric(a);
  const bInternal = isSdkMetric(b);
  if (aInternal !== bInternal) {
    return aInternal ? 1 : -1;
  }
  return naturalCompare(a, b);
}

/** Reorders server results so the best match is first: the SearchChannels API
 *  scores '.'/'_' variants of a name identically (pg_trgm) and tie-breaks by
 *  row UUID, while the Combobox default-highlights row 0. SDK metric channels
 *  sort after every customer match unless the query itself starts with '__'.
 *  Never drops rows; non-matches keep server order at the end. Always returns
 *  a new array, so callers may mutate the result. */
export function rankChannelOptions(options: ChannelOption[], searchText: string): ChannelOption[] {
  const query = searchText.trim();
  if (!query) {
    return [...options].sort((a, b) => compareNames(a.value, b.value));
  }
  const lowerQuery = query.toLowerCase();
  const demoteSdkMetrics = !lowerQuery.startsWith('__');
  return options
    .map((option, index) => {
      const rank = matchRank(option.value, query, lowerQuery);
      const demoted = rank !== NO_MATCH && demoteSdkMetrics && isSdkMetric(option.value);
      return { option, index, rank, demoted };
    })
    .sort((a, b) => {
      if ((a.rank === NO_MATCH) !== (b.rank === NO_MATCH)) {
        return a.rank === NO_MATCH ? 1 : -1;
      }
      if (a.rank === NO_MATCH) {
        return a.index - b.index;
      }
      if (a.demoted !== b.demoted) {
        return a.demoted ? 1 : -1;
      }
      if (a.rank !== b.rank) {
        return b.rank - a.rank;
      }
      return naturalCompare(a.option.value, b.option.value) || a.index - b.index;
    })
    .map((entry) => entry.option);
}
